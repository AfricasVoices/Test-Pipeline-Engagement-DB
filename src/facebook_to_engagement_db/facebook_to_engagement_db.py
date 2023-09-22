from dateutil.parser import isoparse
import csv

from google.cloud.firestore_v1 import FieldFilter
from storage.google_cloud import google_cloud_utils
from social_media_tools.facebook import (FacebookClient, facebook_utils)
from core_data_modules.logging import Logger
from core_data_modules.util import IOUtils

from engagement_database.data_models import (Message, MessageDirections, MessageOrigin, MessageStatuses,
                                             HistoryEntryOrigin)

from src.facebook_to_engagement_db.cache import FacebookSyncCache
from src.facebook_to_engagement_db.sync_stats import FacebookSyncEvents, FacebookToEngagementDBSyncStats

log = Logger(__name__)


def _facebook_comment_to_engagement_db_message(facebook_comment, dataset, origin_id, uuid_table):
    """
    Converts a Facebook comment to an engagement database message.

    :param facebook_comment: Dictionary containing the facebook comment data values.
    :type facebook_comment: dict
    :param dataset: Initial dataset to assign this message to in the engagement database.
    :type dataset: str
    :param origin_id: Origin id, for the comment origin field.
    :type origin_id: str
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :return: `facebook_comment` as an engagement db message.
    :rtype: engagement_database.data_models.Message
    """
    participant_uuid = uuid_table.data_to_uuid(facebook_comment["from"]["id"])
    channel_operator = 'facebook' #TODO move to core as a CONSTANT

    return Message(
        participant_uuid=participant_uuid,
        text=facebook_comment["message"],
        timestamp=isoparse(facebook_comment["created_time"]).isoformat(),
        direction=MessageDirections.IN,
        channel_operator=channel_operator,
        status=MessageStatuses.LIVE,
        dataset=dataset,
        labels=[],
        origin=MessageOrigin(
            origin_id=origin_id,
            origin_type="facebook"
        )
    )


def _engagement_db_has_message(engagement_db, message):
    """
    Checks if an engagement database contains a comment with the same origin id as the given comment.

    :param engagement_db: Engagement database to check for the comment.
    :type engagement_db: engagement_database.EngagementDatabase
    :param message: Comment to check for existence.
    :type message: engagement_database.data_models.Message
    :return: Whether a message with this text, timestamp, and participant_uuid exists in the engagement database.
    :rtype: bool
    """
    matching_messages_filter = lambda q: q.where(filter=FieldFilter("origin.origin_id", "==", message.origin.origin_id))
    matching_messages = engagement_db.get_messages(firestore_query_filter=matching_messages_filter)
    assert len(matching_messages) < 2

    return len(matching_messages) > 0

# Todo pass stats out via return statements
def _ensure_engagement_db_has_comment(engagement_db, facebook_comment, message_origin_details, sync_stats):
    """
    Ensures that the given facebook comment exists in an engagement database.
    This function will only write to the database if a message with the same origin_id doesn't already exist in the
    database.

    :param engagement_db: Engagement database to use.
    :type engagement_db: engagement_database.EngagementDatabase
    :param facebook_comment: Comment to make sure exists in the engagement database.
    :type facebook_comment: engagement_database.data_models.Message
    :param message_origin_details: Comment origin details, to be logged in the HistoryEntryOrigin.details.
    :type message_origin_details: dict
    :param sync_stats: An instance of FacebookToEngagementDBSyncStats to update adding message to db event.
    :type sync_stats: src.facebook_to_engagement_db.sync_stats.FacebookToEngagementDBSyncStats
    """
    if _engagement_db_has_message(engagement_db, facebook_comment):
        log.debug(f"comment already in engagement database")
        return

    sync_stats.add_event(FacebookSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB)
    log.debug(f"Adding comment to engagement database")
    engagement_db.set_message(
        facebook_comment,
        HistoryEntryOrigin(origin_name="Facebook -> Database Sync", details=message_origin_details)
    )


def _get_facebook_post_ids(facebook_client, page_id, post_ids=None, search=None,):
    """
    Gets the ids of the target facebook posts to download comments from.
    Posts can be defined as a list of post_ids and/or a search object containing a search string and time range.

    :param facebook_client: Instance of facebook page to download the comments from.
    :type facebook_client: social_media_tools.facebook_client.FacebookClient
    :param page_id: Id of the page to download all the posts from.
    :type page_id: str
    :param post_id: Id of post to download the comments from.
    :type post_id: str
    :param search: Search parameters for downloading target comments
    :type search: dict containing match, start_date, end_date keys to their values.
    """
    combined_post_ids = []
    if post_ids is not None:
        combined_post_ids.extend(post_ids)

    if search is not None:
        # Download the posts in the time-range to search, and add those which contain the match string to the list
        # of post_ids to download comments from.
        posts_to_search = facebook_client.get_posts_published_by_page(
            page_id, fields=["message", "created_time"],
            created_after=search["start_date"], created_before=search["end_date"]
        )
        for post in posts_to_search:
            if "message" in post and search["match"] in post["message"] and post["id"] not in combined_post_ids:
                combined_post_ids.append(post["id"])

    return combined_post_ids


def _fetch_post_engagement_metrics(facebook_client, page_id, post, post_id, engagement_db_dataset):
    """
    Fetches engagement metrics for a facebook post.

    :param facebook_client: Instance of the facebook page to generate the post metrics from.
    :type facebook_client: social_media_tools.facebook_client.FacebookClient
    :param page_id: Id of the page with the post.
    :type page_id: str
    :param post: Post to generate engagement metrics from.
    :type post: dict.
    :param post_id: Id of post to download the comments from.
    :type post_id: str
    :param engagement_db_dataset: Engagement db dataset name for this post.
    :type engagement_db_dataset: str
    :return post_metrics: dict of post engagement metrics
    :rtype post_metrics: dict
    """

    post_engagement_metrics = facebook_client.get_metrics_for_post(
        post_id, ["post_impressions", "post_impressions_unique",
                  "post_engaged_users", "post_reactions_by_type_total"]
    )
    post_metrics = {
        "Page ID": page_id,
        "Dataset": engagement_db_dataset,
        "Post URL": f"facebook.com/{post_id}",
        "Post Created Time": post["created_time"],
        "Post Text": post["message"],
        "Post Type": facebook_utils.clean_post_type(post),
        "Post Impressions": post_engagement_metrics["post_impressions"],
        "Unique Post Impressions": post_engagement_metrics["post_impressions_unique"],
        "Post Engaged Users": post_engagement_metrics["post_engaged_users"],
        "Total Comments": post["comments"]["summary"]["total_count"],
        "Visible (analysed) Comments": len(post_engagement_metrics),
        # post_reactions_by_type_total is a dict of reaction_type -> total, but we're only interested in
        # the total across all types, so sum all the values.
        "Reactions": sum(
            [type_total for type_total in post_engagement_metrics["post_reactions_by_type_total"].values()])
    }

    return post_metrics


def _export_facebook_metrics_csv(facebook_metrics, facebook_metrics_dir_path):
    """
    Exports a csv file with facebook metrics.

    :param facebook_metrics: List of dicts of post_ids ->  engagement metrics.
    :type facebook_metrics: list
    :param facebook_metrics_dir_path: Path to a directory to save facebook metrics CSV file.
    :type facebook_metrics_dir_path: str
    """

    IOUtils.ensure_dirs_exist(facebook_metrics_dir_path)
    headers = ["Page ID", "Dataset", "Post URL", "Post Created Time", "Post Text", "Post Type", "Post Impressions",
               "Unique Post Impressions", "Post Engaged Users", "Total Comments", "Visible (analysed) Comments",
               "Reactions"]

    if len(facebook_metrics) == 0:
        log.info("No Facebook posts detected, so don't write a metrics file.")
        return

    facebook_metrics.sort(key=lambda m: (m["Page ID"], m["Dataset"], m["Post Created Time"]))
    with open(f"{facebook_metrics_dir_path}/facebook_metrics.csv", "w") as f:
        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        for metric in facebook_metrics:
            writer.writerow(metric)


def _fetch_and_sync_facebook_to_engagement_db(google_cloud_credentials_file_path, facebook_source,
                                              engagement_db, uuid_table, metrics_dir_path, cache=None):
    """
    Fetches facebook comments from target pages and syncs them to an engagement database.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use when
                                               downloading facebook page token.
    :type google_cloud_credentials_file_path: str
    :param facebook_sources: Facebook source to sync to the engagement database.
    :type facebook_sources: src.social_media_to_engagement_db.configuration.FacebookSource
    :param engagement_db: Engagement database to sync the comments to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to re-identify the URNs so we can set the channel operator.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param metrics_dir_path: Path to a directory to save facebook metrics CSV file.
    :type metrics_dir_path: str
    :param cache: Cache to check for a timestamp of the latest seen comment. If None, downloads all comments.
    :type cache: src.facebook_to_engagement_db.FacebookSyncCache | None
    """
    log.info("Fetching data from Facebook...")
    log.info("Downloading Facebook access token...")
    facebook_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, facebook_source.token_file_url).strip()

    facebook_client = FacebookClient(facebook_token)

    facebook_metrics = []
    dataset_to_sync_stats = dict()  # of '{dataset.engagement_db_dataset}' -> FacebookToEngagementDBSyncStats
    for dataset in facebook_source.datasets:
        # Download and sync all the comments on all the posts in this dataset.
        dataset_post_ids = _get_facebook_post_ids(facebook_client, facebook_source.page_id, search=dataset.search)
        if len(dataset_post_ids) == 0:
            log.warning(f"No posts found for {dataset.engagement_db_dataset}, please investigate "
                        f"search parameters or check if posts were published skipping ...")
            continue

        sync_stats = FacebookToEngagementDBSyncStats()
        for post_id in dataset_post_ids:
            latest_comment_timestamp = None if cache is None else cache.get_latest_comment_timestamp(post_id)
            post_comments = facebook_client.get_all_comments_on_post(
                post_id, ["from{id}", "parent", "attachments", "created_time", "message"],
                )
            sync_stats.add_event(FacebookSyncEvents.READ_POSTS_FROM_FACEBOOK)

            # Download the post and add it as context to all the comments. Adding a reference to the post under
            # which a comment was made enables downstream features such as post-type labelling and comment context
            # in Coda, as well as allowing us to track how many comments were made on each post.
            post = facebook_client.get_post(post_id, fields=["attachments", "message", "created_time",
                                                          "comments.filter(stream).limit(0).summary(true)"])

            post_metrics = _fetch_post_engagement_metrics(facebook_client, facebook_source.page_id, post, post_id,
                                                dataset.engagement_db_dataset)

            facebook_metrics.append(post_metrics)

            for comment_count, comment in enumerate(post_comments):
                sync_stats.add_event(FacebookSyncEvents.READ_COMMENTS_FROM_POSTS)
                log.info(f'Processing comment {comment_count}/{len(post_comments)} ')
                # Facebook only returns a parent if the comment is a reply to another comment.
                # If there is no parent, set one to the empty-dict.
                if "parent" not in comment:
                    comment["parent"] = {}

                # Only try to add the db comments that were created after the last seen comment.created_time
                # This helps us reduce the number of reads to the db when checking for existing comments.
                add_comment_to_db = True
                if latest_comment_timestamp is not None and isoparse(comment['created_time']) <= latest_comment_timestamp:
                    add_comment_to_db = False

                if not add_comment_to_db:
                    log.info(f'Comment synced in previous run skipping ...')
                    sync_stats.add_event(FacebookSyncEvents.COMMENT_SYNCED_IN_PREVIOUS_RUN)
                    continue

                origin_id = f'facebook_comment_id_{comment["id"]}'
                message = _facebook_comment_to_engagement_db_message(comment, dataset.engagement_db_dataset,
                                                                     origin_id, uuid_table)

                message_origin_details = {
                    "page_id": facebook_source.page_id,
                    "post_id": post_id,
                    "user_id": comment["from"]["id"],
                    "comment_id": comment["id"],
                }

                _ensure_engagement_db_has_comment(engagement_db, message, message_origin_details, sync_stats)

                if cache is not None:
                    cache.set_latest_comment_timestamp(post_id, isoparse(comment['created_time']))

            dataset_to_sync_stats[dataset.engagement_db_dataset] = sync_stats


    _export_facebook_metrics_csv(facebook_metrics, metrics_dir_path)

    all_sync_stats = FacebookToEngagementDBSyncStats()
    for dataset in dataset_to_sync_stats:
        log.info(f"Summary of actions for dataset '{dataset}':")
        dataset_to_sync_stats[dataset].print_summary()
        all_sync_stats.add_stats(dataset_to_sync_stats[dataset])

    log.info(f"Summary of actions for all datasets in {facebook_source.page_id} page:")
    all_sync_stats.print_summary()


def sync_facebook_to_engagement_db(google_cloud_credentials_file_path, facebook_sources, engagement_db, uuid_table,
                                   metrics_dir_path, cache_path):
    """
    Syncs Facebook comments to an engagement database.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use when
                                               downloading facebook page token.
    :type google_cloud_credentials_file_path: str
    :param facebook_sources: Facebook sources to sync to the engagement database.
    :type facebook_sources: list of src.social_media_to_engagement_db.configuration.FacebookSource
    :param engagement_db: Engagement database to sync the comments to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to re-identify the URNs so we can set the channel operator.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param metrics_dir_path: Path to a directory to save facebook metrics CSV file.
    :type metrics_dir_path: str
    """
    if cache_path is None:
        cache = None
    else:
        log.info(f"Initialising FacebookSyncCache at '{cache_path}/facebook_to_engagement_db'")
        cache = FacebookSyncCache(f"{cache_path}/facebook_to_engagement_db")

    for i, facebook_source in enumerate(facebook_sources):
        _fetch_and_sync_facebook_to_engagement_db(google_cloud_credentials_file_path, facebook_source, engagement_db,
                                                  uuid_table, metrics_dir_path, cache)
