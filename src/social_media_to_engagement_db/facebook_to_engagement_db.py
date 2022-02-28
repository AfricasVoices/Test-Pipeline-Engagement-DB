from dateutil.parser import isoparse

from storage.google_cloud import google_cloud_utils
from social_media_tools.facebook import FacebookClient, facebook_utils
from core_data_modules.logging import Logger

from engagement_database.data_models import (Message, MessageDirections, MessageOrigin, MessageStatuses,
                                             HistoryEntryOrigin)

log = Logger(__name__)


def _facebook_comment_to_engagement_db_message(facebook_comment, dataset, origin_id, uuid_table):
    """
    Converts a Facebook comment  to an engagement database message.

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
    channel_operator = 'facebook'

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
    matching_messages_filter = lambda q: q.where("origin.origin_id", "==", message.origin.origin_id)
    matching_messages = engagement_db.get_messages(firestore_query_filter=matching_messages_filter)
    assert len(matching_messages) < 2

    return len(matching_messages) > 0


def _ensure_engagement_db_has_comment(engagement_db, facebook_comment, message_origin_details):
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
    """
    if _engagement_db_has_message(engagement_db, facebook_comment):
        log.debug(f"comment already in engagement database")
        return

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


def _fetch_and_sync_facebook_to_engagement_db(google_cloud_credentials_file_path, facebook_source, engagement_db, uuid_table):
    """
    Fetches facebook comments from target pages and syncs them  to an engagement database.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use when
                                               downloading facebook page token.
    :type google_cloud_credentials_file_path: str
    :param facebook_sources: Facebook source to sync to the engagement database.
    :type facebook_sources: src.social_media_to_engagement_db.configuration.FacebookSource
    :param engagement_db: Engagement database to sync the comments to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to re-identify the URNs so we can set the channel operator.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    """

    log.info("Fetching data from Facebook...")
    log.info("Downloading Facebook access token...")
    facebook_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, facebook_source.token_file_url).strip()

    facebook = FacebookClient(facebook_token)

    for dataset in facebook_source.datasets:
        # Download and sync all the comments on all the posts in this dataset.
        for post_id in _get_facebook_post_ids(facebook, facebook_source.page_id, search=dataset.search):
            post_comments = facebook.get_all_comments_on_post(post_id,
                        fields=["from{id}", "parent", "attachments", "created_time", "message"]
                    )

            # Download the post and add it as context to all the comments. Adding a reference to the post under
            # which a comment was made enables downstream features such as post-type labelling and comment context
            # in Coda, as well as allowing us to track how many comments were made on each post.
            post = facebook.get_post(post_id, fields=["attachments"])
            for comment in post_comments:
                comment["post"] = post

                # Facebook only returns a parent if the comment is a reply to another comment.
                # If there is no parent, set one to the empty-dict.
                if "parent" not in comment:
                    comment["parent"] = {}

                origin_id = f'page_id_{facebook_source.page_id}.user_id_{comment["from"]["id"]}._comment_id_{comment["id"]}'
                message = _facebook_comment_to_engagement_db_message(comment, dataset.engagement_db_dataset,
                                                                     origin_id, uuid_table)

                message_origin_details = {
                    "page_id": facebook_source.page_id,
                    "post_id": post_id,
                    "user_id": comment["from"]["id"],
                    "comment_id": comment["id"],
                }

                _ensure_engagement_db_has_comment(engagement_db, message, message_origin_details)


def sync_facebook_to_engagement_db(google_cloud_credentials_file_path, facebook_sources, engagement_db, uuid_table):
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
    """

    for i, facebook_source in enumerate(facebook_sources):
        _fetch_and_sync_facebook_to_engagement_db(google_cloud_credentials_file_path, facebook_source, engagement_db, uuid_table)
