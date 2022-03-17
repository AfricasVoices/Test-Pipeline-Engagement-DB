from core_data_modules.logging import Logger
from core_data_modules.util import SHAUtils
from engagement_database.data_models import MessageStatuses, HistoryEntryOrigin
from google.cloud import firestore

from src.engagement_db_coda_sync.cache import CodaSyncCache
from src.engagement_db_coda_sync.lib import _update_engagement_db_message_from_coda_message, _add_message_to_coda
from src.engagement_db_coda_sync.sync_stats import EngagementDBToCodaSyncStats, CodaSyncEvents

log = Logger(__name__)


@firestore.transactional
def _sync_next_engagement_db_message_to_coda(transaction, engagement_db, coda, coda_config, dataset_config, last_seen_message, dry_run=False):
    """
    Syncs a message from an engagement database to Coda.

    This method:
     - Gets the least recently updated message that was last updated after `last_seen_message`.
     - Writes back a coda id if the engagement db message doesn't have one yet.
     - Syncs the labels from Coda to this message if the message already exists in Coda.
     - Creates a new message in Coda if this message hasn't been seen in Coda yet.

    :param transaction: Transaction in the engagement database to perform the update in.
    :type transaction: google.cloud.firestore.Transaction
    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param coda: Coda instance to sync the message to.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param coda_config: Coda sync configuration.
    :type coda_config: src.engagement_db_coda_sync.configuration.CodaSyncConfiguration
    :param dataset_config: Configuration for the dataset to sync.
    :type dataset_config: src.engagement_db_coda_sync.configuration.CodaDatasetConfiguration
    :param last_seen_message: Last seen message, downloaded from the database in a previous call, or None.
                              If provided, downloads the least recently updated (next) message after this one, otherwise
                              downloads the least recently updated message in the database.
    :type last_seen_message: engagement_database.data_models.Message | None
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return: A tuple of:
             1. The engagement database message that was synced. If there was no new message to sync, returns None.
             2. Sync stats.
    :rtype: (engagement_database.data_models.Message | None, src.engagement_db_coda_sync.sync_stats.EngagementDBToCodaSyncStats)
    """
    if last_seen_message is None:
        messages_filter = lambda q: q \
            .where("status", "in", [MessageStatuses.LIVE, MessageStatuses.STALE]) \
            .where("dataset", "==", dataset_config.engagement_db_dataset) \
            .order_by("last_updated") \
            .order_by("message_id") \
            .limit(1)
    else:
        # Get the next message after the last_seen_message, having sorted by last_updated than message_id
        # Note: The last_seen_message can be the next/later message to be synced if it was updated
        messages_filter = lambda q: q \
            .where("status", "in", [MessageStatuses.LIVE, MessageStatuses.STALE]) \
            .where("dataset", "==", dataset_config.engagement_db_dataset) \
            .order_by("last_updated") \
            .order_by("message_id") \
            .where("last_updated", ">=", last_seen_message.last_updated) \
            .start_after({"last_updated": last_seen_message.last_updated, "message_id": last_seen_message.message_id}) \
            .limit(1)

    next_message_results = engagement_db.get_messages(firestore_query_filter=messages_filter, transaction=transaction)

    sync_stats = EngagementDBToCodaSyncStats()
    if len(next_message_results) == 0:
        return None, sync_stats
    else:
        engagement_db_message = next_message_results[0]
        sync_stats.add_event(CodaSyncEvents.READ_MESSAGE_FROM_ENGAGEMENT_DB)

    if engagement_db_message.text is None or engagement_db_message.text == "":
        # Don't sync messages that don't have text
        log.info(f"Message {engagement_db_message.message_id} is empty (.text == {engagement_db_message.text}), "
                 f"not adding to Coda")
        sync_stats.add_event(CodaSyncEvents.SKIP_EMPTY_MESSAGE)
        return engagement_db_message, sync_stats

    log.info(f"Syncing message {engagement_db_message.message_id}...")
    # Ensure the message has a valid coda id. If it doesn't have one yet, write one back to the database.
    if engagement_db_message.coda_id is None:
        log.debug("Creating coda id")
        sync_stats.add_event(CodaSyncEvents.SET_CODA_ID)
        engagement_db_message.coda_id = SHAUtils.sha_string(engagement_db_message.text)
        if not dry_run:
            engagement_db.set_message(
                message=engagement_db_message,
                origin=HistoryEntryOrigin(origin_name="Set coda_id", details={}),
                transaction=transaction
            )
    assert engagement_db_message.coda_id == SHAUtils.sha_string(engagement_db_message.text)

    # Look-up this message in Coda
    coda_message = coda.get_dataset_message(dataset_config.coda_dataset_id, engagement_db_message.coda_id)

    # If the message exists in Coda, update the database message based on the labels assigned in Coda
    if coda_message is not None:
        log.debug("Message already exists in Coda")
        update_sync_events = _update_engagement_db_message_from_coda_message(
            engagement_db, engagement_db_message, coda_message, coda_config, transaction=transaction, dry_run=dry_run
        )
        sync_stats.add_events(update_sync_events)
        return engagement_db_message, sync_stats

    # The message isn't in Coda, so add it
    sync_stats.add_event(CodaSyncEvents.ADD_MESSAGE_TO_CODA)
    _add_message_to_coda(coda, dataset_config, coda_config.ws_correct_dataset_code_scheme, engagement_db_message, dry_run)

    return engagement_db_message, sync_stats


def _sync_engagement_db_dataset_to_coda(engagement_db, coda, coda_config, dataset_config, cache, dry_run=False):
    """
    Syncs messages from one engagement database dataset to Coda.

    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param coda: Coda instance to sync the message to.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param coda_config: Coda sync configuration.
    :type coda_config: src.engagement_db_coda_sync.configuration.CodaSyncConfiguration
    :param dataset_config: Configuration for the dataset to sync.
    :type dataset_config: src.engagement_db_coda_sync.configuration.CodaDatasetConfiguration
    :param cache: Coda sync cache.
    :type cache: src.engagement_db_coda_sync.cache.CodaSyncCache | None
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return: Sync stats for the update.
    :rtype: src.engagement_db_coda_sync.sync_stats.EngagementDBToCodaSyncStats
    """
    last_seen_message = None if cache is None else cache.get_last_seen_message(dataset_config.engagement_db_dataset)
    synced_messages = 0
    synced_message_ids = set()

    sync_stats = EngagementDBToCodaSyncStats()

    first_run = True
    while first_run or last_seen_message is not None:
        first_run = False

        last_seen_message, message_sync_stats = _sync_next_engagement_db_message_to_coda(
            engagement_db.transaction(), engagement_db, coda, coda_config, dataset_config, last_seen_message, dry_run
        )
        sync_stats.add_stats(message_sync_stats)

        if last_seen_message is not None:
            synced_messages += 1
            synced_message_ids.add(last_seen_message.message_id)
            if cache is not None and not dry_run:
                cache.set_last_seen_message(dataset_config.engagement_db_dataset, last_seen_message)

            # We can see the same message twice in a run if we need to set a coda id, labels, or do WS correction,
            # because in these cases we'll write back to one of the retrieved documents.
            # Log both the number of message objects processed and the number of unique message ids seen so we can
            # monitor both.
            log.info(f"Synced {synced_messages} message objects ({len(synced_message_ids)} unique message ids) in "
                     f"dataset {dataset_config.engagement_db_dataset}")
        else:
            log.info(f"No more new messages in dataset {dataset_config.engagement_db_dataset}")

    return sync_stats


def sync_engagement_db_to_coda(engagement_db, coda, coda_config, cache_path=None, dry_run=False):
    """
    Syncs messages from an engagement database to Coda.

    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param coda: Coda instance to sync the message to.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param coda_config: Coda sync configuration.
    :type coda_config: src.engagement_db_coda_sync.configuration.CodaSyncConfiguration
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str | None
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    """
    # Initialise the cache
    if cache_path is None:
        cache = None
        log.warning(f"No `cache_path` provided. This tool will process all relevant messages from all of time")
    else:
        log.info(f"Initialising Coda sync cache at '{cache_path}/engagement_db_to_coda'")
        cache = CodaSyncCache(f"{cache_path}/engagement_db_to_coda")

    if dry_run:
        log.warning("Running without --dry-run may cause more reads than suggested here, because any update made to "
                    "an engagement db message when syncing it will result in it being synced again")

    # Sync each dataset in turn to Coda
    dataset_to_sync_stats = dict()  # of engagement db dataset -> EngagementDBToCodaSyncStats
    for dataset_config in coda_config.dataset_configurations:
        log.info(f"Syncing engagement db dataset {dataset_config.engagement_db_dataset} to Coda dataset "
                 f"{dataset_config.coda_dataset_id}...")
        dataset_sync_stats = _sync_engagement_db_dataset_to_coda(engagement_db, coda, coda_config, dataset_config, cache, dry_run)
        dataset_to_sync_stats[dataset_config.engagement_db_dataset] = dataset_sync_stats

    # Log the summaries of actions taken for each dataset then for all datasets combined.
    all_sync_stats = EngagementDBToCodaSyncStats()
    for dataset_config in coda_config.dataset_configurations:
        log.info(f"Summary of actions for engagement db dataset '{dataset_config.engagement_db_dataset}':")
        dataset_to_sync_stats[dataset_config.engagement_db_dataset].print_summary()
        all_sync_stats.add_stats(dataset_to_sync_stats[dataset_config.engagement_db_dataset])

    dry_run_text = "(dry run)" if dry_run else ""
    log.info(f"Summary of actions for all datasets {dry_run_text}:")
    all_sync_stats.print_summary()
