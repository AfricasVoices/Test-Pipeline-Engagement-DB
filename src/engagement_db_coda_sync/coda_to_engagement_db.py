from core_data_modules.logging import Logger
from engagement_database.data_models import MessageStatuses
from google.cloud import firestore

from src.engagement_db_coda_sync.cache import CodaSyncCache
from src.engagement_db_coda_sync.lib import _update_engagement_db_message_from_coda_message
from src.engagement_db_coda_sync.sync_stats import CodaToEngagementDBSyncStats, CodaSyncEvents

log = Logger(__name__)


@firestore.transactional
def _sync_coda_message_to_engagement_db_batch(transaction, coda_message, engagement_db, engagement_db_dataset,
                                              coda_config, start_after=None, dry_run=False):
    """
    Syncs a Coda message to a batch of up to 250 engagement database messages.

    :param transaction: Transaction in the engagement database to perform the update in.
    :type transaction: google.cloud.firestore.Transaction
    :param coda_message: Coda Message to sync.
    :type coda_message: core_data_modules.data_models.Message
    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param engagement_db_dataset: Dataset in the engagement database to update.
    :type engagement_db_dataset: str
    :param coda_config: Configuration for the update.
    :type coda_config: src.engagement_db_coda_sync.configuration.CodaSyncConfiguration
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return Sync stats.
    :rtype src.engagement_db_coda_sync.sync_stats.CodaToEngagementDBSyncStats
    :param start_after: Engagement database message to start this batch after.
    :type start_after: engagement_database.data_models.Message
    :return: Tuple of:
                1. Next start_after message, or None. If a message, pass this into the next call to run the next
                   batch correctly. If None, we've fetched the last message this batch so there are no further batches
                   to run on.
                2. Sync stats for this sync operation.
    :rtype: (engagement_database.data_models.Message | None,
             src.engagement_db_coda_sync.sync_stats.CodaToEngagementDBSyncStats)
    """
    sync_stats = CodaToEngagementDBSyncStats()

    if start_after is None:
        start_after_dict = {"last_updated": None, "message_id": None}
    else:
        start_after_dict = {"last_updated": start_after.last_updated, "message_id": start_after.message_id}

    engagement_db_messages = engagement_db.get_messages(
        firestore_query_filter=lambda q: q
            .where("dataset", "==", engagement_db_dataset)
            .where("coda_id", "==", coda_message.message_id)
            .where("status", "in", [MessageStatuses.LIVE, MessageStatuses.STALE])
            .order_by("last_updated")
            .order_by("message_id")
            .start_after(start_after_dict)
            .limit(250),
        transaction=transaction
    )
    log.info(f"{len(engagement_db_messages)} engagement db message(s) match Coda message {coda_message.message_id} "
             f"in this batch")

    for _ in engagement_db_messages:
        sync_stats.add_event(CodaSyncEvents.READ_MESSAGE_FROM_ENGAGEMENT_DB)

    # Update each of the matching messages with the labels currently in Coda.
    for i, engagement_db_message in enumerate(engagement_db_messages):
        log.info(f"Processing matching engagement message {i + 1}/{len(engagement_db_messages)}: "
                 f"{engagement_db_message.message_id}...")
        message_sync_events = _update_engagement_db_message_from_coda_message(
            engagement_db, engagement_db_message, coda_message, coda_config, transaction=transaction, dry_run=dry_run
        )
        sync_stats.add_events(message_sync_events)

    # If we downloaded a full-batch worth of messages, return a next_start_after document so the calling function
    # knows to request another batch of messages to be updated.
    next_start_after = None
    if len(engagement_db_messages) == 250:
        next_start_after = engagement_db_messages[-1]
    return next_start_after, sync_stats


def _sync_coda_message_to_engagement_db(coda_message, engagement_db, engagement_db_dataset, coda_config, dry_run=False):
    """
    Syncs a coda message to an engagement database, by downloading all the engagement database messages which match the
    coda message's id and dataset, and making sure the labels match.

    :param coda_message: Coda Message to sync.
    :type coda_message: core_data_modules.data_models.Message
    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param engagement_db_dataset: Dataset in the engagement database to update.
    :type engagement_db_dataset: str
    :param coda_config: Configuration for the update.
    :type coda_config: src.engagement_db_coda_sync.configuration.CodaSyncConfiguration
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return Sync stats.
    :rtype src.engagement_db_coda_sync.sync_stats.CodaToEngagementDBSyncStats
    """
    sync_stats = CodaToEngagementDBSyncStats()

    # Sync the coda message by fetching and updating the matching engagement db messages in 1 or more batches.
    # (A multiple-batch approach is needed because the number of matching messages may exceed the Firestore batch limit)
    start_after = None
    first_run = True
    batches = 0
    while first_run or start_after is not None:
        first_run = False
        start_after, batch_sync_stats = _sync_coda_message_to_engagement_db_batch(
            engagement_db.transaction(), coda_message, engagement_db, engagement_db_dataset, coda_config, start_after, dry_run
        )
        sync_stats.add_stats(batch_sync_stats)
        batches += 1
        log.info(f"Synced {batches} batch(es) of engagement_db messages for coda_message {coda_message.message_id}")

    return sync_stats


def _sync_coda_dataset_to_engagement_db(coda, engagement_db, coda_config, dataset_config, cache=None, dry_run=False):
    """
    Syncs messages from one Coda dataset to an engagement database.
    
    :param coda: Coda instance to sync from.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param engagement_db: Engagement database to sync to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param coda_config: Coda sync configuration.
    :type coda_config: src.engagement_db_coda_sync.configuration.CodaSyncConfiguration
    :param cache: Coda sync cache.
    :type cache: src.engagement_db_coda_sync.cache.CodaSyncCache | None
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return Sync stats for the update.
    :rtype: src.engagement_db_coda_sync.sync_stats.CodaToEngagementDBSyncStats
    """
    log.info(f"Getting messages from Coda dataset {dataset_config.coda_dataset_id}...")

    sync_stats = CodaToEngagementDBSyncStats()

    coda_messages = coda.get_dataset_messages(
        dataset_config.coda_dataset_id,
        last_updated_after=None if cache is None else cache.get_last_updated_timestamp(dataset_config.coda_dataset_id)
    )
    for _ in coda_messages:
        sync_stats.add_event(CodaSyncEvents.READ_MESSAGE_FROM_CODA)

    coda_messages.sort(key=lambda msg: msg.last_updated)

    for i, coda_message in enumerate(coda_messages):
        log.info(f"Processing Coda message {i + 1}/{len(coda_messages)}: {coda_message.message_id}...")
        message_sync_stats = _sync_coda_message_to_engagement_db(
            coda_message, engagement_db, dataset_config.engagement_db_dataset, coda_config, dry_run
        )
        sync_stats.add_stats(message_sync_stats)

        # If there's a cache and we've read the last message, or the next message's last updated timestamp is greater
        # than the message we are currently syncing, update the cache.

        have_read_last_message = (i == len(coda_messages) - 1)
        # Note that this ensures we don't update the time-based cache when we are processing messages with the same timestamp.
        if not have_read_last_message:
            has_timestamp_changed = coda_messages[i + 1].last_updated > coda_message.last_updated

        if not dry_run and cache is not None and (have_read_last_message or has_timestamp_changed):
            cache.set_last_updated_timestamp(dataset_config.coda_dataset_id, coda_message.last_updated)

    return sync_stats


def sync_coda_to_engagement_db(coda, engagement_db, coda_config, cache_path=None, dry_run=False):
    """
    Syncs messages from Coda to an engagement database.

    :param coda: Coda instance to sync from.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param engagement_db: Engagement database to sync to.
    :type engagement_db: engagement_database.EngagementDatabase
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
        log.warning(f"No `cache_path` provided. This tool will process all relevant Coda messages from all of time")
    else:
        log.info(f"Initialising Coda sync cache at '{cache_path}/coda_to_engagement_db'")
        cache = CodaSyncCache(f"{cache_path}/coda_to_engagement_db")

    # Sync each Coda dataset to the engagement db in turn
    dataset_to_sync_stats = dict()  # of coda dataset id -> CodaToEngagementDBSyncStats
    for dataset_config in coda_config.dataset_configurations:
        log.info(f"Syncing Coda dataset {dataset_config.coda_dataset_id} to engagement db dataset "
                 f"{dataset_config.coda_dataset_id}")
        dataset_sync_stats = _sync_coda_dataset_to_engagement_db(coda, engagement_db, coda_config, dataset_config, cache, dry_run)
        dataset_to_sync_stats[dataset_config.coda_dataset_id] = dataset_sync_stats

    # Log the summaries of actions taken for each dataset then for all datasets combined.
    all_sync_stats = CodaToEngagementDBSyncStats()
    for dataset_config in coda_config.dataset_configurations:
        log.info(f"Summary of actions for Coda dataset '{dataset_config.coda_dataset_id}':")
        dataset_to_sync_stats[dataset_config.coda_dataset_id].print_summary()
        all_sync_stats.add_stats(dataset_to_sync_stats[dataset_config.coda_dataset_id])

    dry_run_text = "(dry run)" if dry_run else ""
    log.info(f"Summary of actions for all datasets {dry_run_text}:")
    all_sync_stats.print_summary()
