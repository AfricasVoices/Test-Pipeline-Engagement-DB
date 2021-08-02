from core_data_modules.logging import Logger
from engagement_database.data_models import MessageStatuses
from google.cloud import firestore

from src.engagement_db_coda_sync.cache import CodaSyncCache
from src.engagement_db_coda_sync.lib import _update_engagement_db_message_from_coda_message

log = Logger(__name__)


@firestore.transactional
def _sync_coda_message_to_engagement_db(transaction, coda_message, engagement_db, engagement_db_dataset, coda_config):
    """
    Syncs a coda message to an engagement database, by downloading all the engagement database messages which match the
    coda message's id and dataset, and making sure the labels match.

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
    """
    # Get the messages in the engagement database that match this dataset and coda message id
    engagement_db_messages = engagement_db.get_messages(
        query_filter=lambda q: q
            .where("dataset", "==", engagement_db_dataset)
            .where("coda_id", "==", coda_message.message_id)
            .where("status", "in", [MessageStatuses.LIVE, MessageStatuses.STALE]),
        transaction=transaction
    )
    log.info(f"{len(engagement_db_messages)} engagement db message(s) match Coda message {coda_message.message_id}")

    # Update each of the matching messages with the labels currently in Coda.
    for i, engagement_db_message in enumerate(engagement_db_messages):
        log.info(f"Processing matching engagement message {i + 1}/{len(engagement_db_messages)}: "
                 f"{engagement_db_message.message_id}...")
        _update_engagement_db_message_from_coda_message(
            engagement_db, engagement_db_message, coda_message, coda_config, transaction=transaction)


def _sync_coda_dataset_to_engagement_db(coda, engagement_db, coda_config, dataset_config, cache=None):
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
    """
    log.info(f"Getting messages from Coda dataset {dataset_config.coda_dataset_id}...")
    coda_messages = coda.get_dataset_messages(
        dataset_config.coda_dataset_id,
        last_updated_after=None if cache is None else cache.get_last_updated_timestamp(dataset_config.coda_dataset_id)
    )

    for i, coda_message in enumerate(coda_messages):
        log.info(f"Processing Coda message {i + 1}/{len(coda_messages)}: {coda_message.message_id}...")
        _sync_coda_message_to_engagement_db(
            engagement_db.transaction(), coda_message, engagement_db, dataset_config.engagement_db_dataset,
            coda_config
        )

    seen_timestamps = [msg.last_updated for msg in coda_messages if msg.last_updated is not None]
    if cache is not None and len(seen_timestamps) > 0:
        most_recently_updated_timestamp = sorted(seen_timestamps)[-1]
        cache.set_last_updated_timestamp(dataset_config.coda_dataset_id, most_recently_updated_timestamp)


def sync_coda_to_engagement_db(coda, engagement_db, coda_config, cache_path=None):
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
    """
    # Initialise the cache
    if cache_path is None:
        cache = None
        log.warning(f"No `cache_path` provided. This tool will process all relevant Coda messages from all of time")
    else:
        log.info(f"Initialising Coda sync cache at '{cache_path}/coda_to_engagement_db'")
        cache = CodaSyncCache(f"{cache_path}/coda_to_engagement_db")

    # Sync each Coda dataset to the engagement db in turn
    for dataset_config in coda_config.dataset_configurations:
        log.info(f"Syncing Coda dataset {dataset_config.coda_dataset_id} to engagement db dataset "
                 f"{dataset_config.coda_dataset_id}")
        _sync_coda_dataset_to_engagement_db(coda, engagement_db, coda_config, dataset_config, cache)
