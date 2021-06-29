from dateutil.parser import isoparse

from core_data_modules.logging import Logger
from core_data_modules.util import TimeUtils
from core_data_modules.traced_data import TracedData, Metadata

from src.engagement_db_to_analysis.cache import AnalysisCache
from src.engagement_db_to_analysis import MessageFilters

log = Logger(__name__)


def serialise_message(msg):
    msg = msg.to_dict()
    msg["timestamp"] = msg["timestamp"].isoformat()
    msg["last_updated"] = msg["last_updated"].isoformat()

    return msg


def _get_project_messages_from_engagement_db(dataset_configurations, engagement_db, cache_path=None):
    """

    Downloads project messages from engagement database. It performs a full download if there is no previous export and
    incrementally otherwise.

    :param pipeline_config: Dataset configuration in pipeline configuration module.
    :type pipeline_config: pipeline_config.coda_sync.sync_config.dataset_configurations
    :param engagement_db: Engagement database to download the messages from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param cache_path: Directory to use for the fetch cache, containing engagement_db dataset files and a timestamp generated from a previous run.
    :type cache_path: str
    :return: engagement_db_dataset_messages_map of engagement_db_dataset to list of messages.
    :rtype: dict of str -> list of Message
    """

    log.info(f"Initialising EngagementAnalysisCache at'{cache_path}'")
    cache = AnalysisCache(cache_path)

    engagement_db_dataset_messages_map = {}  # of engagement_db_dataset to list of messages
    for dataset_config in dataset_configurations:

        engagement_db_dataset = dataset_config.engagement_db_dataset

        messages = []
        latest_message_timestamp = cache.get_latest_message_timestamp(engagement_db_dataset)
        if latest_message_timestamp is not None:
            log.info(f"Performing incremental download for {engagement_db_dataset} messages...")

            # Download messages that have been updated/created after the previous run
            incremental_messages_filter = lambda q: q \
                .where("dataset", "==", engagement_db_dataset) \
                .where("last_updated", ">", latest_message_timestamp)

            messages.extend(
                serialise_message(msg) for msg in engagement_db.get_messages(filter=incremental_messages_filter))

            # Check and remove cache messages that have been ws corrected after the previous run
            ws_corrected_messages_filter = lambda q: q \
                .where("previous_datasets", "array_contains", engagement_db_dataset) \
                .where("last_updated", ">", latest_message_timestamp)

            ws_corrected_messages = engagement_db.get_messages(filter=ws_corrected_messages_filter)

            cache_messages = cache.get_previous_export_messages(engagement_db_dataset)
            for msg in cache_messages:
                if msg["message_id"] in {msg.message_id for msg in ws_corrected_messages}:
                    continue
                messages.append(msg)

        else:
            log.warning(f"Performing a full download for {engagement_db_dataset} messages...")

            full_download_filter = lambda q: q \
                .where("dataset", "==", engagement_db_dataset)

            messages.extend(
                serialise_message(msg) for msg in engagement_db.get_messages(filter=full_download_filter))

        engagement_db_dataset_messages_map[engagement_db_dataset] = messages

        # Update latest_message_timestamp
        for msg in messages:
            if latest_message_timestamp is None or isoparse(msg["last_updated"]) > latest_message_timestamp:
                latest_message_timestamp = isoparse(msg["last_updated"])

        # Export latest message timestamp to cache
        if latest_message_timestamp is not None or len(messages) > 0:
            cache.set_latest_message_timestamp(engagement_db_dataset, latest_message_timestamp)

        # Export project engagement_dataset files
        cache.export_engagement_db_dataset(engagement_db_dataset, messages)

    return engagement_db_dataset_messages_map

def _convert_messages_to_traced_data(user, messages_map):
    #TODO: add docsting

    data = []
    for engagement_db_dataset in messages_map:

        engagement_db_dataset_messages = messages_map[engagement_db_dataset]
        for msg in engagement_db_dataset_messages:
            data.append(
                TracedData(msg, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())))

    log.info(f"Converted {len(data)} raw messages to TracedData")

    return data


def _filter_messages(user, data,  pipeline_config):
        #TODO: add docsting

    # Filter out test messages sent by Test Contacts.
    if pipeline_config.filter_test_messages:
        data = MessageFilters.filter_test_messages(user, data)
    else:
        log.debug("Not filtering out test messages (because the pipeline_config.filter_test_messages was set to false)")

    # Filter out runs sent outwith the project start and end dates
    data = MessageFilters.filter_time_range(user, data, pipeline_config)

    return data

#TODO: Fold messages by uid
def _fold_messages_by_uid():
    return None

def generate_analysis_files(user, pipeline_config, engagement_db, engagement_db_datasets_cache_dir):

    messages_map = _get_project_messages_from_engagement_db(pipeline_config.analysis_config, engagement_db,
                                               engagement_db_datasets_cache_dir)

    data = _convert_messages_to_traced_data(user, messages_map)

    data = _filter_messages(user, data, pipeline_config)

    return data

