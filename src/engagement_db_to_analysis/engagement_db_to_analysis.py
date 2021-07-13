from dateutil.parser import isoparse

from core_data_modules.logging import Logger
from core_data_modules.util import TimeUtils
from core_data_modules.traced_data import TracedData, Metadata

from src.engagement_db_to_analysis.cache import AnalysisCache
from src.engagement_db_to_analysis.traced_data_filters import filter_messages

log = Logger(__name__)

# Todo move to Pipeline Infrastructure
def serialise_message(msg):
    msg = msg.to_dict()
    msg["timestamp"] = msg["timestamp"].isoformat()
    msg["last_updated"] = msg["last_updated"].isoformat()

    return msg


def _get_project_messages_from_engagement_db(analysis_configurations, engagement_db, cache_path):
    """

    Downloads project messages from engagement database. It performs a full download if there is no previous export and
    incrementally otherwise.

    :param analysis_config: Analysis dataset configuration in pipeline configuration module.
    :type analysis_config: pipeline_config.analysis_config
    :param engagement_db: Engagement database to download the messages from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param cache_path: Directory to use for the fetch cache, containing engagement_db dataset files and a timestamp generated from a previous run.
    :type cache_path: str
    :return: engagement_db_dataset_messages_map of engagement_db_dataset to list of messages.
    :rtype: dict of str -> list of Message
    """

    log.info(f"Initialising EngagementAnalysisCache at '{cache_path}'")
    cache = AnalysisCache(cache_path)

    engagement_db_dataset_messages_map = {}  # of engagement_db_dataset to list of messages
    for config in analysis_configurations:
        for engagement_db_dataset in config.engagement_db_datasets:

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
                msg_last_updated = isoparse(msg["last_updated"])
                if latest_message_timestamp is None or msg_last_updated > latest_message_timestamp:
                    latest_message_timestamp = msg_last_updated

            # Export latest message timestamp to cache
            if latest_message_timestamp is not None or len(messages) > 0:
                cache.set_latest_message_timestamp(engagement_db_dataset, latest_message_timestamp)

            # Export project engagement_dataset files
            cache.export_engagement_db_dataset(engagement_db_dataset, messages)

    return engagement_db_dataset_messages_map

def _convert_messages_to_traced_data(user, messages_map):
    """
    Converts messages dict objects to TracedData objects.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_map: Dict containing messages data.
    :type messages_map: dict
    :return: A list of Traced data message objects.
    :type: list of Traced data
    """

    messages_traced_data = []
    for engagement_db_dataset in messages_map:

        engagement_db_dataset_messages = messages_map[engagement_db_dataset]
        for msg in engagement_db_dataset_messages:
            messages_traced_data.append(
                TracedData(msg, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())))

    log.info(f"Converted {len(messages_traced_data)} raw messages to TracedData")

    return messages_traced_data

def _fold_messages_by_uid(user, messages_traced_data):
    """
    Groups Messages TracedData objects into Individual TracedData objects.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to group.
    :type messages_traced_data: list of TracedData
    :return: Individual TracedData objects.
    :rtype: dict of uid -> individual TracedData objects.
    """

    participants_traced_data = {}
    for message in messages_traced_data:

        participant_uuid = message["participant_uuid"]
        message_dataset = message["dataset"]

        if message["participant_uuid"] not in participants_traced_data.keys():

            participant_td = TracedData({message_dataset: [message.serialize()]}, Metadata(user,
                                                                                           Metadata.get_call_location(),
                                                                                           TimeUtils.utc_now_as_iso_string()))
            participants_traced_data[participant_uuid] = participant_td

        else:

            if message_dataset in participants_traced_data[participant_uuid].keys():
                message_dataset_map = participants_traced_data[participant_uuid].get(message_dataset)
                message_dataset_map_copy = message_dataset_map.copy()
                message_dataset_map_copy.append(message.serialize())

                participants_traced_data[participant_uuid].append_data({message_dataset: message_dataset_map_copy},
                                                                       Metadata(user, Metadata.get_call_location(),
                                                                                TimeUtils.utc_now_as_iso_string()))
            else:
                participants_traced_data[participant_uuid].append_data({message_dataset: [message.serialize()]},
                                        Metadata(user,
                                                 Metadata.get_call_location(),
                                                 TimeUtils.utc_now_as_iso_string()))
    return  participants_traced_data

def generate_analysis_files(user, pipeline_config, engagement_db, engagement_db_datasets_cache_dir):

    messages_map = _get_project_messages_from_engagement_db(pipeline_config.analysis_config, engagement_db,
                                               engagement_db_datasets_cache_dir)

    messages_traced_data = _convert_messages_to_traced_data(user, messages_map)

    messages_traced_data = filter_messages(user, messages_traced_data, pipeline_config)

    participants_traced_data = _fold_messages_by_uid(user, messages_traced_data)

    return participants_traced_data
