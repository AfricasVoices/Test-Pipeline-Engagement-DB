from core_data_modules.logging import Logger
from core_data_modules.util import TimeUtils
from core_data_modules.traced_data import TracedData, Metadata

from src.engagement_db_to_analysis.cache import AnalysisCache
from src.engagement_db_to_analysis.traced_data_filters import filter_messages


log = Logger(__name__)


def _get_project_messages_from_engagement_db(analysis_configurations, engagement_db, cache_path=None):
    """
    Downloads project messages from engagement database.

    :param analysis_config: Analysis dataset configuration in pipeline configuration module.
    :type analysis_config: pipeline_config.analysis_config
    :param engagement_db: Engagement database to download the messages from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str
    :return: engagement_db_dataset_messages_map of engagement_db_dataset to list of messages.
    :rtype: dict of str -> list of engagement_database.data_models.Message
    """

    log.info(f"Initialising EngagementAnalysisCache at '{cache_path}/engagement_db_to_analysis'")
    cache = AnalysisCache(f"{cache_path}/engagement_db_to_analysis")

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

                messages.extend(engagement_db.get_messages(filter=incremental_messages_filter))

                # Check and remove cache messages that have been ws corrected after the previous run
                ws_corrected_messages_filter = lambda q: q \
                    .where("previous_datasets", "array_contains", engagement_db_dataset) \
                    .where("last_updated", ">", latest_message_timestamp)

                ws_corrected_messages = engagement_db.get_messages(filter=ws_corrected_messages_filter)

                cache_messages = cache.get_messages(engagement_db_dataset)
                for msg in cache_messages:
                    if msg.message_id in {msg.message_id for msg in ws_corrected_messages}:
                        continue
                    messages.append(msg)

            else:
                log.warning(f"Performing a full download for {engagement_db_dataset} messages...")

                full_download_filter = lambda q: q \
                    .where("dataset", "==", engagement_db_dataset)

                messages.extend(engagement_db.get_messages(filter=full_download_filter))

            engagement_db_dataset_messages_map[engagement_db_dataset] = messages

            # Update latest_message_timestamp
            for msg in messages:
                msg_last_updated = msg.last_updated
                if latest_message_timestamp is None or msg_last_updated > latest_message_timestamp:
                    latest_message_timestamp = msg_last_updated

            # Export latest message timestamp to cache
            if latest_message_timestamp is not None or len(messages) > 0:
                cache.set_latest_message_timestamp(engagement_db_dataset, latest_message_timestamp)

            # Export project engagement_dataset files
            cache.set_messages(engagement_db_dataset, messages)

    return engagement_db_dataset_messages_map


def _convert_messages_to_traced_data(user, messages_map):
    """
    Converts messages dict objects to TracedData objects.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_map: Dict of engagement db dataset -> list of Messages in that dataset.
    :type messages_map: dict of str -> list of engagement_database.data_models.Message
    :return: A list of Traced data message objects.
    :type: list of Traced data
    """
    messages_traced_data = []
    for engagement_db_dataset in messages_map:
        engagement_db_dataset_messages = messages_map[engagement_db_dataset]
        for msg in engagement_db_dataset_messages:
            messages_traced_data.append(TracedData(
                msg.to_dict(serialize_datetimes_to_str=True),
                Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
            ))

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

    participants_traced_data_map = {}
    for message in messages_traced_data:
        participant_uuid = message["participant_uuid"]
        message_dataset = message["dataset"]

        # Create an empty TracedData for this participant if this participant hasn't been seen yet.
        if participant_uuid not in participants_traced_data_map.keys():
            participants_traced_data_map[participant_uuid] = \
                TracedData({}, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        # Get the existing list of messages for this dataset, if it exists, otherwise initialise with []
        participant_td = participants_traced_data_map[participant_uuid]
        participant_dataset_messages = participant_td.get(message_dataset, [])

        # Append this message to the list of messages for this dataset, and write-back to TracedData.
        participant_dataset_messages = participant_dataset_messages.copy()
        participant_dataset_messages.append(dict(message))
        participant_td.append_data(
            {message_dataset: participant_dataset_messages},
            Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
        )
        # Append the message's traced data, as it contains the history of which filters were passed.
        message.hide_keys(message.keys(), Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))
        participant_td.append_traced_data(
            "message_history", message,
            Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
        )

    return participants_traced_data_map


def generate_analysis_files(user, pipeline_config, engagement_db, cache_path=None):

    messages_map = _get_project_messages_from_engagement_db(pipeline_config.analysis_config, engagement_db, cache_path)

    messages_traced_data = _convert_messages_to_traced_data(user, messages_map)

    messages_traced_data = filter_messages(user, messages_traced_data, pipeline_config)

    participants_traced_data_map = _fold_messages_by_uid(user, messages_traced_data)

    return participants_traced_data_map
