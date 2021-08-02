from core_data_modules.analysis import AnalysisConfiguration, engagement_counts
from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO, TracedDataCSVIO
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies
from core_data_modules.util import TimeUtils
from engagement_database.data_models import Message

from src.engagement_db_to_analysis.cache import AnalysisCache
from src.engagement_db_to_analysis.configuration import DatasetTypes
from src.engagement_db_to_analysis.traced_data_filters import filter_messages

log = Logger(__name__)


def _get_project_messages_from_engagement_db(analysis_configurations, engagement_db, cache_path=None):
    """
    Downloads project messages from engagement database. It performs a full download if there is no cache path and
    incrementally otherwise.

    :param analysis_config: Analysis dataset configuration in pipeline configuration module.
    :type analysis_config: pipeline_config.analysis_configs
    :param engagement_db: Engagement database to download the messages from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str
    :return: engagement_db_dataset_messages_map of engagement_db_dataset to list of messages.
    :rtype: dict of str -> list of engagement_database.data_models.Message
    """

    if cache_path is None:
        cache = None
        log.warning(f"No `cache_path` provided. This tool will perform a full download of project messages from engagement database")
    else:
        log.info(f"Initialising EngagementAnalysisCache at '{cache_path}/engagement_db_to_analysis'")
        cache = AnalysisCache(f"{cache_path}/engagement_db_to_analysis")

    engagement_db_dataset_messages_map = {}  # of engagement_db_dataset to list of messages
    for analysis_dataset_config in analysis_configurations:
        for engagement_db_dataset in analysis_dataset_config.engagement_db_datasets:
            messages = []
            latest_message_timestamp = None if cache is None else cache.get_latest_message_timestamp(engagement_db_dataset)
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

            if cache is not None:
                # Export latest message timestamp to cache
                if latest_message_timestamp is not None:
                    cache.set_latest_message_timestamp(engagement_db_dataset, latest_message_timestamp)

                # Export project engagement_dataset files
                if len(messages) > 0:
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


def _analysis_dataset_config_to_column_config(analysis_dataset_config):
    """
    Converts an analysis dataset configuration to the relevant "column-view" configurations.

    The analysis dataset configuration is the normalised configuration that's specified in the pipeline configuration
    files, and works great for processing individual messages.

    The "column-view" configurations describe how to reconfigure the dataset for the final stages of analysis, where
    we are less interested in individual messages, but in the labelled opinions, collated by labelled dataset, by
    participant, or by project.

    :param analysis_dataset_config: Analysis dataset configuration to convert.
    :type analysis_dataset_config: src.engagement_db_to_analysis.configuration.AnalysisDatasetConfiguration
    :return: List of all the column configurations for this analysis dataset configuration.
    :rtype: list of core_data_modules.analysis.analysis_utils.AnalysisConfiguration
    """
    column_configs = []
    for coding_config in analysis_dataset_config.coding_configs:
        config = AnalysisConfiguration(
            dataset_name=coding_config.analysis_dataset,
            raw_field=analysis_dataset_config.raw_dataset,
            coded_field=f"{coding_config.analysis_dataset}_labels",
            code_scheme=coding_config.code_scheme
        )
        column_configs.append(config)
    return column_configs


def _analysis_dataset_config_for_message(analysis_dataset_configs, message):
    """
    Gets the analysis dataset configuration to use to process this message, by looking-up the configuration that refers
    to this message's engagement db "dataset" property.

    :param analysis_dataset_configs: Dataset configurations to search for the one that relates to the given message.
    :type analysis_dataset_configs: list of src.engagement_db_to_analysis.configuration.AnalysisDatasetConfiguration
    :param message: Message to retrieve the analysis dataset configuration for.
    :type message: engagement_database.data_models.Message
    :return: Analysis dataset configuration to use for this message.
    :rtype: src.engagement_db_to_analysis.configuration.AnalysisDatasetConfiguration
    """
    for config in analysis_dataset_configs:
        if message.dataset in config.engagement_db_datasets:
            return config
    raise ValueError(f"No analysis dataset configuration found for message '{message.message_id}', which has engagement"
                     f"db dataset {message.dataset}")


def _get_normalised_labels_for_code_scheme(message, code_scheme):
    """
    Gets the labels assigned to this message under the given `code_scheme` (or a duplicate of this code scheme),
    with label scheme_id's normalised to the primary code scheme id in cases where the code scheme was duplicated
    e.g. scheme_id 'scheme-abc123-1' will be re-written to 'scheme-abc123'.

    :param message: Message to get the labels from.
    :type message: engagement_database.data_models.Message
    :param code_scheme: Code scheme to get the latest labels for.
    :type code_scheme: core_data_modules.data_models.CodeScheme
    :return: List of the relevant, normalised labels for the given code scheme.
    :rtype: list of core_data_modules.data_models.Label
    """
    relevant_labels = []
    for label in message.get_latest_labels():
        if label.scheme_id.startswith(code_scheme.scheme_id):
            label.scheme_id = code_scheme.scheme_id
            relevant_labels.append(label)
    return relevant_labels


def _add_message_to_column_td(user, message_td, column_td, analysis_config):
    """
    Adds a message to a "column-view" TracedData object in-place.

    This function adds this message's text and labels fields to an existing TracedData object in column-view format.
    In cases where data already exists in the column-view columns:
     - Raw texts are handled by concatenation, by `core_data_modules.util.fold_traced_data.FoldStrategies.concatenate`.
     - Labels are handled by `core_data_modules.util.fold_traced_data.FoldStrategies.list_of_labels`.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param message_td: TracedData representing the message to add
    :type message_td: core_data_modules.traced_data.TracedData
    :param column_td: An existing TracedData object in column-view format, to which the relevant data from this message
                      will be appended.
    :type column_td: core_data_modules.traced_data.TracedData
    :param analysis_config: Analysis configuration.
    :type analysis_config: src.engagement_db_to_analysis.configuration.AnalysisConfiguration
    """
    message = Message.from_dict(dict(message_td))

    # Get the analysis dataset configuration for this message
    analysis_dataset_config = _analysis_dataset_config_for_message(analysis_config.dataset_configurations, message)

    # Convert the analysis dataset config to its "column-view" configurations
    column_configs = _analysis_dataset_config_to_column_config(analysis_dataset_config)

    new_data = dict()

    # Add this message's raw text to the the raw_dataset column in the column-view TracedData.
    # If the TracedData already contains data here, append this text to the existing, previously added texts by
    # concatenating.
    existing_text = column_td.get(analysis_dataset_config.raw_dataset)
    if existing_text is None:
        new_data[analysis_dataset_config.raw_dataset] = message.text
    else:
        new_data[analysis_dataset_config.raw_dataset] = FoldStrategies.concatenate(existing_text, message.text)

    # For each column config, get the latest, normalised labels under that column config's code scheme, and them
    # to the column-view TracedData.
    # If the TracedData already contains data here, combine the labels with the existing labels using
    # FoldStrategies.list_of_labels.
    for column_config in column_configs:
        relevant_message_labels = _get_normalised_labels_for_code_scheme(message, column_config.code_scheme)

        if len(relevant_message_labels) == 0:
            continue

        relevant_message_labels = [label.to_dict() for label in relevant_message_labels]  # serialize for TracedData
        existing_labels = column_td.get(column_config.coded_field)
        if existing_labels is None:
            new_data[column_config.coded_field] = relevant_message_labels
        else:
            new_data[column_config.coded_field] = FoldStrategies.list_of_labels(
                column_config.code_scheme, existing_labels, relevant_message_labels
            )

    # Append the TracedData history for this message to the column-view.
    message_td = message_td.copy()
    message_td.hide_keys(message_td.keys(), Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))
    column_td.append_traced_data("appended_message", message_td, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    # Write the new data to the column-view TracedData
    # (we do this after appending the message_td so the TracedData is slightly easier to read)
    column_td.append_data(new_data, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))


def _convert_to_messages_column_format(user, messages_traced_data, analysis_config):
    """
    Converts a list of messages traced data into "column-view" format by rqa-message.

    Returns one "column-view" TracedData object for each rqa message, with all demographic messages from the same
    participant appended to each TracedData.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages traced data to convert.
    :type messages_traced_data: list of core_data_modules.traced_data.TracedData
    :param analysis_config: Configuration for the conversion.
    :type analysis_config: src.engagement_db_to_analysis.configuration.AnalysisConfiguration
    :return: Messages organised by rqa message into column-view format suitable for further analysis.
    :rtype: list of core_data_modules.traced_data.TracedData
    """
    messages_by_column = dict()  # of participant_uuid -> list of rqa messages in column view

    # Conduct the conversion in 2 passes.
    # Pass 1: Convert each rqa message to a new TracedData object in column-view.
    for msg_td in messages_traced_data:
        # Skip this message if it's not an RQA
        message = Message.from_dict(dict(msg_td))
        analysis_dataset_config = _analysis_dataset_config_for_message(analysis_config.dataset_configurations, message)
        if analysis_dataset_config.dataset_type != DatasetTypes.RESEARCH_QUESTION_ANSWER:
            continue

        # Convert to column-view TracedData
        column_td = TracedData(
            {"participant_uuid": message.participant_uuid},
            Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
        )
        _add_message_to_column_td(user, msg_td, column_td, analysis_config)

        # Add to the list of converted rqa messages for this participant.
        if message.participant_uuid not in messages_by_column:
            messages_by_column[message.participant_uuid] = []
        messages_by_column[message.participant_uuid].append(column_td)

    # Pass 2: Update each converted rqa message with the demographic messages
    for msg_td in messages_traced_data:
        # Skip this message if it's not a demographic.
        message = Message.from_dict(dict(msg_td))
        analysis_dataset_config = _analysis_dataset_config_for_message(analysis_config.dataset_configurations, message)
        if analysis_dataset_config.dataset_type != DatasetTypes.DEMOGRAPHIC:
            continue

        # Add this demographic to each of the column-view rqa message TracedData for this participant.
        # (Use messages_by_column.get() because we might have demographics for people who never sent an RQA message).
        for column_td in messages_by_column.get(message.participant_uuid, []):
            _add_message_to_column_td(user, msg_td, column_td, analysis_config)

    flattened_messages = []
    for msgs in messages_by_column.values():
        flattened_messages.extend(msgs)
    return flattened_messages


def _convert_to_participants_column_format(user, messages_traced_data, analysis_config):
    """
    Converts a list of messages traced data into "column-view" format by participant.

    Returns one "column-view" TracedData object for each participant, with all rqa and demographic messages from the
    same participant appended to each TracedData.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages traced data to convert.
    :type messages_traced_data: list of core_data_modules.traced_data.TracedData
    :param analysis_config: Configuration for the conversion.
    :type analysis_config: src.engagement_db_to_analysis.configuration.AnalysisConfiguration
    :return: Messages organised by participant into column-view format  suitable for further analysis.
    :rtype: list of core_data_modules.traced_data.TracedData
    """
    participants_by_column = dict()  # of participant_uuid -> participant traced data in column view
    for msg_td in messages_traced_data:
        message = Message.from_dict(dict(msg_td))

        # If we've not seen this participant before, create an empty Traced Data to represent them.
        if message.participant_uuid not in participants_by_column:
            participants_by_column[message.participant_uuid] = TracedData(
                {"participant_uuid": message.participant_uuid},
                Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
            )

        # Add this message to the relevant participant's column-view TracedData.
        participant = participants_by_column[message.participant_uuid]
        _add_message_to_column_td(user, msg_td, participant, analysis_config)

    return participants_by_column.values()


def export_production_file(traced_data, analysis_config, export_path):
    with open(export_path, "w") as f:
        headers = ["participant_uuid"] + [c.raw_dataset for c in analysis_config.dataset_configurations]
        TracedDataCSVIO.export_traced_data_iterable_to_csv(traced_data, f, headers)


def generate_analysis_files(user, pipeline_config, engagement_db, cache_path=None):
    analysis_dataset_configurations = pipeline_config.analysis_configs.dataset_configurations
    messages_map = _get_project_messages_from_engagement_db(analysis_dataset_configurations, engagement_db, cache_path)

    messages_traced_data = _convert_messages_to_traced_data(user, messages_map)

    messages_traced_data = filter_messages(user, messages_traced_data, pipeline_config)

    messages_by_column = _convert_to_messages_column_format(user, messages_traced_data, pipeline_config.analysis_configs)
    participants_by_column = _convert_to_participants_column_format(user, messages_traced_data, pipeline_config.analysis_configs)

    export_production_file(messages_by_column, pipeline_config.analysis_configs, "analysis/messages-production.csv")
    export_production_file(participants_by_column, pipeline_config.analysis_configs, "analysis/participants-production.csv")
