from core_data_modules.analysis import AnalysisConfiguration
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata, TracedData
from core_data_modules.traced_data.util.fold_traced_data import FoldStrategies
from core_data_modules.util import TimeUtils
from engagement_database.data_models import Message

from src.engagement_db_to_analysis.configuration import DatasetTypes, OperatorDatasetConfiguration

"""
This module contains utility functions for converting configurations and datasets from the format used in the pipeline
configuration files and for the stages of analysis (that operate on messages) to the format used for the later stages 
of analysis (that operate on collated data, either by participant, rqa message, or by project).
"""


log = Logger(__name__)


def coding_config_to_column_config(coding_config, raw_dataset):
    """
    Converts a coding configuration to a "column-view" configuration.

    :param coding_config: Coding configuration to convert.
    :type coding_config: src.engagement_db_to_analysis.configuration.CodingConfiguration
    :param raw_dataset: Raw dataset field name.
    :type raw_dataset: str
    :return: Column configuration for the given coding configuration.
    :rtype: core_data_modules.analysis.analysis_utils.AnalysisConfiguration
    """
    return AnalysisConfiguration(
        dataset_name=coding_config.analysis_dataset,
        raw_field=raw_dataset,
        coded_field=f"{coding_config.analysis_dataset}_labels",
        code_scheme=coding_config.code_scheme
    )


def analysis_dataset_config_to_column_configs(analysis_dataset_config):
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
        column_config = AnalysisConfiguration(
            dataset_name=coding_config.analysis_dataset,
            raw_field=analysis_dataset_config.raw_dataset,
            coded_field=f"{coding_config.analysis_dataset}_labels",
            code_scheme=coding_config.code_scheme
        )
        column_configs.append(column_config)
    return column_configs


def analysis_dataset_configs_to_column_configs(analysis_dataset_configs):
    """
    Converts a list of analysis dataset configurations to the relevant "column-view" configurations.

    :param analysis_dataset_configs: Analysis dataset configurations to convert.
    :type analysis_dataset_configs: list of src.engagement_db_to_analysis.configuration.AnalysisDatasetConfiguration
    :return: List of all the column configurations for the analysis dataset configurations.
    :rtype: list of core_data_modules.analysis.analysis_utils.AnalysisConfiguration
    """
    column_configs = []
    for analysis_dataset_config in analysis_dataset_configs:
        column_configs.extend(analysis_dataset_config_to_column_configs(analysis_dataset_config))
    return column_configs


def analysis_dataset_configs_to_rqa_column_configs(analysis_dataset_configs):
    """
    Converts a list of analysis dataset configurations to a list of "column-view" configurations, for the rqa datasets
    only.

    :param analysis_dataset_configs: Analysis dataset configurations to convert.
    :type analysis_dataset_configs: list of src.engagement_db_to_analysis.configuration.AnalysisDatasetConfiguration
    :return: List of rqa column configurations for the analysis dataset configurations.
    :rtype: list of core_data_modules.analysis.analysis_utils.AnalysisConfiguration
    """
    rqa_column_configs = []
    for analysis_dataset_config in analysis_dataset_configs:
        if analysis_dataset_config.dataset_type == DatasetTypes.RESEARCH_QUESTION_ANSWER:
            rqa_column_configs.extend(analysis_dataset_config_to_column_configs(analysis_dataset_config))
    return rqa_column_configs


def analysis_dataset_configs_to_demog_column_configs(analysis_dataset_configs):
    """
    Converts a list of analysis dataset configurations to a list of "column-view" configurations, for the demographic
    datasets only.

    :param analysis_dataset_configs: Analysis dataset configurations to convert.
    :type analysis_dataset_configs: list of src.engagement_db_to_analysis.configuration.AnalysisDatasetConfiguration
    :return: List of demographic column configurations for the analysis dataset configurations.
    :rtype: list of core_data_modules.analysis.analysis_utils.AnalysisConfiguration
    """
    demog_column_configs = []
    for analysis_dataset_config in analysis_dataset_configs:
        if analysis_dataset_config.dataset_type == DatasetTypes.DEMOGRAPHIC:
            demog_column_configs.extend(analysis_dataset_config_to_column_configs(analysis_dataset_config))
    return demog_column_configs


def analysis_dataset_config_for_message(analysis_dataset_configs, message):
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


def get_latest_labels_with_code_scheme(message, code_scheme):
    """
    Gets the labels assigned to this message under the given `code_scheme` (or a duplicate of this code scheme).

    Labels assigned under duplicate code schemes are normalised to have the primary code scheme id e.g. scheme_id
    'scheme-abc123-1' will be re-written to 'scheme-abc123'.

    :param message: Message to get the labels from.
    :type message: engagement_database.data_models.Message
    :param code_scheme: Code scheme to get the latest labels for.
    :type code_scheme: core_data_modules.data_models.CodeScheme
    :return: List of the relevant, normalised labels for the given code scheme.
    :rtype: list of core_data_modules.data_models.Label
    """
    latest_labels_with_code_scheme = []
    for label in message.get_latest_labels():
        if label.scheme_id.startswith(code_scheme.scheme_id):
            label.scheme_id = code_scheme.scheme_id
            latest_labels_with_code_scheme.append(label)
    return latest_labels_with_code_scheme


def _filter_out_demogs_only(messages_traced_data, analysis_dataset_configs):
    """
    Filters out messages from participants who only sent demographics.

    :param messages_traced_data: Messages traced data to filter.
    :type messages_traced_data: list of core_data_modules.traced_data.TracedData
    :param analysis_dataset_configs: Configuration for the filter.
    :type analysis_dataset_configs: list of src.engagement_db_to_analysis.configuration.AnalysisDatasetConfiguration
    :param messages_traced_data: Filtered messages traced data.
    :type messages_traced_data: list of core_data_modules.traced_data.TracedData
    """
    # Find the rqa engagement_db datasets in the given configuration.
    rqa_engagement_db_datasets = []
    for config in analysis_dataset_configs:
        if config.dataset_type == DatasetTypes.RESEARCH_QUESTION_ANSWER:
            rqa_engagement_db_datasets.extend(config.engagement_db_datasets)

    # Find the participants who have a message in an rqa engagement_db dataset
    rqa_participant_uuids = set()
    for msg in messages_traced_data:
        for rqa_dataset in rqa_engagement_db_datasets:
            if msg["dataset"] == rqa_dataset:
                rqa_participant_uuids.add(msg["participant_uuid"])

    # Filter all the messages so that we exclude messages from people who didn't send an rqa.
    filtered = []
    excluded_uuids = set()
    for msg in messages_traced_data:
        if msg["participant_uuid"] in rqa_participant_uuids:
            filtered.append(msg)
        else:
            excluded_uuids.add(msg["participant_uuid"])

    log.info(f"Filtered out messages from participants who only sent demogs. Returning "
             f"{len(filtered)}/{len(messages_traced_data)} messages (excluded {len(excluded_uuids)} uuids)")
    return filtered


def _add_message_to_column_td(user, message_td, column_td, analysis_dataset_configs):
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
    :param analysis_dataset_configs: Dataset configurations to use to decide how to process the message.
    :type analysis_dataset_configs: list of src.engagement_db_to_analysis.configuration.AnalysisDatasetConfiguration
    """
    message = Message.from_dict(dict(message_td))

    # Get the analysis dataset configuration for this message
    message_analysis_dataset_config = analysis_dataset_config_for_message(analysis_dataset_configs, message)

    # Convert the analysis dataset config to its "column-view" configurations
    column_configs = analysis_dataset_config_to_column_configs(message_analysis_dataset_config)

    updated_column_data = dict()

    # Add this message's raw text to the the raw_dataset column in the column-view TracedData.
    # If the TracedData already contains data here, append this text to the existing, previously added texts by
    # concatenating.
    existing_text = column_td.get(message_analysis_dataset_config.raw_dataset)
    if existing_text is None:
        updated_column_data[message_analysis_dataset_config.raw_dataset] = message.text
    else:
        updated_column_data[message_analysis_dataset_config.raw_dataset] = FoldStrategies.concatenate(existing_text, message.text)

    # For each column config, get the latest, normalised labels under that column config's code scheme, and them
    # to the column-view TracedData.
    # If the TracedData already contains data here, combine the labels with the existing labels using
    # FoldStrategies.list_of_labels.
    for column_config in column_configs:
        latest_labels_with_code_scheme = get_latest_labels_with_code_scheme(message, column_config.code_scheme)

        if len(latest_labels_with_code_scheme) == 0:
            continue

        latest_labels_with_code_scheme = [label.to_dict() for label in latest_labels_with_code_scheme]
        existing_labels = column_td.get(column_config.coded_field)
        if existing_labels is None:
            updated_column_data[column_config.coded_field] = latest_labels_with_code_scheme
        else:
            updated_column_data[column_config.coded_field] = FoldStrategies.list_of_labels(
                column_config.code_scheme, existing_labels, latest_labels_with_code_scheme
            )

    # Append the TracedData history for this message to the column-view.
    message_td = message_td.copy()
    message_td.hide_keys(message_td.keys(), Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))
    column_td.append_traced_data("appended_message", message_td, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    # Write the new data to the column-view TracedData
    # (we do this after appending the message_td so the TracedData is slightly easier to read)
    column_td.append_data(updated_column_data, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))


def _add_operators_to_column_td(user, column_td, operators, dataset_config):
    """
    Adds the given operators to the column traced data.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param column_td: An existing TracedData object in column-view format, to which the operator(s) will be appended.
    :type column_td: core_data_modules.traced_data.TracedData
    :param operators: Operator strings to add
    :type operators: iterable of str
    :param dataset_config: Configuration for the operators dataset
    :type dataset_config: src.engagement_db_to_analysis.configuration.AnalysisDatasetConfiguration
    """
    for column_config in analysis_dataset_config_to_column_configs(dataset_config):
        labels = []
        for operator in set(operators):
            labels.append(CleaningUtils.make_label_from_cleaner_code(
                    column_config.code_scheme,
                    column_config.code_scheme.get_code_with_match_value(operator),
                    Metadata.get_call_location()
                ).to_dict())

        column_td.append_data({
            dataset_config.raw_dataset: ";".join(operators),
            column_config.coded_field: labels
        }, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))


def convert_to_messages_column_format(user, messages_traced_data, analysis_config):
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
    log.info(f"Converting {len(messages_traced_data)} messages traced data objects to column-view format by "
             f"message...")
    messages_traced_data = _filter_out_demogs_only(messages_traced_data, analysis_config.dataset_configurations)

    messages_by_column = dict()  # of participant_uuid -> list of rqa messages in column view

    # Conduct the conversion in 2 passes.
    # Pass 1: Convert each rqa message to a new TracedData object in column-view.
    for msg_td in messages_traced_data:
        # Skip this message if it's not an RQA
        message = Message.from_dict(dict(msg_td))
        analysis_dataset_config = analysis_dataset_config_for_message(analysis_config.dataset_configurations, message)
        if analysis_dataset_config.dataset_type != DatasetTypes.RESEARCH_QUESTION_ANSWER:
            continue

        # Convert to column-view TracedData
        column_td = TracedData(
            {"participant_uuid": message.participant_uuid, "timestamp": message.timestamp.isoformat()},
            Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
        )
        _add_message_to_column_td(user, msg_td, column_td, analysis_config.dataset_configurations)

        # Assign operators
        for dataset_config in analysis_config.dataset_configurations:
            if type(dataset_config) == OperatorDatasetConfiguration:
                _add_operators_to_column_td(user, column_td, [message.channel_operator], dataset_config)

        # Add to the list of converted rqa messages for this participant.
        if message.participant_uuid not in messages_by_column:
            messages_by_column[message.participant_uuid] = []
        messages_by_column[message.participant_uuid].append(column_td)

    # Pass 2: Update each converted rqa message with the demographic messages
    for msg_td in messages_traced_data:
        # Skip this message if it's not a demographic.
        message = Message.from_dict(dict(msg_td))
        analysis_dataset_config = analysis_dataset_config_for_message(analysis_config.dataset_configurations, message)
        if analysis_dataset_config.dataset_type != DatasetTypes.DEMOGRAPHIC:
            continue

        # Add this demographic to each of the column-view rqa message TracedData for this participant.
        # (Use messages_by_column.get() because we might have demographics for people who never sent an RQA message).
        for column_td in messages_by_column.get(message.participant_uuid, []):
            _add_message_to_column_td(user, msg_td, column_td, analysis_config.dataset_configurations)

    flattened_messages = []
    for msgs in messages_by_column.values():
        flattened_messages.extend(msgs)

    log.info(f"Converted {len(messages_traced_data)} messages traced data objects to {len(flattened_messages)} "
             f"column-view format objects, by message")
    return flattened_messages


def convert_to_participants_column_format(user, messages_traced_data, analysis_config):
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
    log.info(f"Converting {len(messages_traced_data)} messages traced data objects to column-view format by "
             f"participant...")
    messages_traced_data = _filter_out_demogs_only(messages_traced_data, analysis_config.dataset_configurations)

    uuids_to_operators = dict()  # of participant_uuid -> set of channel_operators
    participants_by_column = dict()  # of participant_uuid -> participant traced data in column view
    for msg_td in messages_traced_data:
        message = Message.from_dict(dict(msg_td))

        # If we've not seen this participant before, create an empty Traced Data to represent them.
        if message.participant_uuid not in participants_by_column:
            participants_by_column[message.participant_uuid] = TracedData(
                {"participant_uuid": message.participant_uuid},
                Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
            )

        # If this message is an RQA message, add its operator to the set of operators for this participant
        analysis_dataset_config = analysis_dataset_config_for_message(analysis_config.dataset_configurations, message)
        if analysis_dataset_config.dataset_type == DatasetTypes.RESEARCH_QUESTION_ANSWER:
            if message.participant_uuid not in uuids_to_operators:
                uuids_to_operators[message.participant_uuid] = set()
            uuids_to_operators[message.participant_uuid].add(message.channel_operator)

        # Add this message to the relevant participant's column-view TracedData.
        participant = participants_by_column[message.participant_uuid]
        _add_message_to_column_td(user, msg_td, participant, analysis_config.dataset_configurations)

    # Assign operator codes to each participant's column_td
    for participant_uuid, column_td in participants_by_column.items():
        for dataset_config in analysis_config.dataset_configurations:
            if type(dataset_config) == OperatorDatasetConfiguration:
                _add_operators_to_column_td(user, column_td, uuids_to_operators[participant_uuid], dataset_config)

    log.info(f"Converted {len(messages_traced_data)} messages traced data objects to "
             f"{len(participants_by_column.values())} column-view format objects, by participant")

    return list(participants_by_column.values())
