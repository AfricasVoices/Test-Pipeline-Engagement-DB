from dateutil.parser import isoparse
import time

from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.util import TimeUtils

from src.pipeline_configuration_spec import *


log = Logger(__name__)


def rqa_time_range_filter(user, messages_traced_data, pipeline_config):
    """
    Filters a list of td for research question messages received within the given time range.

    :param messages_traced_data: List of message objects to filter.
    :type messages_traced_data: list of TracedData
    :pipeline_config: pipeline configuration module
    :type PIPELINE_CONFIGURATION:
    :return: Filtered list.
    :rtype: list of TracedData
    """

    # Inclusive start time of the time range to keep. Messages sent before this time will be dropped.
    start_time_inclusive = pipeline_config.project_start_date

    # Inclusive end time of the time range to keep. Messages sent after this time will be dropped.
    end_time_inclusive = pipeline_config.project_end_date

    if start_time_inclusive is None and end_time_inclusive is None:
        log.info("No time range filters specified, returning input data unchanged")
        return messages_traced_data

    time_range_log = ""
    if start_time_inclusive is not None:
        time_range_log += f", modified on or after {start_time_inclusive.isoformat()}"
    if end_time_inclusive is not None:
        time_range_log += f", modified on or before {end_time_inclusive.isoformat()}"

    log.debug(f"Filtering out research question messages{time_range_log}...")

    # Filter a list of td for research question messages received within the given time range.
    rqa_engagement_db_datasets = []
    for analysis_dataset_config in pipeline_config.analysis.dataset_configurations:
        if analysis_dataset_config.dataset_type == DatasetTypes.RESEARCH_QUESTION_ANSWER:
            for engagement_db_dataset in analysis_dataset_config.engagement_db_datasets:
                rqa_engagement_db_datasets.append(engagement_db_dataset)

    filtered = []
    for td in messages_traced_data:
        if td["dataset"] in rqa_engagement_db_datasets:
            if start_time_inclusive is not None and isoparse(td["timestamp"]) < start_time_inclusive:
                continue
            if end_time_inclusive is not None and isoparse(td["timestamp"]) > end_time_inclusive:
                continue
            td.append_data(td, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))
            filtered.append(td)
        else:
            filtered.append(td)

    log.info(f"Returning {len(filtered)}/{len(messages_traced_data)} messages...")

    return filtered


def filter_test_messages(user, messages_traced_data, test_participant_uuids):
    """
    Filters out test messages sent by pipeline_config.test_participant_uuids i.e AVF/Aggregator staff

    :param messages_traced_data: List of message objects to filter.
    :type messages_traced_data: list of TracedData
    :param test_participant_uuids: a list containing test participant uids.
    :type test_participant_uuids: list of str
    :return: Filtered dict.
    :rtype: dict of participant_uid -> TracedData
    """
    log.debug("Filtering test messages data...")
    filtered = []

    filtered_participants_uuids = set()
    for td in messages_traced_data:
        if td["participant_uuid"] in test_participant_uuids:
            filtered_participants_uuids.add(td["participant_uuid"])
            continue

        # Updates the td object with new Metadata for this filter function.
        # The allows us to hold history of the td update for easy traceback.
        td.append_data(td, Metadata(user, Metadata.get_call_location(),
                                                    TimeUtils.utc_now_as_iso_string()))
        filtered.append(td)

    log.info(f"Filtered out {len(filtered_participants_uuids)}/{len(test_participant_uuids)} test participants messages...")
    log.info(f"Returning {len(filtered)}/{len(messages_traced_data)} messages...")

    return filtered


def filter_messages(user, messages_traced_data, pipeline_config):

    # Filter out runs sent outwith the project start and end dates
    messages_traced_data = rqa_time_range_filter(user, messages_traced_data, pipeline_config)

    if pipeline_config.test_participant_uuids is not None:
        messages_traced_data = filter_test_messages(user, messages_traced_data, pipeline_config.test_participant_uuids)
    else:
        log.debug(
            "Not filtering out test participants messages (because the pipeline_config.filter_test_participants was set to `False`)")

    return messages_traced_data
