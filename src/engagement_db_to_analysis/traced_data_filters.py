from core_data_modules.util import TimeUtils
from dateutil.parser import isoparse
import time

from core_data_modules.traced_data import Metadata
from core_data_modules.logging import Logger


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

    # Exclusive end time of the time range to keep. Messages sent after this time will be dropped.
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
    for analysis_config in pipeline_config.analysis_config:
        if analysis_config.dataset_type == DatasetTypes.RESEARCH_QUESTION_ANSWER:
            for engagement_db_dataset in analysis_config.engagement_db_datasets:
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

    log.info(f"Filtered out messages{time_range_log}. "
             f"Returning {len(filtered)}/{len(messages_traced_data)} messages.")

    return filtered


def filter_messages(user, messages_data, pipeline_config):

    # Filter out runs sent outwith the project start and end dates
    messages_data = rqa_time_range_filter(user, messages_data, pipeline_config)

    return messages_data
