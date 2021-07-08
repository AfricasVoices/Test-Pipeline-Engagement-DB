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

    log.debug(f"Filtering out research question messages sent outside the project time range "
              f"{start_time_inclusive.isoformat()} to {end_time_inclusive.isoformat()}...")

    # Filter a list of td for research question messages received within the given time range.
    rqa_engagement_db_datasets = []
    for analysis_config in pipeline_config.analysis_config:
        if analysis_config.dataset_type == DatasetTypes.RESEARCH_QUESTION_ANSWER:
            for engagement_db_dataset in analysis_config.engagement_db_datasets:
                rqa_engagement_db_datasets.append(engagement_db_dataset)

    filtered = []
    for td in messages_traced_data:
        if td["dataset"] in rqa_engagement_db_datasets:
            if start_time_inclusive <= isoparse(td["timestamp"]) < end_time_inclusive:
                td.append_data(td, Metadata(user, Metadata.get_call_location(), time.time()))
                filtered.append(td)
        else:
            filtered.append(td)

    log.info(f"Filtered out messages sent outside the time range "
             f"{start_time_inclusive.isoformat()} to {end_time_inclusive.isoformat()}. "
             f"Returning {len(filtered)}/{len(messages_traced_data)} messages.")

    return filtered

def filter_test_individuals(user, individual_traced_data, test_contacts):
    """
    Filters a list of individuals who are not in pipeline_config.test_contacts e.g AVF/Aggregator staff

    :param individual_traced_data: List of TracedData individuals objects to filter.
    :type individual_traced_data: list of TracedData
    :param test_contacts: a list containing test participant uids.
    :type test_contacts: list of str
    :return: Filtered list.
    :rtype: list of TracedData
    """
    log.debug("Filtering out test messages...")
    filtered = []
    for ind_td in individual_traced_data:
        if ind_td["participant_uuid"] in test_contacts:
            continue

        ind_td.append_data(ind_td, Metadata(user, Metadata.get_call_location(), time.time()))
        filtered.append(ind_td)

    log.info(f"Filtered out test messages. "
             f"Returning {len(filtered)}/{len(individual_traced_data)} messages.")
    return filtered


def filter_messages(user, messages_data, pipeline_config):

    # Filter out runs sent outwith the project start and end dates
    messages_data = rqa_time_range_filter(user, messages_data, pipeline_config)

    return messages_data


def filter_individuals(user, individuals_data, pipeline_config):
    # Filter out test messages sent by Test Contacts.
    if pipeline_config.filter_test_messages:
        individuals_data = filter_test_individuals(user, individuals_data, pipeline_config.test_participant_uids)
    else:
        log.debug(
            "Not filtering out test messages (because the pipeline_config.filter_test_messages was set to false)")

    return individuals_data
