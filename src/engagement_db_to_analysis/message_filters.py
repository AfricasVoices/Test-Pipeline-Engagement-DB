from core_data_modules.logging import Logger
from dateutil.parser import isoparse
import time

from core_data_modules.traced_data import Metadata

from src.pipeline_configuration_spec import *


log = Logger(__name__)


# TODO: Move to Core.
class Filters(object):

    @classmethod
    def rqa_time_range_filter(cls, user, data, pipeline_config):
        """
        Filters a list of td for research question messages received within the given time range.

        :param data: List of message objects to filter.
        :type data: list of TracedData
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
        for td in data:
            if td["dataset"] in rqa_engagement_db_datasets:
                if start_time_inclusive <= isoparse(td["timestamp"]) < end_time_inclusive:
                    td.append_data(td, Metadata(user, Metadata.get_call_location(), time.time()))
                    filtered.append(td)
            else:
                filtered.append(td)

        log.info(f"Filtered out messages sent outside the time range "
                 f"{start_time_inclusive.isoformat()} to {end_time_inclusive.isoformat()}. "
                 f"Returning {len(filtered)}/{len(data)} messages.")

        return filtered


    @classmethod
    def filter_messages(cls, user, data, pipeline_config):

        # Filter out runs sent outwith the project start and end dates
        messages_data = cls.rqa_time_range_filter(user, data, pipeline_config)

        return messages_data
