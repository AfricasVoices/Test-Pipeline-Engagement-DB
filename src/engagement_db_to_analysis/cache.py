import json
from os import path

from core_data_modules.util import IOUtils

from src.common.cache import Cache


class AnalysisCache(Cache):
    def _latest_message_timestamp_path(self, engagement_db_dataset):
        return f"{self.cache_dir}/last_updated_{engagement_db_dataset}.txt"

    def get_latest_message_timestamp(self, engagement_db_dataset):
        """
        Gets the latest seen message.last_updated from cache for the given engagement_db_dataset.

        :param engagement_db_dataset: Engagement db dataset name for this context.
        :type engagement_db_dataset: str
        :return: Timestamp for the last updated message in cache, or None if there is no cache yet for this context.
        :rtype: datetime.datetime | None
        """
        return self.get_date_time(engagement_db_dataset)

    def set_latest_message_timestamp(self, engagement_db_dataset, latest_timestamp):
        """
        Sets the latest seen message.last_updated in cache for the given engagement_db_dataset context.

        :param engagement_db_dataset: Engagement db dataset name for this context.
        :type engagement_db_dataset: str
        :param latest_timestamp: Latest run timestamp.
        :type latest_timestamp: datetime.datetime
        """
        self.set_date_time(engagement_db_dataset, latest_timestamp)

    def set_synced_uuids(self, group_name, participants_uuids):
        """
        Sets a set of participants_uuids for the given rapid pro group.

        :param group_name: name of the rapid pro group.
        :type group_name: str
        :param participants_uuids: participants uuids to set, for the given rapid pro group.
        :type participants_uuids: list of participants uuids
        """
        export_file_path = path.join(f"{self.cache_dir}/rapid_pro_adverts/{group_name}.jsonl")
        IOUtils.ensure_dirs_exist_for_file(export_file_path)
        with open(export_file_path, "w") as f:
            f.write(json.dumps(participants_uuids))

    def get_synced_uuids(self, group_name):
        """
        Gets a set of participants_uuids for the given rapid pro group.

        :param group_name: name of the rapid pro group.
        :type group_name: str
        :retun participants_uuids: participants uuids for the given rapid pro group or none if not found.
        :rtype participants_uuids: list of participants uuids | None
        """

        previous_export_file_path = path.join(f"{self.cache_dir}/rapid_pro_adverts/{group_name}.jsonl")
        try:
            with open(previous_export_file_path) as f:
                participants_uuids = json.load(f)

        except FileNotFoundError:
            return []

        return participants_uuids
