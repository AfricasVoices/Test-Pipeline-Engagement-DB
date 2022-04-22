import json

from core_data_modules.util import IOUtils
from src.common.cache import Cache
from src.rapid_pro_to_engagement_db.configuration import FlowResultConfiguration


class RapidProSyncCache(Cache):
    def get_contacts(self):
        """
        Gets cached contacts.

        :return: Cached contacts, or None if there is no contacts cache.
        :rtype: list of temba_client.v2.Contact | None
        """
        return self.get_rapid_pro_contacts("contacts")

    def set_contacts(self, contacts):
        """
        Sets cached contacts.

        :return: Contacts to write to the cache.
        :rtype: list of temba_client.v2.Contact | None
        """
        self.set_rapid_pro_contacts("contacts", contacts)

    def get_latest_run_timestamp(self, flow_id):
        """
        Gets the latest seen run.modified_on cache for the given flow_id.

        :param flow_id: Flow id.
        :type flow_id: str
        :return: Cached latest run.modified_on, or None if there is no cached value for the given flow_id.
        :rtype: datetime.datetime | None
        """
        return self.get_date_time(flow_id)

    def set_latest_run_timestamp(self, flow_id, timestamp):
        """
        Sets the latest seen run.modified_on cache for the given flow_id.

        :param flow_id: Flow id.
        :type flow_id: str
        :param timestamp: Latest seen run.modified_on for the given flow_id.
        :type timestamp: datetime.datetime
        """
        self.set_date_time(flow_id, timestamp)

    def reset_latest_run_timestamp(self, flow_id):
        """
        Resets the latest seen run.modified_on cache for the given flow_id.

        :param flow_id: Flow id.
        :type flow_id: str
        """
        self.delete_file(flow_id)

    def set_flow_result_configs(self, configs):
        export_path = f"{self.cache_dir}/flow_result_configurations.json"
        IOUtils.ensure_dirs_exist_for_file(export_path)
        with open(export_path, "w") as f:
            json.dump([c.to_dict() for c in configs], f)

    def get_flow_result_configs(self):
        try:
            print(f"{self.cache_dir}/flow_result_configurations.json")
            with open(f"{self.cache_dir}/flow_result_configurations.json") as f:
                return [FlowResultConfiguration.from_dict(d) for d in json.load(f)]
        except FileNotFoundError:
            return None
