from datetime import datetime
import json

from core_data_modules.util import IOUtils
from temba_client.v2 import Contact


class RapidProSyncCache:
    def __init__(self, cache_dir):
        """
        Initialises a Rapid Pro sync cache at the given directory.

        The sync cache can be used to locally save/retrieve data needed to enable incremental running of a
        Rapid Pro -> Engagement Database sync tool.

        :param cache_dir: Directory to use for the cache.
        :type cache_dir: str
        """

        self.cache_dir = cache_dir

    def _contacts_path(self):
        return f"{self.cache_dir}/contacts.json"

    def get_contacts(self):
        """
        Gets cached contacts.

        :return: Cached contacts, or None if there is no cache yet.
        :rtype: list of temba_client.v2.Contact | None
        """
        try:
            with open(self._contacts_path()) as f:
                return [Contact.deserialize(d) for d in json.load(f)]
        except FileNotFoundError:
            return None

    def set_contacts(self, contacts):
        """
        Sets cached contacts.

        :return: Contacts to write to the cache.
        :rtype: list of temba_client.v2.Contact | None
        """
        export_path = self._contacts_path()
        IOUtils.ensure_dirs_exist_for_file(export_path)
        with open(export_path, "w") as f:
            json.dump([c.serialize() for c in contacts], f)

    def _latest_run_timestamp_path(self, flow_id, result_field):
        return f"{self.cache_dir}/latest_seen_run_{flow_id}_{result_field}.txt"

    def get_latest_run_timestamp(self, flow_id, result_field):
        """
        Gets the latest seen run.modified_on cache for the given flow_id and result_field context.

        :param flow_id: Flow id.
        :type flow_id: str
        :param result_field: Flow result field.
        :type result_field: str
        :return: Cached latest run timestamp, or None if there is no cache yet for this context.
        :rtype: datetime.datetime | None
        """
        try:
            with open(self._latest_run_timestamp_path(flow_id, result_field)) as f:
                return datetime.fromisoformat(f.read())
        except FileNotFoundError:
            return None

    def set_latest_run_timestamp(self, flow_id, result_field, last_updated):
        """
        Sets the latest seen run.modified_on cache for the given flow_id and result_field context.

        :param flow_id: Flow id.
        :type flow_id: str
        :param result_field: Flow result field.
        :type result_field: str
        :return: Latest run timestamp.
        :rtype: datetime.datetime
        """
        export_path = self._latest_run_timestamp_path(flow_id, result_field)
        IOUtils.ensure_dirs_exist_for_file(export_path)
        with open(export_path, "w") as f:
            f.write(last_updated.isoformat())
