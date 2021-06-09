from datetime import datetime
import json

from core_data_modules.util import IOUtils
from temba_client.v2 import Contact


class RapidProSyncCache(object):
    def __init__(self, cache_dir):
        self.cache_dir = cache_dir

    def _contacts_path(self):
        return f"{self.cache_dir}/contacts.json"

    def get_contacts(self):
        try:
            with open(self._contacts_path()) as f:
                return [Contact.deserialize(d) for d in json.load(f)]
        except FileNotFoundError:
            return None

    def set_contacts(self, contacts):
        IOUtils.ensure_dirs_exist_for_file(self._contacts_path())
        with open(self._contacts_path(), "w") as f:
            json.dump([c.serialize() for c in contacts], f)

    def get_flow_last_updated(self, flow_id, result_field):
        try:
            with open(f"{self.cache_dir}/flow-{flow_id}-{result_field}-last-updated.txt") as f:
                return datetime.fromisoformat(f.read())
        except FileNotFoundError:
            return None

    def set_flow_last_updated(self, flow_id, result_field, last_updated):
        with open(f"{self.cache_dir}/flow-{flow_id}-{result_field}-last-updated.txt", "w") as f:
            f.write(last_updated.isoformat())
