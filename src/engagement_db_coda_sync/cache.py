from datetime import datetime
import json

from core_data_modules.util import IOUtils
from engagement_database.data_models import Message


class CodaSyncCache:
    def __init__(self, cache_dir):
        """
        Initialises a Coda sync cache at the given directory.

        The sync cache can be used to locally save/retrieve data needed to enable incremental running of a
        engagement database <-> Coda sync tools.

        :param cache_dir: Directory to use for the cache.
        :type cache_dir: str
        """
        self.cache_dir = cache_dir

    def message_to_json(self, message):
        message_dict = message.to_dict()
        message_dict["timestamp"] = message_dict["timestamp"].isoformat()
        message_dict["last_updated"] = message_dict["last_updated"].isoformat()
        return json.dumps(message_dict)

    def json_to_message(self, blob):
        message_dict = json.loads(blob)
        message_dict["timestamp"] = datetime.fromisoformat(message_dict["timestamp"])
        message_dict["last_updated"] = datetime.fromisoformat(message_dict["last_updated"])
        return Message.from_dict(message_dict)

    def _last_seen_message_path(self, dataset):
        return f"{self.cache_dir}/last-seen-message-{dataset}.json"

    def get_last_seen_message(self, dataset):
        try:
            with open(self._last_seen_message_path(dataset)) as f:
                return self.json_to_message(f.read())
        except FileNotFoundError:
            return None

    def set_last_seen_message(self, dataset, message):
        export_path = self._last_seen_message_path(dataset)
        IOUtils.ensure_dirs_exist_for_file(export_path)
        with open(export_path, "w") as f:
            f.write(self.message_to_json(message))
