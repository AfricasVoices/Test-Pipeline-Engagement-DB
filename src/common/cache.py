from datetime import datetime
from os import path
import json


from core_data_modules.util import IOUtils
from engagement_database.data_models import Message
from temba_client.v2 import Contact


class Cache:
    def __init__(self, cache_dir):
        """
        Initialises an Engagement to Analysis cache at the given directory.
        The cache can be used to locally save/retrieve data needed to enable incremental running of a
        Engagement database-> Analysis tool.

        :param cache_dir: Directory to use for the cache.
        :type cache_dir: str
        """
        self.cache_dir = cache_dir

    def set_date_time(self, entry_name, date_time):
        export_path = f"{self.cache_dir}/{entry_name}.txt"
        IOUtils.ensure_dirs_exist_for_file(export_path)
        with open(export_path, "w") as f:
            f.write(date_time.isoformat())

    def get_date_time(self, entry_name):
        try:
            with open(f"{self.cache_dir}/{entry_name}.txt") as f:
                return datetime.fromisoformat(f.read())
        except FileNotFoundError:
            return None

    def set_rapid_pro_contacts(self, entry_name, contacts):
        export_path = f"{self.cache_dir}/{entry_name}.json"
        IOUtils.ensure_dirs_exist_for_file(export_path)
        with open(export_path, "w") as f:
            json.dump([c.serialize() for c in contacts], f)

    def get_rapid_pro_contacts(self, entry_name):
        try:
            with open(f"{self.cache_dir}/{entry_name}.json") as f:
                return [Contact.deserialize(d) for d in json.load(f)]
        except FileNotFoundError:
            return None

    def set_message(self, entry_name, message):
        export_path = f"{self.cache_dir}/{entry_name}.json"
        IOUtils.ensure_dirs_exist_for_file(export_path)
        with open(export_path, "w") as f:
            json.dump(message.to_dict(serialize_datetimes_to_str=True), f)

    def get_message(self, entry_name):
        try:
            with open(f"{self.cache_dir}/{entry_name}.json") as f:
                return Message.from_dict(json.load(f))
        except FileNotFoundError:
            return None

    def get_messages(self, entry_name):
        previous_export_file_path = path.join(f"{self.cache_dir}/{entry_name}.jsonl")
        messages = []
        try:
            with open(previous_export_file_path) as f:
                for line in f:
                    messages.append(Message.from_dict(json.loads(line)))
        except FileNotFoundError:
            return None

        return messages

    def set_messages(self, entry_name, messages):
        export_file_path = path.join(f"{self.cache_dir}/{entry_name}.jsonl")
        IOUtils.ensure_dirs_exist_for_file(export_file_path)
        with open(export_file_path, "w") as f:
            for msg in messages:
                f.write(f"{json.dumps(msg.to_dict(serialize_datetimes_to_str=True))}\n")
