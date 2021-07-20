from datetime import datetime
from os import path
import json


from core_data_modules.util import IOUtils
from engagement_database.data_models import Message


class AnalysisCache(object):
    def __init__(self, cache_dir):
        """
        Initialises an Engagement to Analysis cache at the given directory.
        The cache can be used to locally save/retrieve data needed to enable incremental running of a
        Engagement database-> Analysis tool.

        :param cache_dir: Directory to use for the cache.
        :type cache_dir: str
        """
        self.cache_dir = cache_dir

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
        try:
            with open(self._latest_message_timestamp_path(engagement_db_dataset)) as f:
                return datetime.fromisoformat(f.read())
        except FileNotFoundError:
            return None

    def set_latest_message_timestamp(self, engagement_db_dataset, last_updated):
        """
        Sets the latest seen message.last_updated in cache for the given engagement_db_dataset context.

        :param engagement_db_dataset: Engagement db dataset name for this context.
        :type engagement_db_dataset: str
        :return: Latest run timestamp.
        :rtype: datetime.datetime
        """
        export_path = self._latest_message_timestamp_path(engagement_db_dataset)
        IOUtils.ensure_dirs_exist_for_file(export_path)
        with open(export_path, "w") as f:
            f.write(last_updated.isoformat())

    def get_messages(self, engagement_db_dataset):
        """
        Gets a list of messages for the given engagement_db_dataset from the cache.

        :param engagement_db_dataset: Engagement db dataset name for this context.
        :type engagement_db_dataset: str
        :return: list of messages
        :rtype: list of engagement_database.data_models.Message
        """
        previous_export_file_path = path.join(f"{self.cache_dir}/{engagement_db_dataset}.jsonl")
        messages = []
        try:
            with open(previous_export_file_path) as f:
                for line in f:
                    messages.append(Message.from_dict(json.loads(line)))
        except FileNotFoundError:
            return []

        return messages

    def set_messages(self, engagement_db_dataset, messages):
        """
        Sets a list of messages for the given engagement_db_dataset.

        :param engagement_db_dataset: Engagement db dataset name for this context.
        :type engagement_db_dataset: str
        :param messages: Messages to set.
        :type messages: list of engagement_database.data_models.Message
        """
        export_file_path = path.join(f"{self.cache_dir}/{engagement_db_dataset}.jsonl")
        with open(export_file_path, "w") as f:
            for msg in messages:
                f.write(f"{json.dumps(msg.to_dict(serialize_datetimes_to_str=True))}\n")
