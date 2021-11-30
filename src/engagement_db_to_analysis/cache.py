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
