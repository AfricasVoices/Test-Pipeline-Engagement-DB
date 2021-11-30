from src.common.cache import Cache


class CodaSyncCache(Cache):
    def get_last_seen_message(self, dataset):
        """
        Gets the last seen engagement db message for the given dataset.

        :param dataset: Dataset
        :type dataset: str
        :return: Last seen message, or None if there is no cached message for this dataset.
        :rtype: engagement_database.data_models.Message | None
        """
        return self.get_message(dataset)

    def set_last_seen_message(self, dataset, message):
        """
        Sets the last seen engagement db message for the given dataset.

        :param dataset: Dataset
        :type dataset: str
        :param message: Last seen message
        :type message: engagement_database.data_models.Message
        """
        return self.set_message(dataset, message)

    def get_last_updated_timestamp(self, dataset):
        """
        Gets the last_updated timestamp for the given dataset.

        :param dataset: Dataset
        :type dataset: str
        :return: Last updated timestamp, or None if there is no cached timestamp for this dataset.
        :rtype: datetime.datetime | None
        """
        return self.get_date_time(dataset)

    def set_last_updated_timestamp(self, dataset, timestamp):
        """
        Sets the last_updated timestamp for the given dataset.

        :param dataset: Dataset
        :type dataset: str
        :param timestamp: Last updated timestamp to write.
        :type timestamp: datetime.datetime | None
        """
        return self.set_date_time(dataset, timestamp)
