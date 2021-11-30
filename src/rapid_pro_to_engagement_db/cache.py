from src.common.cache import Cache


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

    def get_latest_run_timestamp(self, flow_id, result_field):
        """
        Gets the latest seen run.modified_on cache for the given flow_id and result_field context.

        :param flow_id: Flow id.
        :type flow_id: str
        :param result_field: Flow result field.
        :type result_field: str
        :return: Cached latest run.modified_on, or None if there is no cached value for this context.
        :rtype: datetime.datetime | None
        """
        return self.get_date_time(f"{flow_id}.{result_field}")

    def set_latest_run_timestamp(self, flow_id, result_field, timestamp):
        """
        Sets the latest seen run.modified_on cache for the given flow_id and result_field context.

        :param flow_id: Flow id.
        :type flow_id: str
        :param result_field: Flow result field.
        :type result_field: str
        :param timestamp: Latest seen run.modified_on for the given floW_id and result_field context.
        :type timestamp: datetime.datetime
        """
        self.set_date_time(f"{flow_id}.{result_field}", timestamp)
