from src.common.cache import Cache


class TelegramGroupSyncCache(Cache):
    def get_latest_max_id(self, group_id):
        """
        Gets the latest seen message.date cache for the given engagement_database.

        :param engagement_database: EngagementDatabase to get cache for.
        :type engagement_database: engagement_database.EngagementDatabase
        :return: Cached latest message.date, or None if there is no cached value for this context.
        :rtype: datetime.datetime | None
        """
        return self.get_max_id(f"{group_id}")


    def set_latest_max_id(self, engagement_database, group_id, max_id):
        """
        Sets the latest seen message.date cache for the given post_id.

        :param engagement_database: EngagementDatabase to get cache for.
        :type engagement_database: engagement_database.EngagementDatabase
        :param timestamp: Latest seen comment.created_time for the given post_id.
        :type timestamp: datetime.datetime
        """
        self.set_max_id(f"{engagement_database}{group_id}", max_id)
