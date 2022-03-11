from src.common.cache import Cache


class FacebookSyncCache(Cache):
    def get_latest_comment_timestamp(self, post_id):
        """
        Gets the latest seen run.modified_on cache for the given flow_id and result_field context.

        :param post_id: Id of post to cache.
        :type post_id: str
        :return: Cached latest comment.created_time, or None if there is no cached value for this context.
        :rtype: datetime.datetime | None
        """
        return self.get_date_time(f"{post_id}")


    def set_latest_comment_timestamp(self, post_id, timestamp):
        """
        Sets the latest seen comment.created_time cache for the given post_id

        :param post_id: Id of post to cache.
        :type post_id: str
        :param timestamp: Latest seen comment.created_time for the given post_id.
        :type timestamp: datetime.datetime
        """
        self.set_date_time(f"{post_id}", timestamp)
