from core_data_modules.logging import Logger

from src.common.sync_stats import SyncStats


log = Logger(__name__)


class FacebookSyncEvents:
    READ_POSTS_FROM_FACEBOOK = "read_posts_from_facebook"
    READ_COMMENTS_FROM_POSTS = "read_comments_from_posts"
    COMMENT_SYNCED_IN_PREVIOUS_RUN = "comment_synced_in_previous_run"
    ADD_MESSAGE_TO_ENGAGEMENT_DB = "add_message_to_engagement_db"


class FacebookToEngagementDBSyncStats(SyncStats):
    def __init__(self):
        super().__init__({
            FacebookSyncEvents.READ_POSTS_FROM_FACEBOOK: 0,
            FacebookSyncEvents.READ_COMMENTS_FROM_POSTS: 0,
            FacebookSyncEvents.COMMENT_SYNCED_IN_PREVIOUS_RUN: 0,
            FacebookSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB: 0
        })

    def print_summary(self):
        log.info(f"Posts downloaded from Facebook: {self.event_counts[FacebookSyncEvents.READ_POSTS_FROM_FACEBOOK]}")
        log.info(f"Comments downloaded from Posts: {self.event_counts[FacebookSyncEvents.READ_COMMENTS_FROM_POSTS]}")
        log.info(f"Comment already in engagement db: {self.event_counts[FacebookSyncEvents.COMMENT_SYNCED_IN_PREVIOUS_RUN]}")
        log.info(f"Messages added to engagement db: {self.event_counts[FacebookSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB]}")
