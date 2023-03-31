from core_data_modules.logging import Logger

from src.common.sync_stats import SyncStats


log = Logger(__name__)


class KoboToolBoxSyncEvents:
    READ_RESPONSE_FROM_KOBOTOOLBOX_FORM = "read_response_from_kobotoolbox_form"
    READ_ANSWER_FROM_RESPONSE = "read_answer_from_response"
    MESSAGE_ALREADY_IN_ENGAGEMENT_DB = "message_already_in_engagement_db"
    ADD_MESSAGE_TO_ENGAGEMENT_DB = "add_message_to_engagement_db"
    FOUND_A_NULL_RESPONSE = "found_a_null_response"


class KoboToolBoxToEngagementDBSyncStats(SyncStats):
    def __init__(self):
        super().__init__({
            KoboToolBoxSyncEvents.READ_RESPONSE_FROM_KOBOTOOLBOX_FORM: 0,
            KoboToolBoxSyncEvents.READ_ANSWER_FROM_RESPONSE: 0,
            KoboToolBoxSyncEvents.FOUND_A_NULL_RESPONSE: 0,
            KoboToolBoxSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB: 0,
            KoboToolBoxSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB: 0
        })

    def print_summary(self):
        log.info(f"Responses read from KoboToolBox Form(s): {self.event_counts[KoboToolBoxSyncEvents.READ_RESPONSE_FROM_KOBOTOOLBOX_FORM]}")
        log.info(f"Answers read from responses: {self.event_counts[KoboToolBoxSyncEvents.READ_ANSWER_FROM_RESPONSE]}")
        log.info(f"Null responses found: {self.event_counts[KoboToolBoxSyncEvents.FOUND_A_NULL_RESPONSE]}")
        log.info(f"Messages already in engagement db: {self.event_counts[KoboToolBoxSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB]}")
        log.info(f"Messages added to engagement db: {self.event_counts[KoboToolBoxSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB]}")
