from core_data_modules.logging import Logger

from src.common.sync_stats import SyncStats


log = Logger(__name__)


class GoogleFormSyncEvents:
    READ_RESPONSE_FROM_GOOGLE_FORM = "read_response_from_google_form"
    READ_ANSWER_FROM_RESPONSE = "read_answer_from_response"
    MESSAGE_ALREADY_IN_ENGAGEMENT_DB = "message_already_in_engagement_db"
    ADD_MESSAGE_TO_ENGAGEMENT_DB = "add_message_to_engagement_db"


class GoogleFormToEngagementDBSyncStats(SyncStats):
    def __init__(self):
        super().__init__({
            GoogleFormSyncEvents.READ_RESPONSE_FROM_GOOGLE_FORM: 0,
            GoogleFormSyncEvents.READ_ANSWER_FROM_RESPONSE: 0,
            GoogleFormSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB: 0,
            GoogleFormSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB: 0
        })

    def print_summary(self):
        log.info(f"Responses read from Google Form(s): {self.event_counts[GoogleFormSyncEvents.READ_RESPONSE_FROM_GOOGLE_FORM]}")
        log.info(f"Answers read from responses: {self.event_counts[GoogleFormSyncEvents.READ_ANSWER_FROM_RESPONSE]}")
        log.info(f"Messages already in engagement db: {self.event_counts[GoogleFormSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB]}")
        log.info(f"Messages added to engagement db: {self.event_counts[GoogleFormSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB]}")
