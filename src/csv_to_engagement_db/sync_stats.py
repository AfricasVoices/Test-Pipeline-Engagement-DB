from core_data_modules.logging import Logger

from src.common.sync_stats import SyncStats

log = Logger(__name__)


class CSVSyncEvents:
    READ_ROW_FROM_CSV = "read_row_from_csv"
    MESSAGE_ALREADY_IN_ENGAGEMENT_DB = "message_already_in_engagement_db"
    ADD_MESSAGE_TO_ENGAGEMENT_DB = "add_message_to_engagement_db"
    MESSAGE_SKIPPED_NO_MATCHING_TIMESTAMP = "message_skipped_no_matching_timestamp"


class CSVToEngagementDBSyncStats(SyncStats):
    def __init__(self):
        super().__init__({
            CSVSyncEvents.READ_ROW_FROM_CSV: 0,
            CSVSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB: 0,
            CSVSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB: 0,
            CSVSyncEvents.MESSAGE_SKIPPED_NO_MATCHING_TIMESTAMP: 0
        })

    def print_summary(self):
        log.info(f"CSV rows read: {self.event_counts[CSVSyncEvents.READ_ROW_FROM_CSV]}")
        log.info(f"Messages already in engagement db: {self.event_counts[CSVSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB]}")
        log.info(f"Messages added to engagement db: {self.event_counts[CSVSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB]}")
        log.info(f"Messages skipped because they didn't match a dataset time-range: {self.event_counts[CSVSyncEvents.MESSAGE_SKIPPED_NO_MATCHING_TIMESTAMP]}")
