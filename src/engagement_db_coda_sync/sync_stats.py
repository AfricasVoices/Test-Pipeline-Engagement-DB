from core_data_modules.logging import Logger

from src.common.sync_stats import SyncStats

log = Logger(__name__)


class CodaSyncEvents:
    READ_MESSAGE_FROM_ENGAGEMENT_DB = "read_message_from_engagement_db"
    SET_CODA_ID = "set_coda_id"
    SKIP_EMPTY_MESSAGE = "skip_empty_message"
    READ_MESSAGE_FROM_CODA = "read_message_from_coda"
    ADD_MESSAGE_TO_CODA = "add_message_to_coda"
    LABELS_MATCH = "labels_match"
    UPDATE_ENGAGEMENT_DB_LABELS = "update_engagement_db_labels"
    WS_CORRECTION = "ws_correction"


class EngagementDBToCodaSyncStats(SyncStats):
    def __init__(self):
        super().__init__({
            CodaSyncEvents.READ_MESSAGE_FROM_ENGAGEMENT_DB: 0,
            CodaSyncEvents.SET_CODA_ID: 0,
            CodaSyncEvents.SKIP_EMPTY_MESSAGE: 0,
            CodaSyncEvents.ADD_MESSAGE_TO_CODA: 0,
            CodaSyncEvents.LABELS_MATCH: 0,
            CodaSyncEvents.UPDATE_ENGAGEMENT_DB_LABELS: 0,
            CodaSyncEvents.WS_CORRECTION: 0
        })

    def print_summary(self):
        log.info(f"Messages read from engagement db: {self.event_counts[CodaSyncEvents.READ_MESSAGE_FROM_ENGAGEMENT_DB]}")
        log.info(f"Coda ids set: {self.event_counts[CodaSyncEvents.SET_CODA_ID]}")
        log.info(f"Skipped messages with text None or '': {self.event_counts[CodaSyncEvents.SKIP_EMPTY_MESSAGE]}")
        log.info(f"Messages added to Coda: {self.event_counts[CodaSyncEvents.ADD_MESSAGE_TO_CODA]}")
        log.info(f"Messages updated with labels from Coda: {self.event_counts[CodaSyncEvents.UPDATE_ENGAGEMENT_DB_LABELS]}")
        log.info(f"Messages with labels already matching Coda: {self.event_counts[CodaSyncEvents.LABELS_MATCH]}")
        log.info(f"Messages WS-corrected: {self.event_counts[CodaSyncEvents.WS_CORRECTION]}")


class CodaToEngagementDBSyncStats(SyncStats):
    def __init__(self):
        super().__init__({
            CodaSyncEvents.READ_MESSAGE_FROM_CODA: 0,
            CodaSyncEvents.READ_MESSAGE_FROM_ENGAGEMENT_DB: 0,
            CodaSyncEvents.LABELS_MATCH: 0,
            CodaSyncEvents.UPDATE_ENGAGEMENT_DB_LABELS: 0,
            CodaSyncEvents.WS_CORRECTION: 0
        })

    def print_summary(self):
        log.info(f"Messages read from Coda: {self.event_counts[CodaSyncEvents.READ_MESSAGE_FROM_CODA]}")
        log.info(f"Messages read from engagement db: {self.event_counts[CodaSyncEvents.READ_MESSAGE_FROM_ENGAGEMENT_DB]}")
        log.info(f"Messages updated with labels from Coda: {self.event_counts[CodaSyncEvents.UPDATE_ENGAGEMENT_DB_LABELS]}")
        log.info(f"Messages with labels already matching Coda: {self.event_counts[CodaSyncEvents.LABELS_MATCH]}")
        log.info(f"Messages WS-corrected: {self.event_counts[CodaSyncEvents.WS_CORRECTION]}")
