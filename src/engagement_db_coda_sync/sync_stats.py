from core_data_modules.logging import Logger

log = Logger(__name__)


class CodaSyncEvents:
    READ_MESSAGE = "read_message"
    SET_CODA_ID = "set_coda_id"
    ADD_TO_CODA = "add_to_coda"
    LABELS_MATCH = "labels_match"
    UPDATE_ENGAGEMENT_DB_LABELS = "update_engagement_db_labels"
    WS_CORRECTION = "ws_correction"


class CodaSyncStats:
    def __init__(self):
        self.event_counts = {
            CodaSyncEvents.READ_MESSAGE: 0,
            CodaSyncEvents.SET_CODA_ID: 0,
            CodaSyncEvents.ADD_TO_CODA: 0,
            CodaSyncEvents.LABELS_MATCH: 0,
            CodaSyncEvents.UPDATE_ENGAGEMENT_DB_LABELS: 0,
            CodaSyncEvents.WS_CORRECTION: 0
        }

    def add_event(self, event):
        if event not in self.event_counts:
            self.event_counts[event] = 0
        self.event_counts[event] += 1

    def add_stats(self, stats):
        for k, v in stats.event_counts.items():
            self.event_counts[k] += v

    def print_summary(self):
        log.info(f"Messages read: {self.event_counts[CodaSyncEvents.READ_MESSAGE]}")
        log.info(f"Coda ids set: {self.event_counts[CodaSyncEvents.SET_CODA_ID]}")
        log.info(f"Messages added to Coda: {self.event_counts[CodaSyncEvents.ADD_TO_CODA]}")
        log.info(f"Messages updated with labels from Coda: {self.event_counts[CodaSyncEvents.UPDATE_ENGAGEMENT_DB_LABELS]}")
        log.info(f"Messages with labels already matching Coda: {self.event_counts[CodaSyncEvents.LABELS_MATCH]}")
        log.info(f"Messages WS-corrected: {self.event_counts[CodaSyncEvents.WS_CORRECTION]}")
