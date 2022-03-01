from core_data_modules.logging import Logger

from src.common.sync_stats import SyncStats

log = Logger(__name__)


class RapidProSyncEvents:
    READ_RUN_FROM_RAPID_PRO = "read_run_from_rapid_pro"
    RUN_EMPTY = "run_empty"
    RUN_CONTACT_UUID_NOT_IN_CONTACTS = "run_contact_uuid_not_in_contacts"
    UUID_FILTER_CONTACT_NOT_IN_UUID_TABLE = "uuid_filter_contact_not_in_uuid_table"
    CONTACT_NOT_IN_UUID_FILTER = "contact_not_in_uuid_filter"
    MESSAGE_ALREADY_IN_ENGAGEMENT_DB = "message_already_in_engagement_db"
    ADD_MESSAGE_TO_ENGAGEMENT_DB = "add_message_to_engagement_db"


class RapidProToEngagementDBSyncStats(SyncStats):
    def __init__(self):
        super().__init__({
            RapidProSyncEvents.READ_RUN_FROM_RAPID_PRO: 0,
            RapidProSyncEvents.RUN_EMPTY: 0,
            RapidProSyncEvents.RUN_CONTACT_UUID_NOT_IN_CONTACTS: 0,
            RapidProSyncEvents.UUID_FILTER_CONTACT_NOT_IN_UUID_TABLE: 0,
            RapidProSyncEvents.CONTACT_NOT_IN_UUID_FILTER: 0,
            RapidProSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB: 0,
            RapidProSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB: 0
        })

    def print_summary(self):
        log.info(f"Runs downloaded from Rapid Pro: {self.event_counts[RapidProSyncEvents.READ_RUN_FROM_RAPID_PRO]}")
        log.info(f"Empty runs: {self.event_counts[RapidProSyncEvents.RUN_EMPTY]}")
        log.info(f"Runs with contact uuids not in contacts export: {self.event_counts[RapidProSyncEvents.RUN_CONTACT_UUID_NOT_IN_CONTACTS]}")
        log.info(f"Runs from contacts not in the uuid table when filtering uuids: {self.event_counts[RapidProSyncEvents.UUID_FILTER_CONTACT_NOT_IN_UUID_TABLE]}")
        log.info(f"Runs from contacts not in the uuid filter: {self.event_counts[RapidProSyncEvents.CONTACT_NOT_IN_UUID_FILTER]}")
        log.info(f"Messages already in engagement db: {self.event_counts[RapidProSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB]}")
        log.info(f"Messages added to engagement db: {self.event_counts[RapidProSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB]}")
