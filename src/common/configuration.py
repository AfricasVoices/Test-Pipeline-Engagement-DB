import json
from dataclasses import dataclass

from core_data_modules.logging import Logger
from engagement_database import EngagementDatabase
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)


@dataclass
# TODO: Convert from data-class once design is better tested
class EngagementDatabaseConfiguration:
    credentials_file_url: str
    database_path: str

    def init_engagement_db(self, google_cloud_credentials_file_path):
        log.info("Initialising engagement database...")
        credentials = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path,
            self.credentials_file_url
        ))

        engagement_db = EngagementDatabase.init_from_credentials(
            credentials,
            self.database_path
        )
        log.info("Initialised engagement database")

        return engagement_db


@dataclass
# TODO: Convert from data-class once design is better tested
class UUIDTableConfiguration:
    credentials_file_url: str
    table_name: str
    uuid_prefix: str

    def init_uuid_table(self, google_cloud_credentials_file_path):
        log.info("Initialising uuid table...")
        credentials = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path,
            self.credentials_file_url
        ))

        uuid_table = FirestoreUuidTable.init_from_credentials(
            credentials,
            self.table_name,
            self.uuid_prefix
        )
        log.info("Initialised uuid table")

        return uuid_table
