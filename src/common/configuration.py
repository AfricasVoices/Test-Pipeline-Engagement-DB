import json
from dataclasses import dataclass

from coda_v2_python_client.firebase_client_wrapper import CodaV2Client
from core_data_modules.logging import Logger
from engagement_database import EngagementDatabase
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from rapid_pro_tools.rapid_pro_client import RapidProClient
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)


class EngagementDatabaseClientConfiguration:
    def __init__(self, credentials_file_url, database_path):
        """
        Configuration for creating an EngagementDatabase client.

        :param credentials_file_url: GS URL to the Firebase credentials file to use to initialise the client.
        :type credentials_file_url: str
        :param database_path: Path to the engagement database within the Firebase project's Firestore
                              e.g. "engagement_databases/test_database"
        :type database_path: str
        """
        self.credentials_file_url = credentials_file_url
        self.database_path = database_path

    def init_engagement_db_client(self, google_cloud_credentials_file_path):
        """
        Initialises an EngagementDatabase client from this configuration.

        :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                                   access the credentials bucket.
        :type google_cloud_credentials_file_path: str
        :rtype: engagement_database.EngagementDatabase
        """
        log.info("Initialising engagement database client...")
        credentials = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path,
            self.credentials_file_url
        ))

        engagement_db = EngagementDatabase.init_from_credentials(
            credentials,
            self.database_path
        )
        log.info("Initialised engagement database client")

        return engagement_db


class UUIDTableClientConfiguration:
    def __init__(self, credentials_file_url, table_name, uuid_prefix):
        """
        Configuration for creating a FirestoreUuidTable client.

        :param credentials_file_url: GS URL to the Firebase credentials file to use to initialise the client.
        :type credentials_file_url: str
        :param table_name: Name of the table to connect to in the Firestore e.g. "urn_to_uuid_test"
        :type table_name: str
        :param uuid_prefix: Prefix to give the generated uuids in the table e.g. "avf-participant-id-"
        :type uuid_prefix: str
        """
        self.credentials_file_url = credentials_file_url
        self.table_name = table_name
        self.uuid_prefix = uuid_prefix

    def init_uuid_table_client(self, google_cloud_credentials_file_path):
        """
        Initialises a FirestoreUuidTable client from this configuration.

        :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                                   access the credentials bucket.
        :type google_cloud_credentials_file_path: str
        :rtype: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
        """
        log.info("Initialising uuid table client...")
        credentials = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path,
            self.credentials_file_url
        ))

        uuid_table = FirestoreUuidTable.init_from_credentials(
            credentials,
            self.table_name,
            self.uuid_prefix
        )
        log.info("Initialised uuid table client")

        return uuid_table


class RapidProClientConfiguration:
    def __init__(self, domain, token_file_url):
        """
        Configuration for creating a RapidProClient.

        :param domain: Server hostname, e.g. 'rapidpro.io'
        :type domain: str
        :param token_file_url: GS URL to a file containing the TextIt organization access token.
        :type token_file_url: str
        """
        self.domain = domain
        self.token_file_url = token_file_url

    def init_rapid_pro_client(self, google_cloud_credentials_file_path):
        """
        Initialises a RapidProClient from this configuration.

        :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                                   access the credentials bucket.
        :type google_cloud_credentials_file_path: str
        :rtype: rapid_pro_tools.rapid_pro_client.RapidProClient
        """
        log.info(f"Initialising Rapid Pro client for domain {self.domain} and auth url {self.token_file_url}...")
        rapid_pro_token = google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, self.token_file_url).strip()
        rapid_pro_client = RapidProClient(self.domain, rapid_pro_token)
        log.info("Initialised Rapid Pro client")

        return rapid_pro_client


class CodaClientConfiguration:
    def __init__(self, credentials_file_url):
        """
        Configuration for creating a CodaV2Client.

        :param credentials_file_url: GS URL to the Firebase credentials file to use to initialise the client.
        :type credentials_file_url: str
        """
        self.credentials_file_url = credentials_file_url

    def init_coda_client(self, google_cloud_credentials_file_path):
        """
        Initialises a CodeV2Client from this configuration.

        :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                                   access the credentials bucket.
        :type google_cloud_credentials_file_path: str
        :rtype: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
        """
        log.info("Initialising Coda client...")
        credentials = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path,
            self.credentials_file_url
        ))

        coda = CodaV2Client.init_client(credentials)
        log.info("Initialised Coda client")

        return coda


@dataclass
class ArchiveConfiguration:
    archive_upload_bucket: str
    bucket_dir_path: str

@dataclass
class OperationsDashboardConfiguration:
    credentials_file_url: str


@dataclass
class PipelineEvents(object):
    PIPELINE_RUN_START = "PipelineRunStart"
    PIPELINE_RUN_END = "PipelineRunEnd"
