import argparse
import subprocess
import uuid

from core_data_modules.logging import Logger
from engagement_database.data_models import HistoryEntryOrigin
from engagement_database.engagement_database import EngagementDatabase

from src.common.configuration import UUIDTableConfiguration, EngagementDatabaseConfiguration
from src.rapid_pro_to_engagement_db.configuration import RapidProToEngagementDBConfiguration, FlowResultConfiguration
from src.rapid_pro_to_engagement_db.rapid_pro_to_engagement_db import sync_rapid_pro_to_engagement_db
from test.mock_uuid_table import MockUuidTable

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Syncs data from a Rapid Pro workspace to an engagement database")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path

    pipeline = "engagement-db-test"
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    project = "Test"  # subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()

    HistoryEntryOrigin.set_globals(user, project, pipeline, commit)

    uuid_table_configuration = UUIDTableConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_test",
        uuid_prefix="avf-participant-uuid-"
    )

    engagement_db_configuration = EngagementDatabaseConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/experimental_test"
    )

    rapid_pro_config = RapidProToEngagementDBConfiguration(
        domain="textit.in",
        token_file_url="gs://avf-credentials/experimental-test-text-it-token.txt",
        flow_result_configurations=[
            FlowResultConfiguration("test_pipeline_daniel_activation", "rqa_s01e01", "s01e01"),
            FlowResultConfiguration("test_pipeline_daniel_demog", "constituency", "location"),
            FlowResultConfiguration("test_pipeline_daniel_demog", "age", "age"),
            FlowResultConfiguration("test_pipeline_daniel_demog", "gender", "gender"),
        ]
    )

    rapid_pro_config = RapidProToEngagementDBConfiguration(
        domain="textit.in",
        token_file_url="gs://avf-credentials/world-vision-textit-token.txt",
        flow_result_configurations=[
            FlowResultConfiguration("worldvision_s01e01_activation", "rqa_s01e01", "world_vision_s01e01"),
        ]
    )

    uuid_table = uuid_table_configuration.init_uuid_table(google_cloud_credentials_file_path)
    engagement_db = engagement_db_configuration.init_engagement_db(google_cloud_credentials_file_path)

    uuid_table = MockUuidTable()

    sync_rapid_pro_to_engagement_db(google_cloud_credentials_file_path, rapid_pro_config, engagement_db, uuid_table)

