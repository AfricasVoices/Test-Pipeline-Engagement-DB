import argparse
import subprocess

from core_data_modules.logging import Logger
from engagement_database.data_models import HistoryEntryOrigin
from rapid_pro_tools.rapid_pro_client import RapidProClient

from configurations import test_pipeline_configuration
from src.engagement_db_to_rapid_pro.configuration import DatasetConfiguration, EngagementDBToRapidProConfiguration, \
    SyncModes
from src.engagement_db_to_rapid_pro.engagement_db_to_rapid_pro import sync_engagement_db_to_rapid_pro

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Syncs data from an engagement database to Rapid Pro")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    # TODO: Load this from a configuration_file_path argument
    pipeline_config = test_pipeline_configuration.PIPELINE_CONFIGURATION

    pipeline = pipeline_config.pipeline_name
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    project = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()

    HistoryEntryOrigin.set_defaults(user, project, pipeline, commit)

    uuid_table = pipeline_config.uuid_table.init_uuid_table_client(google_cloud_credentials_file_path)
    engagement_db = pipeline_config.engagement_database.init_engagement_db_client(google_cloud_credentials_file_path)
    rapid_pro = pipeline_config.rapid_pro_target.rapid_pro.init_rapid_pro_client(google_cloud_credentials_file_path)
    sync_config = pipeline_config.rapid_pro_target.sync_config

    sync_engagement_db_to_rapid_pro(engagement_db, rapid_pro, uuid_table, sync_config)
