import argparse
import subprocess

from core_data_modules.logging import Logger
from engagement_database.data_models import HistoryEntryOrigin

from configurations import test_pipeline_configuration
from src.rapid_pro_to_engagement_db.rapid_pro_to_engagement_db import sync_rapid_pro_to_engagement_db

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
    project = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()

    HistoryEntryOrigin.set_defaults(user, project, pipeline, commit)

    pipeline_config = test_pipeline_configuration.PIPELINE_CONFIGURATION

    if pipeline_config.rapid_pro_sources is None or len(pipeline_config.rapid_pro_sources) == 0:
        log.info(f"No Rapid Pro sources specified; exiting")
        exit(0)

    uuid_table = pipeline_config.uuid_table.init_uuid_table(google_cloud_credentials_file_path)
    engagement_db = pipeline_config.engagement_database.init_engagement_db(google_cloud_credentials_file_path)

    for i, rapid_pro_config in enumerate(pipeline_config.rapid_pro_sources):
        log.info(f"Syncing Rapid Pro source {i + 1}/{len(pipeline_config.rapid_pro_sources)}...")
        rapid_pro = rapid_pro_config.rapid_pro.init_rapid_pro_client(google_cloud_credentials_file_path)

        sync_rapid_pro_to_engagement_db(rapid_pro, engagement_db, uuid_table, rapid_pro_config.flow_results)

