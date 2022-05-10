import argparse
import importlib
import subprocess

from core_data_modules.logging import Logger
from engagement_database.data_models import HistoryEntryOrigin

from src.facebook_to_engagement_db.facebook_to_engagement_db import sync_facebook_to_engagement_db

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Syncs data from target facebook posts to an engagement database")

    parser.add_argument("--incremental-cache-path",
                        help="Path to a directory to use to cache results needed for incremental operation.")
    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("configuration_module",
                        help="Configuration module to import e.g. 'configurations.test_config'. "
                             "This module must contain a PIPELINE_CONFIGURATION property"),
    parser.add_argument("metrics_dir_path", metavar="metrics-dir-path",
                        help="Path to a directory to save any fetch-time engagement metrics to, for example "
                             "facebook post engagement metrics")


    args = parser.parse_args()

    incremental_cache_path = args.incremental_cache_path
    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    metrics_dir_path = args.metrics_dir_path #TODO write to dashboard firebase db for graphing
    pipeline_config = importlib.import_module(args.configuration_module).PIPELINE_CONFIGURATION

    pipeline = pipeline_config.pipeline_name
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    project = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()

    HistoryEntryOrigin.set_defaults(user, project, pipeline, commit)

    if pipeline_config.facebook_sources is None:
        log.info(f"No Facebook sources specified; exiting")
        exit(0)

    engagement_db = pipeline_config.engagement_database.init_engagement_db_client(google_cloud_credentials_file_path)
    uuid_table = pipeline_config.uuid_table.init_uuid_table_client(google_cloud_credentials_file_path)

    sync_facebook_to_engagement_db(google_cloud_credentials_file_path, pipeline_config.facebook_sources, engagement_db,
                                   uuid_table, metrics_dir_path, incremental_cache_path)
