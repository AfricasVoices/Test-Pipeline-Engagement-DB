import argparse
import importlib
import subprocess

from core_data_modules.logging import Logger
from engagement_database.data_models import HistoryEntryOrigin

from src.engagement_db_coda_sync.coda_to_engagement_db import sync_coda_to_engagement_db
from src.engagement_db_coda_sync.lib import ensure_coda_datasets_up_to_date

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Syncs data from Coda to an engagement database")

    parser.add_argument("--dry-run", action="store_true",
                        help="Logs the updates that would be made without updating anything.")
    parser.add_argument("--incremental-cache-path",
                        help="Path to a directory to use to cache results needed for incremental operation.")
    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("configuration_module",
                        help="Configuration module to import e.g. 'configurations.test_config'. "
                             "This module must contain a PIPELINE_CONFIGURATION property")

    args = parser.parse_args()

    dry_run = args.dry_run
    incremental_cache_path = args.incremental_cache_path

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_config = importlib.import_module(args.configuration_module).PIPELINE_CONFIGURATION

    pipeline = pipeline_config.pipeline_name
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    project = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()

    HistoryEntryOrigin.set_defaults(user, project, pipeline, commit)

    dry_run_text = "(dry run)" if dry_run else ""
    log.info(f"Synchronizing data from Coda to an engagement database {dry_run_text}")

    if pipeline_config.coda_sync is None:
        log.info(f"No Coda sync configuration provided; exiting")
        exit(0)

    uuid_table = pipeline_config.uuid_table.init_uuid_table_client(google_cloud_credentials_file_path)
    engagement_db = pipeline_config.engagement_database.init_engagement_db_client(google_cloud_credentials_file_path)
    coda = pipeline_config.coda_sync.coda.init_coda_client(google_cloud_credentials_file_path)

    ensure_coda_datasets_up_to_date(coda, pipeline_config.coda_sync.sync_config, google_cloud_credentials_file_path, dry_run)
    sync_coda_to_engagement_db(coda, engagement_db, pipeline_config.coda_sync.sync_config, incremental_cache_path, dry_run)
