import argparse
import importlib
import subprocess

from core_data_modules.logging import Logger
from engagement_database.data_models import HistoryEntryOrigin

from src.rapid_pro_to_engagement_db.rapid_pro_archive_client import RapidProArchiveClient
from src.rapid_pro_to_engagement_db.rapid_pro_to_engagement_db import sync_rapid_pro_to_engagement_db

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Syncs data from a Rapid Pro workspace to an engagement database")

    parser.add_argument("--dry-run", action="store_true",
                        help="Logs the updates that would be made without updating anything.")
    parser.add_argument("--incremental-cache-path",
                        help="Path to a directory to use to cache results needed for incremental operation.")
    parser.add_argument("--local-archive", action="append",
                        help="Configures a local archive directory to use in place of a production Rapid Pro "
                             "workspace, in the form '<gs-url>=<local-path>' "
                             "e.g. --local-archive gs://bucket/test.json=~/test-archive"
                             "To configure multiple local archives, pass multiple --local-archive flags")
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
    local_archives = [] if args.local_archive is None else args.local_archive

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_config = importlib.import_module(args.configuration_module).PIPELINE_CONFIGURATION

    dry_run_text = "(dry run)" if dry_run else ""
    log.info(f"Synchronizing data from rapidpro to an engagement database {dry_run_text}")

    # Parse any local archive arguments, validating that all arguments do override a Rapid Pro source
    local_archives_map = dict()  # of gs url -> local path
    rapid_pro_urls = {rapid_pro_source.rapid_pro.token_file_url for rapid_pro_source in pipeline_config.rapid_pro_sources}
    overridden_urls = 0
    for archive_arg in local_archives:
        gs_url = archive_arg.split("=")[0]
        local_path = archive_arg.split("=")[1]
        local_archives_map[gs_url] = local_path

        assert gs_url in rapid_pro_urls, f"--local-archive url {gs_url} not found in any Rapid Pro sources"
        overridden_urls += 1
    log.info(f"{overridden_urls}/{len(pipeline_config.rapid_pro_sources)} Rapid Pro source urls will be overridden "
             f"with --local-archives")

    pipeline = pipeline_config.pipeline_name
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    project = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()

    HistoryEntryOrigin.set_defaults(user, project, pipeline, commit)

    if pipeline_config.rapid_pro_sources is None or len(pipeline_config.rapid_pro_sources) == 0:
        log.info(f"No Rapid Pro sources specified; exiting")
        exit(0)

    uuid_table = pipeline_config.uuid_table.init_uuid_table_client(google_cloud_credentials_file_path)
    engagement_db = pipeline_config.engagement_database.init_engagement_db_client(google_cloud_credentials_file_path)

    for i, rapid_pro_config in enumerate(pipeline_config.rapid_pro_sources):
        log.info(f"Syncing Rapid Pro source {i + 1}/{len(pipeline_config.rapid_pro_sources)}...")

        # If a local archive was specified for this gs url, use a rapid pro archive client, otherwise connect to the
        # production workspace.
        rapid_pro_token_url = rapid_pro_config.rapid_pro.token_file_url
        if rapid_pro_token_url in local_archives_map:
            log.info(f"Overriding Rapid Pro source {rapid_pro_token_url} with local archive "
                     f"{local_archives_map[rapid_pro_token_url]}")
            rapid_pro = RapidProArchiveClient(local_archives_map[rapid_pro_token_url])
        else:
            rapid_pro = rapid_pro_config.rapid_pro.init_rapid_pro_client(google_cloud_credentials_file_path)

        sync_rapid_pro_to_engagement_db(
            rapid_pro, engagement_db, uuid_table, rapid_pro_config.sync_config, google_cloud_credentials_file_path,
            incremental_cache_path, dry_run
        )
