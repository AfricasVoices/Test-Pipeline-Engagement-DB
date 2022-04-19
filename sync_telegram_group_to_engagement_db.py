import argparse
import importlib
import subprocess
import asyncio

from core_data_modules.logging import Logger
from engagement_database.data_models import HistoryEntryOrigin

from src.telegram_to_engagement_db.telegram_group_to_engagement_db import (sync_messages_from_groups_to_engagement_db,
                                                                           _initialize_telegram_client)

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Syncs data from telegram groups to an engagement database")

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

    incremental_cache_path = args.incremental_cache_path
    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_config = importlib.import_module(args.configuration_module).PIPELINE_CONFIGURATION

    pipeline = pipeline_config.pipeline_name
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    project = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()

    HistoryEntryOrigin.set_defaults(user, project, pipeline, commit)

    if pipeline_config.telegram_group_sources is None:
        log.info(f"No Telegram group sources specified; exiting")
        exit(0)

    engagement_db = pipeline_config.engagement_database.init_engagement_db_client(google_cloud_credentials_file_path)
    uuid_table = pipeline_config.uuid_table.init_uuid_table_client(google_cloud_credentials_file_path)

    async def main():
        for telegram_group_source in pipeline_config.telegram_group_sources:

            telegram = await _initialize_telegram_client(telegram_group_source.token_file_url,
                                                         google_cloud_credentials_file_path, pipeline)

            await sync_messages_from_groups_to_engagement_db(telegram_group_source, telegram,
                                                             engagement_db, uuid_table, incremental_cache_path)

    main_coroutine = main()
    asyncio.run(main_coroutine)
