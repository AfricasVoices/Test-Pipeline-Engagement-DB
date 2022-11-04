import argparse
import csv
import importlib

from core_data_modules.logging import Logger

from src.common.cache import Cache
from src.common.get_messages_in_datasets import get_duplicate_origin_ids_in_datasets


def get_engagement_db_datasets(pipeline_config):
    engagement_db_datasets = set()
    if pipeline_config.google_form_sources is not None:
        for google_form_source in pipeline_config.google_form_sources:
            for config in google_form_source.sync_config.question_configurations:
                if config.engagement_db_dataset not in engagement_db_datasets:
                    engagement_db_datasets.add(config.engagement_db_dataset)

    if pipeline_config.rapid_pro_sources is not None:
        for rapid_pro_source in pipeline_config.rapid_pro_sources:
            for config in rapid_pro_source.sync_config.flow_result_configurations:
                if config.engagement_db_dataset not in engagement_db_datasets:
                    engagement_db_datasets.add(config.engagement_db_dataset)

    if pipeline_config.facebook_sources is not None:
        for facebook_source in pipeline_config.facebook_sources:
            for facebook_dataset in facebook_source.datasets:
                if facebook_dataset.engagement_db_dataset not in engagement_db_datasets:
                    engagement_db_datasets.add(facebook_dataset.engagement_db_dataset)

    if pipeline_config.telegram_group_sources is not None:
        for telegram_group_source in pipeline_config.telegram_group_sources:
            for telegram_group_dataset in telegram_group_source.datasets:
                if telegram_group_dataset.engagement_db_dataset not in engagement_db_datasets:
                    engagement_db_datasets.add(telegram_group_dataset.engagement_db_dataset)

    if pipeline_config.csv_sources is not None:
        for csv_source in pipeline_config.csv_sources:
            for config in csv_source.engagement_db_datasets:
                if config.engagement_db_dataset not in engagement_db_datasets:
                    engagement_db_datasets.add(csv_source.engagement_db_dataset)

    return list(engagement_db_datasets)


log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gets messages from all datasets, computes duplicate"
                                                 "origin ids in engagement db and writes them in a csv")

    parser.add_argument("--dry-run", action="store_true",
                        help="Logs the updates that would be made without updating anything.")
    parser.add_argument("--incremental-cache-path",
                        help="Path to a directory to use to cache results needed for incremental operation.")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket"),
    parser.add_argument("configuration_module",
                        help="Configuration module to import e.g. 'configurations.test_config'. "
                             "This module must contain a PIPELINE_CONFIGURATION property")
    parser.add_argument("duplicate_origin_ids_output_path", metavar="duplicate-origin-ids-output-path",
                        help="Path to write the duplicate origin ids CSV to")

    args = parser.parse_args()

    dry_run = args.dry_run
    incremental_cache_path = args.incremental_cache_path

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_config = importlib.import_module(args.configuration_module).PIPELINE_CONFIGURATION
    duplicate_origin_ids_output_path = args.duplicate_origin_ids_output_path

    pipeline = pipeline_config.pipeline_name

    dry_run_text = "(dry run)" if dry_run else ""
    log.info(f"Computing duplicate engagement db origin ids in pipeline: {pipeline} {dry_run_text}")

    engagement_db = pipeline_config.engagement_database.init_engagement_db_client(google_cloud_credentials_file_path)
    engagement_db_datasets = get_engagement_db_datasets(pipeline_config)
    log.info(f"Found {len(engagement_db_datasets)} engagement db datasets from the data sources configured in {pipeline} pipeline")

    if incremental_cache_path is None:
        cache = None
        log.warning(f"No `cache_path` provided. This tool will perform a full download of project messages from engagement database")
    else:
        log.info(f"Initialising Cache at '{incremental_cache_path}/raw_engement_db_messages'")
        cache = Cache(f"{incremental_cache_path}/raw_engement_db_messages")

    duplicate_origin_ids = get_duplicate_origin_ids_in_datasets(engagement_db, list(engagement_db_datasets), cache, dry_run)

    fields = ["origin_id", "engagement_db_datasets", "duplicate_origin_id_count"]
    log.info(f"Exporting duplicate origin ids to {duplicate_origin_ids_output_path}")
    with open(duplicate_origin_ids_output_path, "w") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields) 
        csvwriter.writerows(duplicate_origin_ids)
