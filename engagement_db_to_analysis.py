import argparse
import importlib

from core_data_modules.logging import Logger

from src.engagement_db_to_analysis.engagement_db_to_analysis import get_project_messages_from_engagement_db


log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs the engagement to analysis phases of the pipeline")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket"),
    parser.add_argument("engagement_db_datasets_cache_dir", metavar="engagement-db-datasets-cache-dir",
                        help="Directory containing engagement_db dataset files generated by a previous run of this pipeline."
                             "new or changed messages will be updated to these files.")
    parser.add_argument("configuration_module",
                        help="Configuration module to import e.g. 'configurations.test_config'. "
                             "This module must contain a PIPELINE_CONFIGURATION property")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    engagement_db_datasets_cache_dir = args.engagement_db_datasets_cache_dir
    pipeline_config = importlib.import_module(args.configuration_module).PIPELINE_CONFIGURATION

    pipeline = pipeline_config.pipeline_name

    uuid_table = pipeline_config.uuid_table.init_uuid_table_client(google_cloud_credentials_file_path)
    engagement_db = pipeline_config.engagement_database.init_engagement_db_client(google_cloud_credentials_file_path)

    data = get_project_messages_from_engagement_db(pipeline_config.coda_sync.sync_config.dataset_configurations,
                                                   engagement_db, engagement_db_datasets_cache_dir)
