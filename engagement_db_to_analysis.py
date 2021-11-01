import argparse
import importlib

from core_data_modules.logging import Logger

from src.engagement_db_to_analysis.engagement_db_to_analysis import generate_analysis_files

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runs the engagement to analysis phases of the pipeline")

    parser.add_argument("--incremental-cache-path",
                        help="Path to a directory to use to cache results needed for incremental operation.")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket"),
    parser.add_argument("configuration_module",
                        help="Configuration module to import e.g. 'configurations.test_config'. "
                             "This module must contain a PIPELINE_CONFIGURATION property")
    parser.add_argument("membership_group_dir_path",
                        help="Path to a directory to use to read & write membership group csvs to.")
    parser.add_argument("output_dir", metavar="output-dir",
                        help="Directory to output all analysis results to. This script will create and organise the "
                             "outputs into sensible sub-directories automatically")

    args = parser.parse_args()

    incremental_cache_path = args.incremental_cache_path
    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    pipeline_config = importlib.import_module(args.configuration_module).PIPELINE_CONFIGURATION
    membership_group_dir_path = args.membership_group_dir_path
    output_dir = args.output_dir

    pipeline = pipeline_config.pipeline_name

    uuid_table = pipeline_config.uuid_table.init_uuid_table_client(google_cloud_credentials_file_path)
    engagement_db = pipeline_config.engagement_database.init_engagement_db_client(google_cloud_credentials_file_path)

    generate_analysis_files(user, google_cloud_credentials_file_path, pipeline_config, engagement_db, membership_group_dir_path,
    output_dir, incremental_cache_path)
