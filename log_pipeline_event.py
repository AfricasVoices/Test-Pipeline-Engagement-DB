import argparse
import json
import importlib

from core_data_modules.logging import Logger
from core_data_modules.util import TimeUtils
from storage.google_cloud import google_cloud_utils
from pipeline_logs.firestore_pipeline_logger import FirestorePipelineLogger

from src.common.configuration import PipelineEvents

log = Logger(__name__)

def log_pipeline_event(pipeline_config, google_cloud_credentials_file_path,  run_id, event_key):
    log.info("Downloading Firestore Operations Dashboard Table credentials...")
    firestore_pipeline_logs_table_credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, pipeline_config.operations_dashboard.credentials_file_url
    ))

    log.info(f"Writing {event_key} event log for run_id: {run_id}")
    firestore_pipeline_logger = FirestorePipelineLogger(pipeline_config.pipeline_name, run_id,
                                                         firestore_pipeline_logs_table_credentials)

    firestore_pipeline_logger.log_event(TimeUtils.utc_now_as_iso_string(), event_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Updates current pipeline event/stage to a firebase table to aid in monitoring")

    parser.add_argument("configuration_module",
                        help="Configuration module to import e.g. 'configurations.test_config'. "
                             "This module must contain a PIPELINE_CONFIGURATION property")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")

    parser.add_argument("run_id", metavar="run-id",
                        help="Identifier of this pipeline run")
    parser.add_argument("event_key", metavar="event-key",
                        help="Key for this pipeline event/stage",
                        choices=[PipelineEvents.PIPELINE_RUN_START, PipelineEvents.PIPELINE_RUN_END]),

    args = parser.parse_args()

    pipeline_config = importlib.import_module(args.configuration_module).PIPELINE_CONFIGURATION

    log_pipeline_event(pipeline_config, args.google_cloud_credentials_file_path, args.run_id, args.event_key)
