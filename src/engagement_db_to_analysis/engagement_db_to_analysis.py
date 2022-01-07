from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import TimeUtils

from src.common.get_messages_in_datasets import get_messages_in_datasets
from src.engagement_db_to_analysis import google_drive_upload
from src.engagement_db_to_analysis.analysis_files import export_production_file, export_analysis_file
from src.engagement_db_to_analysis.automated_analysis import run_automated_analysis
from src.engagement_db_to_analysis.cache import AnalysisCache
from src.engagement_db_to_analysis.code_imputation_functions import (impute_codes_by_message,
                                                                     impute_codes_by_column_traced_data)
from src.engagement_db_to_analysis.column_view_conversion import (convert_to_messages_column_format,
                                                                  convert_to_participants_column_format)
from src.engagement_db_to_analysis.traced_data_filters import filter_messages
from src.engagement_db_to_analysis.membership_group import (get_membership_groups_csvs,
                                                            tag_membership_groups_participants)

log = Logger(__name__)


def _convert_messages_to_traced_data(user, messages_map):
    """
    Converts messages dict objects to TracedData objects.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_map: Dict of engagement db dataset -> list of Messages in that dataset.
    :type messages_map: dict of str -> list of engagement_database.data_models.Message
    :return: A list of Traced data message objects.
    :type: list of Traced data
    """
    messages_traced_data = []
    for engagement_db_dataset in messages_map:
        engagement_db_dataset_messages = messages_map[engagement_db_dataset]
        for msg in engagement_db_dataset_messages:
            messages_traced_data.append(TracedData(
                msg.to_dict(serialize_datetimes_to_str=True),
                Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
            ))

    log.info(f"Converted {len(messages_traced_data)} raw messages to TracedData")

    return messages_traced_data


def export_traced_data(traced_data, export_path):
    with open(export_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_jsonl(traced_data, f)


def generate_analysis_files(user, google_cloud_credentials_file_path, pipeline_config, engagement_db, membership_group_dir_path,
                            output_dir, cache_path=None):
    analysis_dataset_configurations = pipeline_config.analysis.dataset_configurations
    # TODO: Tidy up which functions get passed analysis_configs and which get passed dataset_configurations

    if cache_path is None:
        cache = None
        log.warning(f"No `cache_path` provided. This tool will perform a full download of project messages from engagement database")
    else:
        log.info(f"Initialising EngagementAnalysisCache at '{cache_path}/engagement_db_to_analysis'")
        cache = AnalysisCache(f"{cache_path}/engagement_db_to_analysis")

    engagement_db_datasets = []
    for config in analysis_dataset_configurations:
        engagement_db_datasets.extend(config.engagement_db_datasets)
    messages_map = get_messages_in_datasets(engagement_db, engagement_db_datasets, cache)

    messages_traced_data = _convert_messages_to_traced_data(user, messages_map)

    messages_traced_data = filter_messages(user, messages_traced_data, pipeline_config)

    impute_codes_by_message(
        user, messages_traced_data, analysis_dataset_configurations,
        pipeline_config.analysis.ws_correct_dataset_code_scheme
    )

    messages_by_column = convert_to_messages_column_format(user, messages_traced_data, pipeline_config.analysis)
    participants_by_column = convert_to_participants_column_format(user, messages_traced_data, pipeline_config.analysis)

    log.info(f"Imputing messages column-view traced data...")
    impute_codes_by_column_traced_data(user, messages_by_column, pipeline_config.analysis.dataset_configurations)

    log.info(f"Imputing participants column-view traced data...")
    impute_codes_by_column_traced_data(user, participants_by_column, pipeline_config.analysis.dataset_configurations)

    # Export to hard-coded files for now.
    export_production_file(messages_by_column, pipeline_config.analysis, f"{output_dir}/production.csv")

    if pipeline_config.analysis.membership_group_configuration is not None:

        membership_group_csv_urls = pipeline_config.analysis.membership_group_configuration.membership_group_csv_urls.items()

        log.info("Downloading membership groups CSVs from g-cloud...")
        get_membership_groups_csvs(google_cloud_credentials_file_path, membership_group_csv_urls, membership_group_dir_path)

        log.info("Tagging membership group participants to messages_by_column traced data...")
        tag_membership_groups_participants(user, messages_by_column, membership_group_csv_urls, membership_group_dir_path)

        log.info("Tagging membership group participants to participants_by_column traced data...")
        tag_membership_groups_participants(user, participants_by_column, membership_group_csv_urls, membership_group_dir_path)

    export_analysis_file(messages_by_column, pipeline_config, f"{output_dir}/messages.csv", export_timestamps=True)
    export_analysis_file(participants_by_column, pipeline_config, f"{output_dir}/participants.csv")

    export_traced_data(messages_by_column, f"{output_dir}/messages.jsonl")
    export_traced_data(participants_by_column, f"{output_dir}/participants.jsonl")

    run_automated_analysis(messages_by_column, participants_by_column, pipeline_config.analysis, f"{output_dir}/automated-analysis")

    if pipeline_config.analysis.google_drive_upload is None:
        log.debug("Not uploading to Google Drive, because the 'google_drive_upload' configuration was None")
    else:
        log.info("Uploading outputs to Google Drive...")
        google_drive_upload.init_client(
            google_cloud_credentials_file_path,
            pipeline_config.analysis.google_drive_upload.credentials_file_url
        )

        drive_dir = pipeline_config.analysis.google_drive_upload.drive_dir
        google_drive_upload.upload_file(f"{output_dir}/production.csv", drive_dir)
        google_drive_upload.upload_file(f"{output_dir}/messages.csv", drive_dir)
        google_drive_upload.upload_file(f"{output_dir}/participants.csv", drive_dir)
        google_drive_upload.upload_all_files_in_dir(
            f"{output_dir}/automated-analysis", f"{drive_dir}/automated-analysis", recursive=True
        )
