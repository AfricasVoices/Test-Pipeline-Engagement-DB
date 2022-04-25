import csv
from io import StringIO

from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import TimeUtils
from dateutil.parser import isoparse
from storage.google_cloud import google_cloud_utils

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
from src.engagement_db_to_analysis.membership_group import (tag_membership_groups_participants)

from src.engagement_db_to_analysis.rapid_pro_advert_functions import sync_advert_contacts_to_rapid_pro


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


def exclude_messages_in_csv_url(google_cloud_credentials_file_path, messages_traced_data, exclude_messages_csv_url):
    """
    Filters out messages listed in a csv in Google Cloud Storage.

    For each message in this csv, the closest message in time in `messages_traced_data` with the same participant uuid
    and text will be excluded.

    :param google_cloud_credentials_file_path: Path to credentials to use to access the csv.
    :type google_cloud_credentials_file_path: str
    :param messages_traced_data: Messages to filter.
    :type messages_traced_data: list of core_data_modules.traced_data.TracedData
    :param exclude_messages_csv_url: GS URL to a CSV containing messages to exclude.
                                     The CSV must contain the headings 'avf-participant-uuid', 'text', and 'timestamp'.
    :type exclude_messages_csv_url: str
    :return: `messages_traced_data`, with the best matching messages to those in the exclusion csv removed.s
    :rtype: list of core_data_modules.traced_data.TracedData
    """
    log.info(f"Downloading messages to exclude from {exclude_messages_csv_url}...")
    messages_to_exclude_csv = \
        google_cloud_utils.download_blob_to_string(google_cloud_credentials_file_path, exclude_messages_csv_url)
    messages_to_exclude = list(csv.DictReader(StringIO(messages_to_exclude_csv)))
    log.info(f"Downloaded {len(messages_to_exclude)} messages to exclude")

    log.info(f"Searching for matching messages in the messages_traced_data...")
    matching_message_ids = set()
    for i, exclude_msg in enumerate(messages_to_exclude):
        # Search the messages traced data for all the possible matches to this message to exclude.
        # Possible matches are those message with the same text and participant_uuid which haven't already matched
        # a message to be excluded.
        possible_matching_messages = []
        for msg_td in messages_traced_data:
            if msg_td["message_id"] not in matching_message_ids and \
                    msg_td["participant_uuid"] == exclude_msg["avf-participant-uuid"] and (
                    msg_td["text"] == exclude_msg["text"] or msg_td["text"] is None and exclude_msg["text"] == ""):
                possible_matching_messages.append(msg_td)

        log.debug(f"Found {len(possible_matching_messages)} possible matches for message {i + 1}")

        # Find the nearest message in time to this duplicate
        timestamp_of_duplicate = isoparse(exclude_msg["timestamp"])
        possible_matching_messages.sort(
            key=lambda msg_td: abs((isoparse(msg_td["timestamp"]) - timestamp_of_duplicate).total_seconds())
        )
        nearest_match = possible_matching_messages[0]
        nearest_timestamp = isoparse(nearest_match["timestamp"])
        log.debug(f"Found best match: message_id '{nearest_match['message_id']}', time difference "
                  f"{(nearest_timestamp - timestamp_of_duplicate).total_seconds()} seconds")

        # Record that this nearest matching message should be excluded from the returned result set
        matching_message_ids.add(nearest_match["message_id"])

    # Return all messages except those that matched messages we were to exclude.
    filtered_messages = [msg for msg in messages_traced_data if msg["message_id"] not in matching_message_ids]
    log.info(f"Returning {len(filtered_messages)}/{len(messages_traced_data)} messages after excluding "
             f"{len(messages_to_exclude)} requested messages")
    return filtered_messages


def export_traced_data(traced_data, export_path):
    with open(export_path, "w") as f:
        TracedDataJsonIO.export_traced_data_iterable_to_jsonl(traced_data, f)


def generate_analysis_files(user, google_cloud_credentials_file_path, pipeline_config, uuid_table, engagement_db, rapid_pro,
                            membership_group_dir_path,output_dir, cache_path=None, dry_run=False):

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
    messages_map = get_messages_in_datasets(engagement_db, engagement_db_datasets, cache, dry_run)

    messages_traced_data = _convert_messages_to_traced_data(user, messages_map)

    if pipeline_config.analysis.messages_to_exclude_csv_url is not None:
        messages_traced_data = exclude_messages_in_csv_url(
            google_cloud_credentials_file_path, messages_traced_data, pipeline_config.analysis.messages_to_exclude_csv_url
        )

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
        log.info("Tagging membership group participants to messages_by_column traced data...")
        tag_membership_groups_participants(user, google_cloud_credentials_file_path, messages_by_column,
                                           membership_group_csv_urls, membership_group_dir_path)

        log.info("Tagging membership group participants to participants_by_column traced data...")
        tag_membership_groups_participants(user, google_cloud_credentials_file_path, participants_by_column,
                                           membership_group_csv_urls, membership_group_dir_path)

    export_analysis_file(messages_by_column, pipeline_config, f"{output_dir}/messages.csv", export_timestamps=True)
    export_analysis_file(participants_by_column, pipeline_config, f"{output_dir}/participants.csv")

    export_traced_data(messages_by_column, f"{output_dir}/messages.jsonl")
    export_traced_data(participants_by_column, f"{output_dir}/participants.jsonl")

    run_automated_analysis(messages_by_column, participants_by_column, pipeline_config.analysis, f"{output_dir}/automated-analysis")

    dry_run_text = "(dry run)" if dry_run else ""
    if pipeline_config.analysis.google_drive_upload is None:
        log.debug(f"Not uploading to Google Drive, because the 'google_drive_upload' configuration was None {dry_run_text}")
    else:
        if dry_run:
            log.info(f"Not uploading to Google Drive {dry_run_text}")
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

    if pipeline_config.rapid_pro_target is not None and pipeline_config.rapid_pro_target.sync_config.sync_advert_contacts:
        sync_advert_contacts_to_rapid_pro(
            participants_by_column, uuid_table, pipeline_config, rapid_pro,
            google_cloud_credentials_file_path, membership_group_dir_path, cache_path
        )
