from core_data_modules.logging import Logger
from core_data_modules.traced_data import TracedData, Metadata
from core_data_modules.traced_data.io import TracedDataJsonIO
from core_data_modules.util import TimeUtils

from src.engagement_db_to_analysis.analysis_files import export_production_file, export_analysis_file
from src.engagement_db_to_analysis.automated_analysis import run_automated_analysis
from src.engagement_db_to_analysis.cache import AnalysisCache
from src.engagement_db_to_analysis.code_imputation_functions import (impute_codes_by_message,
                                                                     impute_codes_by_column_traced_data)
from src.engagement_db_to_analysis.column_view_conversion import (convert_to_messages_column_format,
                                                                  convert_to_participants_column_format)
from src.engagement_db_to_analysis.traced_data_filters import filter_messages

log = Logger(__name__)


def _get_project_messages_from_engagement_db(analysis_dataset_configurations, engagement_db, cache_path=None):
    """
    Downloads project messages from engagement database. It performs a full download if there is no cache path and
    incrementally otherwise.

    :param analysis_dataset_configurations: Analysis dataset configurations in pipeline configuration module.
    :type analysis_dataset_configurations: list of src.engagement_db_to_analysis.configuration.AnalysisDatasetConfiguration
    :param engagement_db: Engagement database to download the messages from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str
    :return: engagement_db_dataset_messages_map of engagement_db_dataset to list of messages.
    :rtype: dict of str -> list of engagement_database.data_models.Message
    """

    if cache_path is None:
        cache = None
        log.warning(f"No `cache_path` provided. This tool will perform a full download of project messages from engagement database")
    else:
        log.info(f"Initialising EngagementAnalysisCache at '{cache_path}/engagement_db_to_analysis'")
        cache = AnalysisCache(f"{cache_path}/engagement_db_to_analysis")

    engagement_db_dataset_messages_map = {}  # of engagement_db_dataset to list of messages
    for analysis_dataset_config in analysis_dataset_configurations:
        for engagement_db_dataset in analysis_dataset_config.engagement_db_datasets:
            messages = []
            latest_message_timestamp = None if cache is None else cache.get_latest_message_timestamp(engagement_db_dataset)
            if latest_message_timestamp is not None:
                log.info(f"Performing incremental download for {engagement_db_dataset} messages...")

                # Download messages that have been updated/created after the previous run
                incremental_messages_filter = lambda q: q \
                    .where("dataset", "==", engagement_db_dataset) \
                    .where("last_updated", ">", latest_message_timestamp)

                updated_messages = engagement_db.get_messages(firestore_query_filter=incremental_messages_filter)
                messages.extend(updated_messages)

                # Check and remove cached messages that have been ws corrected away from this dataset after the previous
                # run. We do this by searching for all messages that used to be in this dataset, that we haven't
                # already seen.
                latest_ws_message_timestamp = cache.get_latest_message_timestamp(f"{engagement_db_dataset}_ws")
                if latest_ws_message_timestamp is None:
                    ws_corrected_messages_filter = lambda q: q \
                        .where("previous_datasets", "array_contains", engagement_db_dataset)
                else:
                    ws_corrected_messages_filter = lambda q: q \
                        .where("previous_datasets", "array_contains", engagement_db_dataset) \
                        .where("last_updated", ">", latest_ws_message_timestamp)

                ws_corrected_messages = engagement_db.get_messages(firestore_query_filter=ws_corrected_messages_filter)

                log.info(f"Downloaded {len(updated_messages)} updated messages in this dataset, and "
                         f"{len(ws_corrected_messages)} messages that were previously in this dataset but have moved.")

                # Update the latest seen ws message from this dataset
                if len(ws_corrected_messages) > 0:
                    for msg in ws_corrected_messages:
                        if latest_ws_message_timestamp is None or msg.last_updated > latest_ws_message_timestamp:
                            latest_ws_message_timestamp = msg.last_updated
                    cache.set_latest_message_timestamp(f"{engagement_db_dataset}_ws", latest_ws_message_timestamp)

                cache_messages = cache.get_messages(engagement_db_dataset)
                for msg in cache_messages:
                    if msg.message_id in {msg.message_id for msg in ws_corrected_messages}:
                        continue
                    messages.append(msg)

            else:
                log.warning(f"Performing a full download for {engagement_db_dataset} messages...")

                full_download_filter = lambda q: q \
                    .where("dataset", "==", engagement_db_dataset)

                messages = engagement_db.get_messages(firestore_query_filter=full_download_filter)
                log.info(f"Downloaded {len(messages)} messages")

            engagement_db_dataset_messages_map[engagement_db_dataset] = messages

            # Update latest_message_timestamp
            for msg in messages:
                msg_last_updated = msg.last_updated
                if latest_message_timestamp is None or msg_last_updated > latest_message_timestamp:
                    latest_message_timestamp = msg_last_updated

            if cache is not None:
                # Export latest message timestamp to cache.
                # Export as both the last seen for this dataset and for the ws case, as there will be no need to
                # check for ws messages that moved from this dataset before this initial fetch.
                if latest_message_timestamp is not None:
                    cache.set_latest_message_timestamp(engagement_db_dataset, latest_message_timestamp)
                    cache.set_latest_message_timestamp(f"{engagement_db_dataset}_ws", latest_message_timestamp)

                # Export project engagement_dataset files
                if len(messages) > 0:
                    cache.set_messages(engagement_db_dataset, messages)

    return engagement_db_dataset_messages_map


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


def generate_analysis_files(user, pipeline_config, engagement_db, output_dir, cache_path=None):
    analysis_dataset_configurations = pipeline_config.analysis.dataset_configurations
    # TODO: Tidy up which functions get passed analysis_configs and which get passed dataset_configurations

    messages_map = _get_project_messages_from_engagement_db(analysis_dataset_configurations, engagement_db, cache_path)

    messages_traced_data = _convert_messages_to_traced_data(user, messages_map)

    messages_traced_data = filter_messages(user, messages_traced_data, pipeline_config)

    impute_codes_by_message(user, messages_traced_data, analysis_dataset_configurations)

    messages_by_column = convert_to_messages_column_format(user, messages_traced_data, pipeline_config.analysis)
    participants_by_column = convert_to_participants_column_format(user, messages_traced_data, pipeline_config.analysis)

    log.info(f"Imputing messages column-view traced data...")
    impute_codes_by_column_traced_data(user, messages_by_column, pipeline_config.analysis.dataset_configurations)

    log.info(f"Imputing participants column-view traced data...")
    impute_codes_by_column_traced_data(user, participants_by_column, pipeline_config.analysis.dataset_configurations)

    # Export to hard-coded files for now.
    export_production_file(messages_by_column, pipeline_config.analysis, f"{output_dir}/production.csv")

    export_analysis_file(messages_by_column, pipeline_config.analysis.dataset_configurations, f"{output_dir}/messages.csv")
    export_analysis_file(participants_by_column, pipeline_config.analysis.dataset_configurations, f"{output_dir}/participants.csv")

    export_traced_data(messages_by_column, f"{output_dir}/messages.jsonl")
    export_traced_data(participants_by_column, f"{output_dir}/participants.jsonl")

    run_automated_analysis(messages_by_column, participants_by_column, pipeline_config.analysis, f"{output_dir}/automated_analysis")
