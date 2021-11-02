import csv

from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataCSVIO
from core_data_modules.util import IOUtils

from src.engagement_db_to_analysis.column_view_conversion import analysis_dataset_configs_to_column_configs

log = Logger(__name__)


def export_production_file(traced_data_iterable, analysis_config, export_path):
    """
    Exports a column-view TracedData to a production file.

    The production file contains the participant uuid and all the raw_datasets only.

    :param traced_data_iterable: Data to export.
    :type traced_data_iterable: iterable of core_data_modules.traced_data.TracedData
    :param analysis_config: Configuration for the export.
    :type analysis_config: src.engagement_db_to_analysis.configuration.AnalysisConfiguration
    :param export_path: Path to export the file to.
    :type export_path: str
    """
    log.info(f"Exporting production file to '{export_path}'...")
    IOUtils.ensure_dirs_exist_for_file(export_path)
    with open(export_path, "w") as f:
        headers = ["participant_uuid", "timestamp"] + [c.raw_dataset for c in analysis_config.dataset_configurations]
        TracedDataCSVIO.export_traced_data_iterable_to_csv(traced_data_iterable, f, headers)



def _get_analysis_file_headers(pipeline_config, export_timestamps=False):
    """
    Gets the headers for an analysis file.

    The headers are:
     - "participant_uuid"
     - "consent_withdrawn"
     - Membership_group e.g "listening_group" - Optional
     - Labels for each normal code scheme, in matrix format e.g. "age:25", "s01e01:healthcare".
     - Raw messages for each dataset.
    
    :param pipeline_config: Pipeline configuration to derive the headers from.
    :type pipeline_config: PipelineConfiguration
    :return: Analysis file headers.
    :rtype: list of str

    """
    headers = ["participant_uuid", "consent_withdrawn"]
    if export_timestamps:
        headers.append("timestamp")

    if pipeline_config.analysis.membership_group_configuration is not None:
        for membership_group in pipeline_config.analysis.membership_group_configuration.membership_group_csv_urls.keys():
            headers.append(membership_group)
    
    column_configs = analysis_dataset_configs_to_column_configs(pipeline_config.analysis.dataset_configurations)
    for config in column_configs:
        # Add headers for each label in this column's code scheme, in matrix format e.g. "age:25", "s01e01:healthcare"
        for code in config.code_scheme.codes:
            headers.append(f"{config.dataset_name}:{code.string_value}")

        # Add the raw field to the headers.
        # If we've already seen this raw_field, move it to the end of the headers added so far so that the raw fields
        # always appear after their respective code schemes e.g. county labels, constituency labels, raw location.
        if config.raw_field in headers:
            headers.remove(config.raw_field)
        headers.append(config.raw_field)

    return headers


def _get_analysis_file_row(column_view_td, pipeline_config, export_timestamps=False):
    """
    Gets a row of an analysis file from a Traced Data object in column-view format

    :param column_view_td: Traced Data object to produce the row for.
    :type column_view_td: core_data_modules.traced_data.TracedData
    :param pipeline_config: Pipeline configuration.
    :type pipeline_config: PipelineConfiguration
    :return: Dictionary representing a row of an analysis file
    :rtype: dict
    """
    
    column_configs = analysis_dataset_configs_to_column_configs(pipeline_config.analysis.dataset_configurations)

    row = {
        "participant_uuid": column_view_td["participant_uuid"],
        "consent_withdrawn": column_view_td["consent_withdrawn"]
    }

    if pipeline_config.analysis.membership_group_configuration is not None:
        for membership_group in pipeline_config.analysis.membership_group_configuration.membership_group_csv_urls.keys():
            row[membership_group] = column_view_td[membership_group]
    
    if export_timestamps:
        row["timestamp"] = column_view_td["timestamp"]

    for config in column_configs:
        # Raw field
        row[config.raw_field] = column_view_td[config.raw_field]

        # Labels, in matrix config
        td_code_ids = [label["CodeID"] for label in column_view_td[config.coded_field]]
        for code in config.code_scheme.codes:
            if code.code_id in td_code_ids:
                row[f"{config.dataset_name}:{code.string_value}"] = Codes.MATRIX_1
            else:
                row[f"{config.dataset_name}:{code.string_value}"] = Codes.MATRIX_0

    return row


def export_analysis_file(traced_data_iterable, pipeline_config, export_path, export_timestamps=False):
    """
    Exports a column-view TracedData to a csv for analysis.

    This csv contains the participant uuids, raw responses, and assigned labels in matrix format.

    :param traced_data_iterable: Data to export.
    :type traced_data_iterable: iterable of core_data_modules.traced_data.TracedData
    :pipeline_config: pipeline configuration module
    :type PipelineConfiguration:
    :param export_path: Path to export the file to.
    :type export_path: str
    """
    log.info(f"Exporting analysis file to '{export_path}'...")

    IOUtils.ensure_dirs_exist_for_file(export_path)
    with open(export_path, "w") as f:
        headers = _get_analysis_file_headers(pipeline_config, export_timestamps)
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        for td in traced_data_iterable:
            row = _get_analysis_file_row(td, pipeline_config, export_timestamps)
            writer.writerow(row)
