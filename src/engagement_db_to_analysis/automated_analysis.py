from core_data_modules.analysis import (engagement_counts, repeat_participations, theme_distributions, sample_messages,
                                        AnalysisConfiguration, traffic_analysis)
from core_data_modules.analysis.mapping import participation_maps, kenya_mapper, somalia_mapper
from core_data_modules.logging import Logger
from core_data_modules.util import IOUtils

from src.engagement_db_to_analysis.column_view_conversion import (analysis_dataset_configs_to_rqa_column_configs,
                                                                  analysis_dataset_configs_to_demog_column_configs)
from src.engagement_db_to_analysis.configuration import AnalysisLocations

log = Logger(__name__)


def run_automated_analysis(messages_by_column, participants_by_column, analysis_config, export_dir_path):
    """
    Runs automated analysis and exports the results to disk.

    :param messages_by_column: Messages traced data in column-view format.
    :type messages_by_column: iterable of core_data_modules.traced_data.TracedData
    :param participants_by_column: Participants traced data in column-view format.
    :type participants_by_column: iterable of core_data_modules.traced_data.TracedData
    :param analysis_config: Configuration for the export.
    :type analysis_config: src.engagement_db_to_analysis.configuration.AnalysisConfiguration
    :param export_dir_path: Directory to export the automated analysis files to.
    :type export_dir_path: str
    """
    log.info(f"Running automated analysis...")
    rqa_column_configs = analysis_dataset_configs_to_rqa_column_configs(analysis_config.dataset_configurations)
    demog_column_configs = analysis_dataset_configs_to_demog_column_configs(analysis_config.dataset_configurations)
    IOUtils.ensure_dirs_exist(export_dir_path)

    log.info(f"Exporting engagement counts.csv...")
    with open(f"{export_dir_path}/engagement_counts.csv", "w") as f:
        engagement_counts.export_engagement_counts_csv(
            messages_by_column, participants_by_column, "consent_withdrawn", rqa_column_configs, f
        )

    log.info("Exporting repeat participations...")
    with open(f"{export_dir_path}/repeat_participations.csv", "w") as f:
        repeat_participations.export_repeat_participations_csv(
            participants_by_column, "consent_withdrawn", rqa_column_configs, f
        )

    log.info("Exporting theme distributions...")
    with open(f"{export_dir_path}/theme_distributions.csv", "w") as f:
        theme_distributions.export_theme_distributions_csv(
            participants_by_column, "consent_withdrawn", rqa_column_configs, demog_column_configs, f
        )

    log.info("Exporting demographic distributions...")
    with open(f"{export_dir_path}/demographic_distributions.csv", "w") as f:
        theme_distributions.export_theme_distributions_csv(
            participants_by_column, "consent_withdrawn", demog_column_configs, [], f
        )

    log.info("Exporting up to 100 sample messages for each RQA code...")
    with open(f"{export_dir_path}/sample_messages.csv", "w") as f:
        sample_messages.export_sample_messages_csv(
            messages_by_column, "consent_withdrawn", rqa_column_configs, f, limit_per_code=100
        )

    if analysis_config.traffic_labels is not None:
        log.info("Exporting traffic analysis...")
        with open(f"{export_dir_path}/traffic_analysis.csv", "w") as f:
            traffic_analysis.export_traffic_analysis_csv(
                messages_by_column, "consent_withdrawn", rqa_column_configs, "timestamp",
                analysis_config.traffic_labels, f
            )
    else:
        log.debug("Not running any traffic analysis because analysis_configuration.traffic_labels is None")

    log.info(f"Exporting participation maps for each location dataset...")
    mappers = {
        AnalysisLocations.KENYA_COUNTY: kenya_mapper.export_kenya_counties_map,
        AnalysisLocations.KENYA_CONSTITUENCY: kenya_mapper.export_kenya_constituencies_map,

        AnalysisLocations.MOGADISHU_SUB_DISTRICT: somalia_mapper.export_mogadishu_sub_district_frequencies_map,
        AnalysisLocations.SOMALIA_DISTRICT: somalia_mapper.export_somalia_district_frequencies_map,
        AnalysisLocations.SOMALIA_REGION: somalia_mapper.export_somalia_region_frequencies_map
    }

    for analysis_dataset_config in analysis_config.dataset_configurations:
        for coding_config in analysis_dataset_config.coding_configs:
            if coding_config.analysis_location in mappers:
                location_column_config = AnalysisConfiguration(
                    dataset_name=coding_config.analysis_dataset,
                    raw_field=analysis_dataset_config.raw_dataset,
                    coded_field=f"{coding_config.analysis_dataset}_labels",
                    code_scheme=coding_config.code_scheme
                )

                participation_maps.export_participation_maps(
                    participants_by_column, "consent_withdrawn", rqa_column_configs, location_column_config,
                    mappers[coding_config.analysis_location],
                    f"{export_dir_path}/maps/{location_column_config.dataset_name}/{location_column_config.dataset_name}_"
                )
