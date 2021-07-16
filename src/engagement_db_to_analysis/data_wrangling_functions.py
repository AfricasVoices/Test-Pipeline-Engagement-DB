from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.cleaners import Codes
from core_data_modules.traced_data import Metadata
from core_data_modules.util import TimeUtils


def _impute_true_missing_labels(user, participants_traced_data_map, analysis_dataset_config):
    """

    Labels analysis_dataset for which there is no response as TRUE_MISSING.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param participants_traced_data_map: Participant TracedData objects map to check and impute TRUE_MISSING.
    :type participants_traced_data_map: dict of uuid -> participant TracedData objects.
    :param analysis_dataset_config: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_config: pipeline_config.analysis_dataset_config
    :return: Participant TracedData map.
    :rtype: dict of uuid -> participant TracedData objects.

    """

    true_missing_imputed_data = {}
    for uuid, participant_traced_data in participants_traced_data_map.items():

        for dataset_config in analysis_dataset_config:

            if dataset_config.analysis_dataset not in participants_traced_data_map:
                for coding_config in dataset_config.coding_configs:

                    true_missing_label = CleaningUtils.make_label_from_cleaner_code(coding_config.code_scheme,
                                                                                    coding_config.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                                                                                     Metadata.get_call_location()).to_dict()

                participant_traced_data.append_data({dataset_config.analysis_dataset: {"labels": true_missing_label}}, Metadata(user,
                                                                                                                                Metadata.get_call_location(),
                                                                                                                                TimeUtils.utc_now_as_iso_string()))

        true_missing_imputed_data[uuid] = participant_traced_data

    return true_missing_imputed_data


def _impute_age_categories():  # Todo
    pass


def _impute_kenyan_locations():  # Todo
    pass


def run_data_wrangling_functions(user, participants_traced_data_map, analysis_dataset_config):

    participants_traced_data_map = _impute_true_missing_labels(user, participants_traced_data_map, analysis_dataset_config)

    return participants_traced_data_map
