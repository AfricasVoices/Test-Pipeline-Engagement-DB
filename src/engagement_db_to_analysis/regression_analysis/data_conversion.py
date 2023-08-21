# TODO: Move this file to CoreDataModules once stable.

from core_data_modules.analysis import analysis_utils
from core_data_modules.analysis.analysis_utils import get_codes_from_td, normal_codes
from core_data_modules.cleaners import Codes

from src.engagement_db_to_analysis.regression_analysis.r_utils import convert_dicts_to_r_data_frame_of_factors


def _get_matrix_values(codes, dataset_name, code_scheme):
    """
    Gets the normal matrix values for a list of codes.
    """
    matrix_values = dict()  # of str -> str

    code_ids = {code.code_id for code in codes}
    for code in normal_codes(code_scheme.codes):
        if code.code_id in code_ids:
            value = 1  # Use 1 here instead of Codes.MATRIX_1 because the type needs to be int, not str.
        else:
            value = 0

        matrix_values[f"{dataset_name}_{code.string_value}"] = value

    return matrix_values


def _get_categorical_value(codes):
    """
    Gets the single, normal categorical value from a list of codes. If there is no normal value, returns None.
    """
    all_normal_codes = normal_codes(codes)

    assert len(all_normal_codes) <= 1, len(all_normal_codes)

    if len(all_normal_codes) == 0:
        return None

    return all_normal_codes[0].string_value


def _get_participant_regression_data(participant, consent_withdrawn_field, rqa_analysis_config, demog_analysis_configs):
    """
    Gets the relevant data needed for regression analysis from a single participant.

    The returned data contains:
     - The participant_uuid
     - The normal RQA labels, in matrix-format.
     - The normal demographic labels, in categorical-format.

    :param participant: Participant to get the relevant regression data from.
    :type participant: core_data_modules.traced_data.TracedData
    :param consent_withdrawn_field: Field in each participants object which records if consent is withdrawn.
    :type consent_withdrawn_field: str
    :param rqa_analysis_config: Configuration for the RQA dataset to include in the returned data-frame.
    :type rqa_analysis_config: core_data_modules.analysis.AnalysisConfiguration
    :param demog_analysis_configs: Configuration for the demographic datasets to include in the returned data-frame.
    :type demog_analysis_configs: list of core_data_modules.analysis.AnalysisConfiguration
    """
    # Ensure participant has not opted-out
    assert participant[consent_withdrawn_field] == Codes.FALSE

    # Extract the regression data into a dictionary.
    # This will contain the relevant RQA labels in matrix-format, and the relevant demog labels in categorical-format.
    regression_data = {
       "participant_uuid": participant["participant_uuid"]
    }

    # Extract the relevant RQA labels in matrix-format.
    rqa_codes = get_codes_from_td(participant, rqa_analysis_config)
    regression_data.update(
        _get_matrix_values(rqa_codes, rqa_analysis_config.dataset_name, rqa_analysis_config.code_scheme)
    )

    # Extract the relevant demographic labels in categorical-format.
    for demog_config in demog_analysis_configs:
        demog_codes = get_codes_from_td(participant, demog_config)
        regression_data[demog_config.dataset_name] = _get_categorical_value(demog_codes)

    return regression_data


def convert_participants_to_regression_data_frame(participants, consent_withdrawn_field,
                                                  rqa_analysis_config, demog_analysis_configs):
    """
    Converts a list of participants into an R data-frame.

    This data-frame will contain the following columns:
     - "participant_uuid".
     - All the normal codes in the rqa configuration, in matrix_format.
     - All the normal codes in the demog configurations, in categorical-format.

     For example, the returned data-frame might look like:
     participant_uuid | s01e01_yes | s01e01_no | gender | age
     id-1             |          1 |         0 | woman  | 30
     ...

    :param participants: Participants to convert to a data-frame suitable for regression_analysis.
    :type participants: iterable of core_data_modules.traced_data.TracedData
    :param consent_withdrawn_field: Field in each participants object which records if consent is withdrawn.
    :type consent_withdrawn_field: str
    :param rqa_analysis_config: Configuration for the RQA dataset to include in the returned data-frame.
    :type rqa_analysis_config: core_data_modules.analysis.AnalysisConfiguration
    :param demog_analysis_configs: Configuration for the demographic datasets to include in the returned data-frame.
    :type demog_analysis_configs: list of core_data_modules.analysis.AnalysisConfiguration
    :return: R data-frame that can be used to run regression analysis.
    :rtype: rpy2.robjects.DataFrame
    """
    responded_participants = analysis_utils.filter_relevant(participants, consent_withdrawn_field, [rqa_analysis_config])

    # Extract the data needed by the regression from the list of participants.
    regression_dicts = []  # of dict of str -> str, where each dict contains the data for one participant.
    for participant in responded_participants:
        regression_dicts.append(
            _get_participant_regression_data(participant, consent_withdrawn_field, rqa_analysis_config, demog_analysis_configs)
        )

    # Convert the regression data into an R data frame.
    return convert_dicts_to_r_data_frame_of_factors(regression_dicts)
