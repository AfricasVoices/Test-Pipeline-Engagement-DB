from core_data_modules.analysis.analysis_utils import normal_codes
from core_data_modules.logging import Logger

from src.engagement_db_to_analysis.regression_analysis.data_conversion import \
    convert_participants_to_regression_data_frame


log = Logger(__name__)

GLM_FAMILY = 'binomial(link="logit")'


def _get_model_formula(theme, predictors):
    """
    Gets the model formula string for a given theme and the names of its predictor variables.
    
    :param theme: Theme in model formula e.g. "s01e01_yes"
    :type theme: str
    :param predictors: List of predictor variables e.g. ["age", "gender", ...]
    :type predictors: list of str
    """
    return f"{theme} ~ {' + '.join(predictors)}"


def run_complete_case_regression_analysis(participants, consent_withdrawn_field, rqa_analysis_config,
                                          demog_analysis_configs):
    """
    Runs complete-case, multivariate regression analysis on one RQA configuration against multiple demographics.

    The regression is run separately on each of the normal themes in the RQA code scheme, each against the normal codes
    in the provided demographics.

    :param participants: Participants to analyse.
    :type participants: iterable of core_data_modules.traced_data.TracedData
    :param consent_withdrawn_field: Field in each participants object which records if consent is withdrawn.
    :type consent_withdrawn_field: str
    :param rqa_analysis_config: Configuration for the RQA dataset to run the regression on.
    :type rqa_analysis_config: core_data_modules.analysis.AnalysisConfiguration
    :param demog_analysis_configs: Configuration for the demographic datasets to run the regression on.
                                   TODO: The actual demographics are currently a hard-coded subset of what is provided
                                         here. Derive automatically or from configuration in future.
    :type demog_analysis_configs: list of core_data_modules.analysis.AnalysisConfiguration
    :return: Dictionary of theme -> regression results table, formatted as a string.
    :rtype: dict of str -> str
            TODO: Return the regression results table as an object that can be inspected and formatted rather than a str
    """
    from rpy2.interactive.packages import importr
    from rpy2.robjects import r

    # Initialise R
    base = importr("base")
    arm = importr("arm")  # Library for 'Data Analysis Using Regression and Multilevel/Hierarchical Models'

    data_frame = convert_participants_to_regression_data_frame(
        participants, consent_withdrawn_field, rqa_analysis_config, demog_analysis_configs
    )

    # TODO: Derive these predictors automatically or from configuration rather than from a hard-coded list.
    predictors = ["gender", "age_category", "disability", "recently_displaced"]

    results = dict()
    for code in normal_codes(rqa_analysis_config.code_scheme.codes):
        theme = f"{rqa_analysis_config.dataset_name}_{code.string_value}"
        formula = _get_model_formula(theme, predictors)

        log.info(f"Running complete case regression '{formula}'...")
        regression_results = arm.bayesglm(formula, family=r(GLM_FAMILY), data=data_frame)

        summarised_results = base.summary(regression_results)
        coefficients = summarised_results.rx2("coefficients")
        results_table = str(coefficients)
        results[theme] = results_table

    return results


def run_all_complete_case_regression_analysis(participants, consent_withdrawn_field, rqa_analysis_configs, demog_analysis_configs):
    """
    Runs all the complete case regression analysis for multiple RQA and demographic configurations.

    This function calls `run_complete_case_regression_analysis` once for each of the given `rqa_analysis_configs`.

    :param participants: Participants to analyse.
    :type participants: iterable of core_data_modules.traced_data.TracedData
    :param consent_withdrawn_field: Field in each participants object which records if consent is withdrawn.
    :type consent_withdrawn_field: str
    :param rqa_analysis_configs: Configurations for the RQA datasets to run the regression on.
                                 Each RQA will be analysed independently.
    :type rqa_analysis_configs: list of core_data_modules.analysis.AnalysisConfiguration
    :param demog_analysis_configs: Configuration for the demographic datasets to run the regression on.
                                   TODO: The actual demographics are currently a hard-coded subset of what is provided
                                         here. Derive automatically or from configuration in future.
    :type demog_analysis_configs: list of core_data_modules.analysis.AnalysisConfiguration
    :return: Dictionary of dataset_name -> (dict of theme -> results table as text)
    :rtype dict of str -> (dict of str -> str)
    """
    all_results = dict()  # of dataset_name -> (dict of theme -> results table as text)
    for rqa_config in rqa_analysis_configs:
        rqa_results = run_complete_case_regression_analysis(
            participants, consent_withdrawn_field, rqa_config, demog_analysis_configs
        )
        all_results[rqa_config.dataset_name] = rqa_results

    return all_results


def export_all_complete_case_regression_analysis_txt(participants, consent_withdrawn_field, rqa_analysis_configs,
                                                     demog_analysis_configs, f):
    """
    Computes all the complete-case regression analysis and exports them to a text file.

    :param participants: Participants to analyse.
    :type participants: iterable of core_data_modules.traced_data.TracedData
    :param consent_withdrawn_field: Field in each participants object which records if consent is withdrawn.
    :type consent_withdrawn_field: str
    :param rqa_analysis_configs: Configurations for the RQA datasets to run the regression on.
                                 Each RQA will be analysed independently.
    :type rqa_analysis_configs: list of core_data_modules.analysis.AnalysisConfiguration
    :param demog_analysis_configs: Configuration for the demographic datasets to run the regression on.
                                   TODO: The actual demographics are currently a hard-coded subset of what is provided
                                         here. Derive automatically or from configuration in future.
    :type demog_analysis_configs: list of core_data_modules.analysis.AnalysisConfiguration
    :param f: Text file to write the regression results to.
    :type f: file-like
    """
    regression_results = run_all_complete_case_regression_analysis(
        participants, consent_withdrawn_field, rqa_analysis_configs, demog_analysis_configs
    )

    for results in regression_results.values():
        for theme, result_text in results.items():
            f.write(theme + "\n")
            f.write(result_text + "\n")
