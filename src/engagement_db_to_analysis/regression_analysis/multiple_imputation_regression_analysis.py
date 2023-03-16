from core_data_modules.analysis.cross_tabs import _normal_codes
from core_data_modules.logging import Logger
from rpy2 import robjects
from rpy2.interactive.packages import importr
from rpy2.robjects import r

from src.engagement_db_to_analysis.regression_analysis.data_conversion import \
    convert_participants_to_regression_data_frame


log = Logger(__name__)

GLM_FAMILY = 'binomial(link="logit")'


def run_multiple_imputation_regression_analysis(participants, consent_withdrawn_field, rqa_analysis_config,
                                                demog_analysis_configs):
    """
    Runs multiple-imputation regression analysis on one RQA configuration against multiple demographics.

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
    # If this RQA has no normal themes, exit early, so we don't spend time creating and imputing datasets unnecessarily.
    if len(_normal_codes(rqa_analysis_config.code_scheme.codes)) == 0:
        return dict()

    # Initialise R
    base = importr("base")  # R standard library
    arm = importr("arm")  # Library for 'Data Analysis Using Regression and Multilevel/Hierarchical Models'
    mice = importr("mice")  # Library for 'Multivariate Imputation by Chained Equations'
    env = robjects.globalenv

    # TODO: Derive these variables automatically or from configuration rather than from a hard-coded string.
    demographic_datasets = {"gender", "disability", "recently_displaced", "age_category"}

    data_frame = convert_participants_to_regression_data_frame(
        participants, consent_withdrawn_field, rqa_analysis_config,
        [d for d in demog_analysis_configs if d.dataset_name in demographic_datasets]
    )

    # Generate 20 copies of the input dataset, where each copy has had the missing data filled in with a different set
    # of plausible values.
    # Reset R's random number generator seed to ensure we get reproducible results.
    log.info(f"Running multiple imputation for dataset '{rqa_analysis_config.dataset_name}...")
    base.set_seed(123)
    multiple_imputed_data_frame = mice.mice(data_frame, m=20, printFlag=False)
    env["multiple_imputed_data_frame"] = multiple_imputed_data_frame
    env["glm_family"] = r(GLM_FAMILY)

    demogs_formula = " + ".join(demographic_datasets)
    results = dict()
    for code in _normal_codes(rqa_analysis_config.code_scheme.codes):
        theme = f"{rqa_analysis_config.dataset_name}_{code.string_value}"
        formula = f"{theme} ~ {demogs_formula}"
        log.info(f"Running multiple imputation regression for '{formula}'...")

        # Run the regression analysis independently on each imputed dataset
        env["multiple_regression_results"] = r(
            f"with(multiple_imputed_data_frame, bayesglm({formula}, family=glm_family))"
        )

        # Pool the results from each independent regression, to give a final estimate of the regression
        # coefficients and confidence intervals.
        env["pooled_results"] = r("pool(multiple_regression_results)")
        summarised_results = r("summary(pooled_results, conf.int=TRUE, conf.level=0.95)")
        results[theme] = str(summarised_results)

    return results


def run_all_multiple_imputation_regression_analysis(participants, consent_withdrawn_field, rqa_analysis_configs, demog_analysis_configs):
    """
    Runs all the multiple imputation regression analysis for multiple RQA and demographic configurations.

    This function calls `run_multiple_imputation_regression_analysis` once for each of the given `rqa_analysis_configs`.

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
        rqa_results = run_multiple_imputation_regression_analysis(
            participants, consent_withdrawn_field, rqa_config, demog_analysis_configs
        )
        all_results[rqa_config.dataset_name] = rqa_results

    return all_results


def export_all_multiple_imputation_regression_analysis_txt(participants, consent_withdrawn_field, rqa_analysis_configs,
                                                           demog_analysis_configs, f):
    """
    Computes all the multiple imputation regression analysis and exports the results to a text file.

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
    regression_results = run_all_multiple_imputation_regression_analysis(
        participants, consent_withdrawn_field, rqa_analysis_configs, demog_analysis_configs
    )

    for results in regression_results.values():
        for (theme, result_text) in results.items():
            f.write(theme + "\n")
            f.write(result_text + "\n")
