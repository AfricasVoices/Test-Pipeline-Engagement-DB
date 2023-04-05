from rpy2.interactive.packages import importr
from rpy2.robjects import r

from src.engagement_db_to_analysis.regression_analysis.data_conversion import \
    convert_participants_to_regression_data_frame


GLM_FAMILY = 'binomial(link="logit")'


def _run_regression_analysis(participants, consent_withdrawn_field, rqa_analysis_config, demog_analysis_configs):
    # Initialise R
    arm = importr("arm")  # Library for 'Data Analysis Using Regression and Multilevel/Hierarchical Models'

    data_frame = convert_participants_to_regression_data_frame(
        participants, consent_withdrawn_field, rqa_analysis_config, demog_analysis_configs
    )

    # TODO: Derive this formula automatically or from configuration rather than from a hard-coded string.
    formula = f"s03e01_unity_and_cooperation ~ gender + age_category + disability + recently_displaced"

    regression_results = arm.bayesglm(formula, family=r(GLM_FAMILY), data=data_frame)
    base = importr("base")
    print(base.summary(regression_results))
    exit(0)


def run_all_regression_analysis(participants, consent_withdrawn_field, rqa_analysis_configs, demog_analysis_configs):
    # For now, just run on the first RQA configuration.
    # TODO: Run on all configurations.
    _run_regression_analysis(participants, consent_withdrawn_field, rqa_analysis_configs[0], demog_analysis_configs)
