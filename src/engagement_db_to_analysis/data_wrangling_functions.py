from core_data_modules.util import TimeUtils

from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.logging import Logger


log = Logger(__name__)

def _impute_age_category(user, messages_traced_data, analysis_dataset_configs):
    """
    Imputes age category for age dataset messages.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute age_category.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_config: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_config: pipeline_config.analysis_configs.dataset_configurations
    """

    log.info(f"Imputing age category for age dataset messages...")

    # Get the configurations for age and age_category analysis datasets
    updated_messages_traced_data = []
    age_cc = None
    age_category_cc = None
    age_engagement_db_datasets = None
    for analysis_dataset_config in analysis_dataset_configs:
        for coding_config in analysis_dataset_config.coding_configs:
            if coding_config.analysis_dataset == "age":
                age_cc = coding_config
                age_engagement_db_datasets = analysis_dataset_config.engagement_db_datasets
            elif coding_config.analysis_dataset == "age_category":
                age_category_cc = coding_config

    #Check and impute age_category in age messages only
    for message in messages_traced_data:
        if message["dataset"] not in age_engagement_db_datasets:
            updated_messages_traced_data.append(message)
            continue

        age_label = message["labels"][0]
        age_code = age_cc.code_scheme.get_code_with_code_id(age_label["CodeID"])

        # Impute age_category for this age_code
        if age_code.code_type == CodeTypes.NORMAL:
            age_category = None
            for age_range, category in age_category_cc.age_categories.items():
                if age_range[0] <= age_code.numeric_value <= age_range[1]:
                    age_category = category
            assert age_category is not None
            age_category_code = age_category_cc.code_scheme.get_code_with_match_value(age_category)
        elif age_code.code_type == CodeTypes.META:
            age_category_code = age_category_cc.code_scheme.get_code_with_meta_code(age_code.meta_code)
        else:
            assert age_code.code_type == CodeTypes.CONTROL
            age_category_code = age_category_cc.code_scheme.get_code_with_control_code(
                age_code.control_code)

        age_category_label = CleaningUtils.make_label_from_cleaner_code(
            age_category_cc.code_scheme, age_category_code, Metadata.get_call_location()
        )

        # Append this age_category_label to the list of labels for this message, and write-back to TracedData.
        message_labels = message["labels"].copy()
        message_labels.append(age_category_label.to_dict())
        message.append_data(
            {"labels": message_labels},
            Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string())
        )

        updated_messages_traced_data.append(message)

        return updated_messages_traced_data

def run_data_wrangling_functions(user, messages_traced_data, analysis_dataset_config):

    messages_traced_data = _impute_age_category(user, messages_traced_data, analysis_dataset_config)

    return messages_traced_data
