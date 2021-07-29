from core_data_modules.util import TimeUtils

from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger


log = Logger(__name__)


def _impute_not_reviewed(user, messages_traced_data, analysis_dataset_configs):

    """
    Imputes not reviewed label for messages that have not been manually labelled in coda.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute age_category.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_config: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_config: pipeline_config.analysis_configs.dataset_configurations
    """

    # Check and impute age_category in age messages only
    log.info(f"Imputing Not Reviewed label for messages that have not been manually labelled")
    updated_messages_traced_data = []
    for message in messages_traced_data:

        for analysis_dataset_config in analysis_dataset_configs:
            if message["dataset"] not in analysis_dataset_config.engagement_db_datasets:
                continue

            # Check if the message has a label and impute NR label otherwise
            if len(message["labels"]) == 0:
                code_scheme = analysis_dataset_config.coding_configs[0].code_scheme
                not_reviewed_label = CleaningUtils.make_label_from_cleaner_code(
                    code_scheme, code_scheme.get_code_with_control_code(Codes.NOT_REVIEWED),
                    Metadata.get_call_location()).to_dict()

                # Append this not_reviewed_label to the list of labels for this message, and write-back to TracedData.
                message_labels = message["labels"].copy()
                message_labels.append(not_reviewed_label)
                message.append_data(
                    {"labels": message_labels},
                    Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

        updated_messages_traced_data.append(message)

    return updated_messages_traced_data

def run_data_wrangling_functions(user, messages_traced_data, analysis_dataset_config):

    messages_traced_data = _impute_not_reviewed(user, messages_traced_data, analysis_dataset_config)

    return messages_traced_data
