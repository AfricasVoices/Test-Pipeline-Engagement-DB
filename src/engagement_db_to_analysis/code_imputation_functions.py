from core_data_modules.cleaners import Codes
from core_data_modules.util import TimeUtils
from core_data_modules.data_models.code_scheme import CodeTypes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.traced_data import Metadata
from core_data_modules.logging import Logger
from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.location_tools import KenyaLocations


from engagement_database.data_models import Message

from src.engagement_db_to_analysis.column_view_conversion import (get_latest_labels_with_code_scheme,
                                                                  analysis_dataset_config_for_message)

from src.engagement_db_to_analysis.column_view_conversion import (analysis_dataset_config_to_column_configs,
                                                                  analysis_dataset_configs_to_column_configs)
from src.pipeline_configuration_spec import *

log = Logger(__name__)

def _insert_label_to_messsage_td(user, message_traced_data, label):
    """
    Inserts a new label to the list of labels for this message, and writes-back to TracedData.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute age_category.
    :type messages_traced_data: list of TracedData
    :param label: New core_data_modules.data_models.Label to insert to the message_traced_data
    :type: core_data_modules.data_models.Label
    """
    label = label.to_dict()
    message_labels = message_traced_data["labels"].copy()
    message_labels.insert(0, label)
    message_traced_data.append_data(
        {"labels": message_labels},
        Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

def _impute_not_reviewed_labels(user, messages_traced_data, analysis_dataset_configs):
    """
    Imputes Codes.NOT_REVIEWED label for messages that have not been manually checked in coda.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute not reviewed labels.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_configs: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """

    log.info(f"Imputing {Codes.NOT_REVIEWED} labels...")
    imputed_labels = 0
    for message_td in messages_traced_data:
        message = Message.from_dict(dict(message_td))

        message_analysis_config = analysis_dataset_config_for_message(analysis_dataset_configs, message)

        # Check if the message has a manual label and impute NOT_REVIEWED if it doesn't
        manually_labelled = False
        for coding_config in message_analysis_config.coding_configs:
            latest_labels_with_code_scheme = get_latest_labels_with_code_scheme(message,
                                                                                coding_config.code_scheme)
            for label in latest_labels_with_code_scheme:
                if label.checked:
                    manually_labelled = True

        if manually_labelled:
            continue

        code_scheme = message_analysis_config.coding_configs[0].code_scheme
        not_reviewed_label = CleaningUtils.make_label_from_cleaner_code(
            code_scheme, code_scheme.get_code_with_control_code(Codes.NOT_REVIEWED),
            Metadata.get_call_location())

        # Insert not_reviewed_label to the list of labels for this message, and write-back to TracedData.
        _insert_label_to_messsage_td(user, message_td, not_reviewed_label)

        imputed_labels += 1

    log.info(f"Imputed {imputed_labels} {Codes.NOT_REVIEWED} labels for {len(messages_traced_data)} "
             f"messages traced data")


def _impute_age_category(user, messages_traced_data, analysis_dataset_configs):
    """
    Imputes age category for age dataset messages.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute age_category.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_configs: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """

    # Get the coding configurations for age and age_category analysis datasets
    age_category_coding_config = None
    for analysis_dataset_config in analysis_dataset_configs:
        for coding_config in analysis_dataset_config.coding_configs:
            if coding_config.age_category_config is None:
                log.info(f"No age_category config in {coding_config.analysis_dataset} skipping...")
                continue

            log.info(f"Found age_category in {coding_config.analysis_dataset} coding config")
            assert age_category_coding_config is None, f"Found more than one age_category configs"
            age_category_coding_config = coding_config

    if age_category_coding_config is None:
        log.info(f"No age category configuration found, returning without imputing any age categories")
        return

    age_coding_config = None
    age_engagement_db_datasets = None
    for analysis_dataset_config in analysis_dataset_configs:
        for coding_config in analysis_dataset_config.coding_configs:
            if coding_config.analysis_dataset == age_category_coding_config.age_category_config.age_analysis_dataset:

                assert age_coding_config is None, f"Found more than one age_coding_config in analysis_dataset_config"
                age_coding_config = coding_config
                age_engagement_db_datasets = analysis_dataset_config.engagement_db_datasets

    # Check and impute age_category in age messages only
    log.info(f"Imputing {age_category_coding_config.analysis_dataset} labels for {age_coding_config.analysis_dataset} messages...")
    imputed_labels = 0
    age_messages = 0
    for message_td in messages_traced_data:
        if message_td["dataset"] in age_engagement_db_datasets:
            age_messages += 1

            age_labels = get_latest_labels_with_code_scheme(Message.from_dict(dict(message_td)), age_coding_config.code_scheme)
            age_code = age_coding_config.code_scheme.get_code_with_code_id(age_labels[0].code_id)

            # Impute age_category for this age_code
            if age_code.code_type == CodeTypes.NORMAL:
                age_category = None
                for age_range, category in age_category_coding_config.age_category_config.categories.items():
                    if age_range[0] <= age_code.numeric_value <= age_range[1]:
                        age_category = category
                assert age_category is not None
                age_category_code = age_category_coding_config.code_scheme.get_code_with_match_value(age_category)
            elif age_code.code_type == CodeTypes.META:
                age_category_code = age_category_coding_config.code_scheme.get_code_with_meta_code(age_code.meta_code)
            else:
                assert age_code.code_type == CodeTypes.CONTROL
                age_category_code = age_category_coding_config.code_scheme.get_code_with_control_code(
                    age_code.control_code)

            age_category_label = CleaningUtils.make_label_from_cleaner_code(
                age_category_coding_config.code_scheme, age_category_code, Metadata.get_call_location()
            )

            # Inserts this age_category_label to the list of labels for this message, and write-back to TracedData.
            _insert_label_to_messsage_td(user, message_td, age_category_label)

            imputed_labels +=1

    log.info(f"Imputed {imputed_labels} age category labels for {age_messages} age messages")


def _make_location_code(scheme, clean_value):
    if clean_value == Codes.NOT_CODED:
        return scheme.get_code_with_control_code(Codes.NOT_CODED)
    else:
        return scheme.get_code_with_match_value(clean_value)


def _impute_kenya_location_codes(user, messages_traced_data, analysis_dataset_configs):
    """
    Imputes Kenya location labels for location dataset messages.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute age_category.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_configs: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """

    log.info(f"Imputing Kenya location labels for location messages...")

    # Get the coding configurations for constituency and county analysis datasets
    constituency_coding_config = None
    county_coding_config = None
    location_engagement_db_datasets = None
    for analysis_dataset_config in analysis_dataset_configs:
        for coding_config in analysis_dataset_config.coding_configs:
            if coding_config.kenya_analysis_location is None:
                log.info(f"No kenya_location config in {coding_config.analysis_dataset} skipping...")

            elif coding_config.kenya_analysis_location == KenyaAnalysisLocations.CONSTITUENCY:
                log.info(f"Found kenya_analysis_location in {coding_config.analysis_dataset} coding config")

                assert constituency_coding_config is None, f"Found more than one constituency_coding_config in " \
                    f"analysis_dataset_config"
                constituency_coding_config = coding_config
                location_engagement_db_datasets = analysis_dataset_config.engagement_db_datasets

            elif coding_config.kenya_analysis_location == KenyaAnalysisLocations.COUNTY:
                log.info(f"Found kenya_analysis_location in {coding_config.analysis_dataset} coding config")

                assert county_coding_config is None, f"Found more than one county_coding_config in " \
                    f"analysis_dataset_config"
                county_coding_config = coding_config

    assert county_coding_config is not None, f"No county_coding_config in " \
        f"analysis_dataset_config"
    assert constituency_coding_config is not None, f"No constituency_coding_config in " \
        f"analysis_dataset_config"

    for message_traced_data in messages_traced_data:
        if message_traced_data["dataset"] in location_engagement_db_datasets:
            message = Message.from_dict(dict(message_traced_data))
            message_analysis_config = analysis_dataset_config_for_message(analysis_dataset_configs, message)

            # Up to 1 location code should have been assigned in Coda. Search for that code,
            # ensuring that only 1 has been assigned or, if multiple have been assigned, that they are non-conflicting
            # control codes
            location_code = None
            for coding_config in message_analysis_config.coding_configs:
                latest_coding_config_labels = get_latest_labels_with_code_scheme(message, coding_config.code_scheme)

                if len(latest_coding_config_labels) > 0:
                    latest_coding_config_label = latest_coding_config_labels[0]
                    if latest_coding_config_label.checked:
                        coda_code = coding_config.code_scheme.get_code_with_code_id(latest_coding_config_label.code_id)
                        if location_code is not None:
                            if location_code.code_id != coda_code.code_id:
                                location_code = constituency_coding_config.code_scheme.get_code_with_control_code(Codes.CODING_ERROR)
                        else:
                            location_code = coda_code

            # If no code was found, then this location is still not reviewed.
            # Synthesise a NOT_REVIEWED code accordingly.
            if location_code is None:
                location_code = constituency_coding_config.code_scheme.get_code_with_control_code(Codes.NOT_REVIEWED)

            # If a control or meta code was found, set all other location keys to that control/meta code,
            # otherwise convert the provided location to the other locations in the hierarchy.
            if location_code.code_type == CodeTypes.CONTROL:
                for coding_config in message_analysis_config.coding_configs:

                    control_code_label = CleaningUtils.make_label_from_cleaner_code(
                            coding_config.code_scheme, coding_config.code_scheme.get_code_with_control_code(location_code.control_code),
                            Metadata.get_call_location())

                    _insert_label_to_messsage_td(user, message_traced_data, control_code_label)

            elif location_code.code_type == CodeTypes.META:
                for coding_config in message_analysis_config.coding_configs:

                    meta_code_label = CleaningUtils.make_label_from_cleaner_code(
                        coding_config.code_scheme, coding_config.code_scheme.get_code_with_control_code(location_code.meta_code),
                        Metadata.get_call_location())

                    _insert_label_to_messsage_td(user, message_traced_data, meta_code_label)

            else:
                location = location_code.match_values[0]
                constituency_label = CleaningUtils.make_label_from_cleaner_code(constituency_coding_config.code_scheme,
                        _make_location_code(constituency_coding_config.code_scheme,
                                           KenyaLocations.constituency_for_location_code(location)),
                                                 Metadata.get_call_location())

                county_label = CleaningUtils.make_label_from_cleaner_code(county_coding_config.code_scheme,
                        _make_location_code(county_coding_config.code_scheme,
                                           KenyaLocations.county_for_location_code(location)),
                                                Metadata.get_call_location())

                _insert_label_to_messsage_td(user, message_traced_data, constituency_label)
                _insert_label_to_messsage_td(user, message_traced_data, county_label)

def impute_codes_by_message(user, messages_traced_data, analysis_dataset_configs):
    """
    Imputes codes for messages TracedData in-place.

    Runs the following imputations:
     - Imputes Codes.NOT_REVIEWED for messages that have not been manually labelled in coda.
     - Imputes Age category labels for age dataset messages.
     - Imputes Kenya Location labels for location dataset messages.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param messages_traced_data: Messages TracedData objects to impute age_category.
    :type messages_traced_data: list of TracedData
    :param analysis_dataset_configs: Analysis dataset configuration in pipeline configuration module.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """

    _impute_not_reviewed_labels(user, messages_traced_data, analysis_dataset_configs)

    _impute_age_category(user, messages_traced_data, analysis_dataset_configs)

    _impute_kenya_location_codes(user, messages_traced_data, analysis_dataset_configs)


def _impute_true_missing(user, column_traced_data_iterable, analysis_dataset_configs):
    """
    Imputes TRUE_MISSING codes on column-view datasets.

    TRUE_MISSING labels are applied to analysis datasets where the raw dataset doesn't exist in the given TracedData.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param column_traced_data_iterable: Column-view traced data objects to apply the impute function to.
    :type column_traced_data_iterable: iterable of core_data_modules.traced_data.TracedData
    :param analysis_dataset_configs: Analysis dataset configurations for the imputation.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """
    imputed_codes = 0
    log.info(f"Imputing {Codes.TRUE_MISSING} codes...")

    column_configs = analysis_dataset_configs_to_column_configs(analysis_dataset_configs)

    for td in column_traced_data_iterable:
        na_dict = dict()

        for column_config in column_configs:
            if column_config.raw_field in td:
                continue

            na_dict[column_config.raw_field] = ""
            na_label = CleaningUtils.make_label_from_cleaner_code(
                column_config.code_scheme,
                column_config.code_scheme.get_code_with_control_code(Codes.TRUE_MISSING),
                Metadata.get_call_location()
            ).to_dict()
            na_dict[column_config.coded_field] = [na_label]
            imputed_codes += 1

        td.append_data(na_dict, Metadata(user, Metadata.get_call_location(), TimeUtils.utc_now_as_iso_string()))

    log.info(f"Imputed {imputed_codes} {Codes.TRUE_MISSING} codes for {len(column_traced_data_iterable)} "
             f"traced data items")


def impute_codes_by_column_traced_data(user, column_traced_data_iterable, analysis_dataset_configs):
    """
    Imputes codes for column-view TracedData in-place.

    Runs the following imputations:
     - Imputes Codes.TRUE_MISSING to columns that don't have a raw_field entry.

    :param user: Identifier of user running the pipeline.
    :type user: str
    :param column_traced_data_iterable: Column-view traced data objects to apply the impute function to.
    :type column_traced_data_iterable: iterable of core_data_modules.traced_data.TracedData
    :param analysis_dataset_configs: Analysis dataset configurations for the imputation.
    :type analysis_dataset_configs: pipeline_config.analysis_configs.dataset_configurations
    """
    _impute_true_missing(user, column_traced_data_iterable, analysis_dataset_configs)
