from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.data_models import Message as CodaMessage, Label, Origin
from core_data_modules.logging import Logger
from core_data_modules.traced_data import Metadata
from core_data_modules.util import TimeUtils
from engagement_database.data_models import HistoryEntryOrigin

from src.engagement_db_coda_sync.sync_stats import CodaSyncEvents, EngagementDBToCodaSyncStats

log = Logger(__name__)


def _add_message_to_coda(coda, coda_dataset_config, ws_correct_dataset_code_scheme, engagement_db_message):
    """
    Adds a message to Coda.

    If this message already has labels, copies these through to Coda.
    Otherwise, if an auto-coder is specified, initialises with those initial labels.
    Otherwise, adds the message with no initial labels.

    :param coda: Coda instance to add the message to.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param coda_dataset_config: Configuration for adding the message.
    :type coda_dataset_config: src.engagement_db_coda_sync.configuration.CodaDatasetConfiguration
    :param ws_correct_dataset_code_scheme: WS Correct Dataset code scheme for the Coda dataset, used to validate any
                                           existing labels, where applicable.
    :type ws_correct_dataset_code_scheme: core_data_modules.data_models.CodeScheme
    :param engagement_db_message: Message to add to Coda.
    :type engagement_db_message: engagement_database.data_models.Message
    """
    log.debug("Adding message to Coda")

    coda_message = CodaMessage(
        message_id=engagement_db_message.coda_id,
        text=engagement_db_message.text,
        creation_date_time_utc=TimeUtils.datetime_to_utc_iso_string(engagement_db_message.timestamp),
        labels=[]
    )

    # If the engagement database message already has labels, initialise with these in Coda.
    if len(engagement_db_message.labels) > 0:
        # Ensure the existing labels are valid under the code schemes being copied to, by checking the label's scheme id
        # exists in this dataset's code schemes or the ws correct dataset scheme, and that the code id is in the
        # code scheme.
        valid_code_schemes = [c.code_scheme for c in coda_dataset_config.code_scheme_configurations]
        valid_code_schemes.append(ws_correct_dataset_code_scheme)
        valid_code_schemes_lut = {code_scheme.scheme_id: code_scheme for code_scheme in valid_code_schemes}
        for label in engagement_db_message.labels:
            assert label.scheme_id in valid_code_schemes_lut.keys(), \
                f"Scheme id {label.scheme_id} not valid for Coda dataset {coda_dataset_config.coda_dataset_id}"
            code_scheme = valid_code_schemes_lut[label.scheme_id]
            valid_codes = code_scheme.codes
            valid_code_ids = [code.code_id for code in valid_codes]
            assert label.code_id == "SPECIAL-MANUALLY_UNCODED" or label.code_id in valid_code_ids, \
                f"Code ID {label.code_id} not found in Scheme {code_scheme.name} (id {label.scheme_id})"

        coda_message.labels = engagement_db_message.labels

    # Otherwise, run any auto-coders that are specified.
    else:
        for scheme_config in coda_dataset_config.code_scheme_configurations:
            if scheme_config.auto_coder is None:
                continue
            label = CleaningUtils.apply_cleaner_to_text(scheme_config.auto_coder, engagement_db_message.text,
                                                        scheme_config.code_scheme)
            if label is not None:
                coda_message.labels.append(label)

    # Add the message to the Coda dataset.
    coda.add_message_to_dataset(coda_dataset_config.coda_dataset_id, coda_message)


def _code_for_label(label, code_schemes):
    """
    Returns the code for the given label.

    Handles duplicated scheme ids (i.e. schemes ending in '-1', '-2' etc.).
    Raises a ValueError if the label isn't for any of the given code schemes.

    :param label: Label to get the code for.
    :type label: core_data_modules.data_models.Label
    :param code_schemes: Code schemes to check for the given label.
    :type code_schemes: list of core_data_modules.data_models.CodeScheme
    :return: Code for the label.
    :rtype: core_data_modules.data_models.Code
    """
    for code_scheme in code_schemes:
        if label.scheme_id.startswith(code_scheme.scheme_id):
            return code_scheme.get_code_with_code_id(label.code_id)

    raise ValueError(f"Label's scheme id '{label.scheme_id}' is not in any of the given `code_schemes` "
                     f"(these have ids {[scheme.scheme_id for scheme in code_schemes]})")


def _impute_coding_error(coda_message, coda_dataset_config, ws_correct_dataset_code_scheme):
    """

    :param coda_message: Coda message to use to update the engagement database message.
    :type coda_message: core_data_modules.data_models.Message
    :param coda_config: Configuration for the update.
    :type coda_config:  src.engagement_db_coda_sync.configuration.CodaSyncConfiguration
    """
    normal_code_schemes = [c.code_scheme for c in coda_dataset_config.code_scheme_configurations]
    ws_code_scheme = ws_correct_dataset_code_scheme

    # Check for a WS code in any of the normal code schemes
    ws_code_in_normal_scheme = False
    for label in coda_message.get_latest_labels():
        if not label.checked:
            continue

        if label.scheme_id != ws_code_scheme.scheme_id:
            code = _code_for_label(label, normal_code_schemes)
            if code.control_code == Codes.WRONG_SCHEME:
                ws_code_in_normal_scheme = True

    # Check for a code in the WS code scheme
    code_in_ws_scheme = False
    for label in coda_message.get_latest_labels():
        if not label.checked:
            continue

        if label.scheme_id == ws_code_scheme.scheme_id:
            code_in_ws_scheme = True

    # Ensure there is a WS code in a normal scheme and a code in the WS scheme.
    # If there isn't, impute a coding_error code.
    if ws_code_in_normal_scheme != code_in_ws_scheme:
        log.warning(f"Imputing {Codes.CODING_ERROR} code (because ws_code_in_normal_scheme {ws_code_in_normal_scheme}) "
                    f"!= code_in_ws_scheme {code_in_ws_scheme} (message id {coda_message.message_id})")
        # Clear all duplicate schemes
        valid_code_scheme_ids = [code_scheme.scheme_id for code_scheme in normal_code_schemes] + [ws_code_scheme.scheme_id]
        for label in coda_message.get_latest_labels():
            cleared_label = None
            for scheme_id in valid_code_scheme_ids:
                if label.scheme_id.startswith(scheme_id):
                    cleared_label = Label(
                        scheme_id,
                        "SPECIAL-MANUALLY_UNCODED",
                        TimeUtils.utc_now_as_iso_string(),
                        Origin(Metadata.get_call_location(), "Engagement DB <-> Coda Sync", "External")
                    )
            assert cleared_label is not None
            coda_message.labels.insert(0, cleared_label)

        # Append a CE code under every normal + WS code scheme
        for code_scheme in normal_code_schemes + [ws_code_scheme]:
            ce_label = CleaningUtils.make_label_from_cleaner_code(
                code_scheme,
                code_scheme.get_code_with_control_code(Codes.CODING_ERROR),
                Metadata.get_call_location(),
                set_checked=True
            )
            coda_message.labels.insert(0, ce_label)


def _get_ws_code(coda_message, ws_code_scheme):
    """
    Gets the WS code assigned to a Coda message, if it exists, otherwise returns None.

    :param coda_message: Coda message to check for a WS code.
    :type coda_message: core_data_modules.data_models.Message
    :param coda_dataset_config: Dataset configuration to use to interpret this message's labels.
    :type coda_dataset_config: src.engagement_db_coda_sync.configuration.CodaDatasetConfiguration
    :param ws_correct_dataset_code_scheme: WS - Correct Dataset code scheme.
    :type ws_correct_dataset_code_scheme: core_data_modules.data_models.CodeScheme
    :return: WS code assigned to this message, if it exists.
    :rtype: core_data_modules.data_models.Code | None
    """
    for label in coda_message.get_latest_labels():
        if not label.checked:
            continue

        if label.scheme_id == ws_code_scheme.scheme_id:
            ws_code = ws_code_scheme.get_code_with_code_id(label.code_id)
            if ws_code.control_code in {Codes.NOT_CODED, Codes.CODING_ERROR}:
                log.warning(f"Code in WS - Correct Dataset scheme has control code '{ws_code.control_code}'; "
                            f"cannot redirect message")
                return None
            return ws_code

    return None


def _update_engagement_db_message_from_coda_message(engagement_db, engagement_db_message, coda_message, coda_config,
                                                    transaction=None):
    """
    Updates a message in the engagement database based on the labels in the Coda message.

    If the labels match, returns without updating anything.
    Otherwise, if the new labels contain a WS code, clears the labels and updates the dataset.
    Otherwise, overwrites the existing labels with the new labels.

    :param engagement_db: Engagement database to update the message in.
    :type engagement_db: engagement_database.EngagementDatabase
    :param engagement_db_message: Engagement database message to update
    :type engagement_db_message: engagement_database.data_models.Message
    :param coda_message: Coda message to use to update the engagement database message.
    :type coda_message: core_data_modules.data_models.Message
    :param coda_config: Configuration for the update.
    :type coda_config:  src.engagement_db_coda_sync.configuration.CodaSyncConfiguration
    :param transaction: Transaction in the engagement database to perform the update in.
    :type transaction: google.cloud.firestore.Transaction | None
    :return: Sync stats for the update.
    :rtype: src.engagement_db_coda_sync.sync_stats.EngagementDBToCodaSyncStats
    """
    coda_dataset_config = coda_config.get_dataset_config_by_engagement_db_dataset(engagement_db_message.dataset)
    sync_stats = EngagementDBToCodaSyncStats()

    # Impute Coding Errors, if needed.
    _impute_coding_error(coda_message, coda_dataset_config, coda_config.ws_correct_dataset_code_scheme)

    # Check if the labels in the engagement database message already match those from the coda message, and that
    # we don't need to WS-correct (in other words, that the dataset is correct).
    # If they do, return without updating anything.
    ws_code = _get_ws_code(coda_message, coda_config.ws_correct_dataset_code_scheme)
    if len(engagement_db_message.labels) == len(coda_message.labels):
        log.debug("Labels match")
        sync_stats.add_event(CodaSyncEvents.LABELS_MATCH)
        return sync_stats

    log.debug("Updating database message labels to match those in Coda")

    # WS-correct if there is a valid ws_code
    if ws_code is not None:
        try:
            correct_dataset = \
                coda_config.get_dataset_config_by_ws_code_string_value(ws_code.string_value).engagement_db_dataset
        except ValueError as e:
            # No dataset configuration found with an appropriate ws_code_string_value to move the message to.
            # Fallback to the default dataset if available, otherwise crash.
            if coda_config.default_ws_dataset is None:
                raise e
            correct_dataset = coda_config.default_ws_dataset

        # Ensure this message isn't being moved to a dataset which it has previously been assigned to.
        # This is because if the message has already been in this new dataset, there is a chance there is an
        # infinite loop in the WS labels, which could get very expensive if we end up cycling this message through
        # the same datasets at high frequency.
        # If this message has been in this dataset before, crash and wait for this to be manually corrected.
        # Note that this is a simple but heavy-handed approach to handling what should be a rare edge case.
        # If we encounter this problem more frequently than expected, upgrade this to a more sophisticated loop
        # detector/handler.
        assert correct_dataset not in engagement_db_message.previous_datasets, \
            f"Engagement db message '{engagement_db_message.message_id}' (text '{engagement_db_message.text}') " \
            f"is being WS-corrected to dataset '{correct_dataset}', but already has this dataset in its " \
            f"previous_datasets ({engagement_db_message.previous_datasets}). " \
            f"This suggests an infinite loop in the WS labels."

        # Clear the labels and correct the dataset (the message will sync with the new dataset on the next sync)
        log.debug(f"WS correcting from {engagement_db_message.dataset} to {correct_dataset}")
        engagement_db_message.labels = []
        engagement_db_message.previous_datasets.append(engagement_db_message.dataset)
        engagement_db_message.dataset = correct_dataset

        origin_details = {"coda_dataset": coda_dataset_config.coda_dataset_id,
                          "coda_message": coda_message.to_firebase_map()}
        engagement_db.set_message(
            message=engagement_db_message,
            origin=HistoryEntryOrigin(origin_name="Coda -> Database Sync (WS Correction)", details=origin_details),
            transaction=transaction
        )

        sync_stats.add_event(CodaSyncEvents.WS_CORRECTION)
        return sync_stats

    # We didn't find a WS label, so simply update the engagement database message to have the same labels as the
    # message in Coda.
    engagement_db_message.labels = coda_message.labels
    origin_details = {"coda_dataset": coda_dataset_config.coda_dataset_id,
                      "coda_message": coda_message.to_firebase_map()}
    engagement_db.set_message(
        message=engagement_db_message,
        origin=HistoryEntryOrigin(origin_name="Coda -> Database Sync", details=origin_details),
        transaction=transaction
    )

    sync_stats.add_event(CodaSyncEvents.UPDATE_ENGAGEMENT_DB_LABELS)
    return sync_stats
