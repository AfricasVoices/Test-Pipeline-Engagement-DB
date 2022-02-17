import json

from coda_v2_python_client.firebase_client_wrapper import CodaV2Client
from core_data_modules.cleaners import Codes
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.data_models import Message as CodaMessage, Label, Origin
from core_data_modules.logging import Logger
from core_data_modules.util import TimeUtils
from engagement_database.data_models import HistoryEntryOrigin
from storage.google_cloud import google_cloud_utils

from src.engagement_db_coda_sync.sync_stats import CodaSyncEvents, EngagementDBToCodaSyncStats

log = Logger(__name__)


def _get_coda_users_from_gcloud(dataset_users_file_url, google_cloud_credentials_file_path):
    return json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, dataset_users_file_url
    ))


def ensure_coda_datasets_up_to_date(coda, coda_config, google_cloud_credentials_file_path, dry_run): 
    """
    Ensures coda datasets are up to date based on coda configuration. 

    :param coda: Coda instance to add the message to.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param coda_config: Coda sync configuration.
    :type coda_config: src.engagement_db_coda_sync.configuration.CodaSyncConfiguration
    :param google_cloud_credentials_file_path: Path to a Google Cloud service account credentials file 
                                               to use to access the credentials bucket.
    :type google_cloud_credentials_file_path: str
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    """
    all_datasets_have_user_file_url = all(
        dataset_config.dataset_users_file_url is not None for dataset_config in coda_config.dataset_configurations)

    default_project_user_ids = []
    if not all_datasets_have_user_file_url:
        assert coda_config.project_users_file_url is not None, \
         f"Specify user ids for coda datasets in CodaDatasetConfiguration or user ids for this project in CodaSyncConfiguration"
        default_project_user_ids = _get_coda_users_from_gcloud(coda_config.project_users_file_url, google_cloud_credentials_file_path)

    ws_correct_dataset_code_scheme = coda_config.ws_correct_dataset_code_scheme
    for dataset_config in coda_config.dataset_configurations:
        log.info(f"Updating user ids and code schemes in coda dataset '{dataset_config.coda_dataset_id}'")
        config_user_ids = []
        if dataset_config.dataset_users_file_url:
            config_user_ids = _get_coda_users_from_gcloud(dataset_config.dataset_users_file_url, google_cloud_credentials_file_path)
        else:
            config_user_ids = default_project_user_ids

        coda_user_ids = coda.get_dataset_user_ids(dataset_config.coda_dataset_id)
        if coda_user_ids is None or set(coda_user_ids) != set(config_user_ids):
            if not dry_run:
                coda.set_dataset_user_ids(dataset_config.coda_dataset_id, config_user_ids)
            log.info(f"User ids added to Coda: {len(config_user_ids)}")
        else:
            log.info(f"User ids are up to date")

        repo_code_schemes = []
        for code_scheme_config in dataset_config.code_scheme_configurations:
            for count in range(1, code_scheme_config.coda_code_schemes_count + 1):
                if count == 1:
                    repo_code_schemes.append(code_scheme_config.code_scheme)
                else:
                    code_scheme_copy = code_scheme_config.code_scheme.copy()
                    code_scheme_copy.scheme_id = f"{code_scheme_copy.scheme_id}-{count}"
                    repo_code_schemes.append(code_scheme_copy)
        repo_code_schemes.append(ws_correct_dataset_code_scheme)
        repo_code_schemes_lut = {code_scheme.scheme_id: code_scheme for code_scheme in repo_code_schemes}

        coda_code_schemes = coda.get_all_code_schemes(dataset_config.coda_dataset_id)
        coda_code_schemes_lut = {code_scheme.scheme_id: code_scheme for code_scheme in coda_code_schemes}

        for coda_scheme_id, coda_code_scheme in coda_code_schemes_lut.items():
            if coda_scheme_id not in repo_code_schemes_lut.keys():
                log.warning(f"There are code schemes in coda not in this repo; The code schemes will be ignored")
                coda_code_schemes.remove(coda_code_scheme)

        updated_code_schemes = []
        for repo_scheme_id, repo_code_scheme in repo_code_schemes_lut.items():
            if repo_scheme_id not in coda_code_schemes_lut.keys():
                updated_code_schemes.append(repo_code_scheme)
                repo_code_schemes.remove(repo_code_scheme)

        assert len(repo_code_schemes) == len(coda_code_schemes), \
                f"`repo_code_schemes` must be equal to `coda_code_schemes`"
        
        repo_code_schemes.sort(key=lambda s: s.scheme_id)
        coda_code_schemes.sort(key=lambda s: s.scheme_id)
        
        repo_and_coda_code_schemes_pairs = zip(repo_code_schemes, coda_code_schemes)
        for repo_code_scheme, coda_code_scheme in repo_and_coda_code_schemes_pairs:
            if repo_code_scheme != coda_code_scheme:
                updated_code_schemes.append(repo_code_scheme)

        if len(updated_code_schemes) > 0:
            if not dry_run:
                coda.add_and_update_dataset_code_schemes(dataset_config.coda_dataset_id, updated_code_schemes)
            log.info(f"Code schemes added to Coda: {len(updated_code_schemes)}")
            for code_scheme in updated_code_schemes:
                log.info(f"Added code scheme {code_scheme.scheme_id}")
        else:
            log.info(f"Code schemes are up to date")


def _add_message_to_coda(coda, coda_dataset_config, ws_correct_dataset_code_scheme, engagement_db_message, dry_run=False):
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
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
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
    if not dry_run:
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


def _get_ws_code(coda_message, coda_dataset_config, ws_correct_dataset_code_scheme):
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
    ws_code = None
    for label in coda_message.get_latest_labels():
        if not label.checked:
            continue

        if label.scheme_id == ws_code_scheme.scheme_id:
            code_in_ws_scheme = True
            ws_code = ws_code_scheme.get_code_with_code_id(label.code_id)

    # Ensure there is a WS code in a normal scheme and a code in the WS scheme.
    # If there isn't, don't attempt any redirect, so we can impute a CE code later.
    if ws_code_in_normal_scheme != code_in_ws_scheme:
        # TODO: Impute CE here?
        log.warning(f"Not WS-correcting message because ws_code_in_normal_scheme ({ws_code_in_normal_scheme}) "
                    f"!= code_in_ws_scheme ({code_in_ws_scheme})")
        ws_code = None

    # If the ws code is 'NC', that means the message was labelled as being in the wrong place, but the right place
    # was unknown/could not be specified. In this case, don't redirect, so we can see the 'WS' in analysis.
    if ws_code is not None and ws_code.control_code == Codes.NOT_CODED:
        log.warning(f"Code in WS - Correct Dataset scheme has control code '{Codes.NOT_CODED}'; cannot redirect message")
        ws_code = None

    return ws_code


def _update_engagement_db_message_from_coda_message(engagement_db, engagement_db_message, coda_message, coda_config,
                                                    transaction=None, dry_run=False):
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
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return: Sync events for the update.
    :rtype: list of str
    """
    coda_dataset_config = coda_config.get_dataset_config_by_engagement_db_dataset(engagement_db_message.dataset)
    sync_events = []

    # Check if the labels in the engagement database message already match those from the coda message, and that
    # we don't need to WS-correct (in other words, that the dataset is correct).
    # If they do, return without updating anything.
    ws_code = _get_ws_code(coda_message, coda_dataset_config, coda_config.ws_correct_dataset_code_scheme)
    if engagement_db_message.labels == coda_message.labels and ws_code is None:
        log.debug("Labels match")
        sync_events.append(CodaSyncEvents.LABELS_MATCH)
        return sync_events

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
                          "coda_message": coda_message.to_dict(serialize_datetimes_to_str=True)}

        if not dry_run:
            engagement_db.set_message(
                message=engagement_db_message,
                origin=HistoryEntryOrigin(origin_name="Coda -> Database Sync (WS Correction)", details=origin_details),
                transaction=transaction
            )

        sync_events.append(CodaSyncEvents.WS_CORRECTION)
        return sync_events

    # We didn't find a WS label, so simply update the engagement database message to have the same labels as the
    # message in Coda.
    engagement_db_message.labels = coda_message.labels
    origin_details = {"coda_dataset": coda_dataset_config.coda_dataset_id,
                      "coda_message": coda_message.to_dict(serialize_datetimes_to_str=True)}

    if not dry_run:
        engagement_db.set_message(
            message=engagement_db_message,
            origin=HistoryEntryOrigin(origin_name="Coda -> Database Sync", details=origin_details),
            transaction=transaction
        )

    sync_events.append(CodaSyncEvents.UPDATE_ENGAGEMENT_DB_LABELS)
    return sync_events
