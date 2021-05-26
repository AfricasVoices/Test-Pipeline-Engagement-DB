import json

import core_data_modules.data_models
from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.logging import Logger
from core_data_modules.util import SHAUtils, TimeUtils
from engagement_database.data_models import HistoryEntryOrigin
from google.cloud import firestore

log = Logger(__name__)


def _add_message_to_coda(coda, coda_dataset_config, db_message):
    log.debug("Adding message to Coda")

    coda_message = core_data_modules.data_models.Message(
        message_id=db_message.coda_id,
        text=db_message.text,
        creation_date_time_utc=TimeUtils.datetime_to_utc_iso_string(db_message.timestamp),
        labels=[]
    )

    # If the engagement database message already has labels, initialise with these in Coda.
    if len(db_message.labels) > 0:
        # TODO: Validate that these labels are valid under the code schemes being copied to.
        coda_message.labels = db_message.labels

    # Otherwise, if there is an auto-coder specified, run that.
    elif coda_dataset_config.auto_coder is not None:
        label = CleaningUtils.apply_cleaner_to_text(db_message.text, coda_dataset_config.auto_coder, coda_dataset_config.code_scheme)
        if label is not None:
            coda_message.labels = [label]

    # Add the message to the Coda dataset.
    coda.add_message(coda_dataset_config.coda_dataset_id, coda_message)


def latest_labels(coda_message):
    out = []
    seen_scheme_ids = set()
    for l in coda_message.labels:
        if l.code_id == "SPECIAL-MANUALLY_UNCODED":
            continue
        if l.scheme_id not in seen_scheme_ids:
            out.append(l)
            seen_scheme_ids.add(l.scheme_id)
    return out


@firestore.transactional
def _process_message(transaction, engagement_db, coda, coda_config, message_id):
    """

    :param transaction:
    :type transaction:
    :param engagement_db:
    :type engagement_db: engagement_database.EngagementDatabase
    :param coda:
    :type coda:
    :param message_id:
    :type message_id:
    :return:
    :rtype:
    """
    db_message = engagement_db.get_messages(filter=lambda q: q.where("message_id", "==", message_id), transaction=transaction)[0]
    # db_message = engagement_db.get_message(message_id, transaction=transaction)

    # Ensure the message has a valid coda id. If it doesn't have one yet, write one back to the database.
    if db_message.coda_id is None:
        log.debug("Creating coda id")
        db_message.coda_id = SHAUtils.sha_string(db_message.text)
        engagement_db.set_message(
            message=db_message,
            origin=HistoryEntryOrigin(origin_name="Set coda_id", details={}),
            transaction=transaction
        )
    assert db_message.coda_id == SHAUtils.sha_string(db_message.text)

    # Look-up this message in Coda
    try:
        coda_dataset_config = coda_config.get_dataset_config_by_engagement_db_dataset(db_message.dataset)
    except ValueError:
        log.debug("No Coda dataset found")
        return
    coda_message = coda.get_message(coda_dataset_config.coda_dataset_id, db_message.coda_id)

    # If the message exists in Coda, update the database message based on the labels assigned in Coda
    if coda_message is not None:
        log.debug("Message already exists in Coda")

        if json.dumps([x.to_dict() for x in db_message.labels]) == json.dumps([y.to_dict() for y in coda_message.labels]):
            log.debug("Labels match")
            return

        log.debug("Updating database message labels to match those in Coda")
        # TODO: consider WS-correction
        ws_scheme = coda_config.ws_correct_dataset_code_scheme
        for label in latest_labels(coda_message):
            if label.scheme_id == ws_scheme.scheme_id:
                ws_code = ws_scheme.get_code_with_code_id(label.code_id)
                correct_dataset = coda_config.get_dataset_config_by_ws_code_string_value(ws_code.string_value).engagement_db_dataset

                # Clear the labels and correct the dataset (the message will sync with the new dataset on the next sync
                # TODO: Probably better to recursively fix this now, so we can detect any cycles in the WS correction code
                log.debug(f"WS correcting from {db_message.dataset} to {correct_dataset}")
                db_message.labels = []
                db_message.dataset = correct_dataset

                origin_details = {"coda_dataset": coda_dataset_config.coda_dataset_id, "coda_message": coda_message.to_firebase_map()}
                engagement_db.set_message(
                    message=db_message,
                    origin=HistoryEntryOrigin(origin_name="Coda -> Database Sync (WS Correction)", details=origin_details),
                    transaction=transaction
                )

                return

        db_message.labels = coda_message.labels
        origin_details = {"coda_dataset": coda_dataset_config.coda_dataset_id, "coda_message": coda_message.to_firebase_map()}
        engagement_db.set_message(
            message=db_message,
            origin=HistoryEntryOrigin(origin_name="Coda -> Database Sync", details=origin_details),
            transaction=transaction
        )

        return

    # The message isn't in Coda, so add it
    _add_message_to_coda(coda, coda_dataset_config, db_message)


def sync_engagement_db_to_coda(google_cloud_credentials_file_path, coda_configuration, engagement_db):
    coda = coda_configuration.init_coda(google_cloud_credentials_file_path)

    messages = engagement_db.get_messages()

    for msg in messages:
        log.info(f"Processing message {msg.message_id}...")
        _process_message(engagement_db.transaction(), engagement_db, coda, coda_configuration, msg.message_id)
