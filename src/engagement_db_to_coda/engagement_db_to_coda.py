from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.data_models import Message as CodaMessage
from core_data_modules.logging import Logger
from core_data_modules.util import SHAUtils, TimeUtils
from engagement_database.data_models import HistoryEntryOrigin, MessageStatuses
from google.cloud import firestore

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
    :type coda_dataset_config: src.engagement_db_to_coda.configuration.CodaDatasetConfiguration
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
    coda.add_message(coda_dataset_config.coda_dataset_id, coda_message)


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
    :type coda_config:  src.engagement_db_to_coda.configuration.CodaSyncConfiguration
    :param transaction: Transaction in the engagement database to perform the update in.
    :type transaction: google.cloud.firestore.Transaction | None
    """
    coda_dataset_config = coda_config.get_dataset_config_by_engagement_db_dataset(engagement_db_message.dataset)

    # Check if the labels in the engagement database message already match those from the coda message.
    # If they do, return without updating anything.
    # TODO: Validate if the dataset is correct too?
    if engagement_db_message.labels == coda_message.labels:
        log.debug("Labels match")
        return

    log.debug("Updating database message labels to match those in Coda")

    # Check the currently assigned labels for one in the WS - Correct Dataset scheme.
    # If we find one, move this message to the correct dataset and return.
    ws_scheme = coda_config.ws_correct_dataset_code_scheme
    for label in coda_message.get_latest_labels():
        if label.scheme_id == ws_scheme.scheme_id:
            ws_code = ws_scheme.get_code_with_code_id(label.code_id)
            correct_dataset = coda_config.get_dataset_config_by_ws_code_string_value(ws_code.string_value).engagement_db_dataset

            # Clear the labels and correct the dataset (the message will sync with the new dataset on the next sync)
            # TODO: There is a risk of creating an infinite update loop here if there is a cycle of WS-correction
            #       in the coda dataset. This needs to be addressed before we can enter production.
            log.debug(f"WS correcting from {engagement_db_message.dataset} to {correct_dataset}")
            engagement_db_message.labels = []
            engagement_db_message.dataset = correct_dataset

            origin_details = {"coda_dataset": coda_dataset_config.coda_dataset_id,
                              "coda_message": coda_message.to_firebase_map()}
            engagement_db.set_message(
                message=engagement_db_message,
                origin=HistoryEntryOrigin(origin_name="Coda -> Database Sync (WS Correction)", details=origin_details),
                transaction=transaction
            )

            return

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


@firestore.transactional
def _sync_next_message_to_coda(transaction, engagement_db, coda, coda_config, last_seen_message):
    """
    Syncs a message from an engagement database to Coda.

    This method:
     - Gets the least recently updated message that was last updated after `last_updated_since`.
     - Writes back a coda id if the engagement db message doesn't have one yet.
     - Syncs the labels from Coda to this message if the message already exists in Coda.
     - Creates a new message in Coda if this message hasn't been seen in Coda yet.

    :param transaction: Transaction in the engagement database to perform the update in.
    :type transaction: google.cloud.firestore.Transaction
    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param coda: Coda instance to sync the message to.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param coda_config: Coda sync configuration.
    :type coda_config: src.engagement_db_to_coda.configuration.CodaSyncConfiguration
    :param last_seen_message: Last seen message, downloaded from the database in a previous call, or None.
                              If provided, downloads the least recently updated (next) message after this one, otherwise
                              downloads the least recently updated message in the database.
    :type last_seen_message: engagement_database.data_models.Message | None
    :return: The engagement database message that was synced.
             If there was no new message to sync, returns None.
    :rtype: engagement_database.data_models.Message | None
    """
    if last_seen_message is None:
        messages_filter = lambda q: q \
            .where("status", "in", [MessageStatuses.LIVE, MessageStatuses.STALE]) \
            .order_by("last_updated") \
            .order_by("message_id") \
            .limit(1)
    else:
        # Get the next message modified at or later than the `last_seen_message`, excluding the `last_seen_message`.
        messages_filter = lambda q: q \
            .where("status", "in", [MessageStatuses.LIVE, MessageStatuses.STALE]) \
            .order_by("last_updated") \
            .order_by("message_id") \
            .where("last_updated", ">=", last_seen_message.last_updated) \
            .start_after({"last_updated": last_seen_message.last_updated, "message_id": last_seen_message.message_id}) \
            .limit(1)

    next_message_results = engagement_db.get_messages(filter=messages_filter, transaction=transaction)

    if len(next_message_results) == 0:
        return None
    else:
        engagement_db_message = next_message_results[0]

    log.info(f"Syncing message {engagement_db_message.message_id}...")

    # Ensure the message has a valid coda id. If it doesn't have one yet, write one back to the database.
    if engagement_db_message.coda_id is None:
        log.debug("Creating coda id")
        engagement_db_message.coda_id = SHAUtils.sha_string(engagement_db_message.text)
        engagement_db.set_message(
            message=engagement_db_message,
            origin=HistoryEntryOrigin(origin_name="Set coda_id", details={}),
            transaction=transaction
        )
    assert engagement_db_message.coda_id == SHAUtils.sha_string(engagement_db_message.text)

    # Look-up this message in Coda
    try:
        coda_dataset_config = coda_config.get_dataset_config_by_engagement_db_dataset(engagement_db_message.dataset)
    except ValueError:
        log.warning(f"No Coda dataset found for dataset '{engagement_db_message.dataset}'")
        return engagement_db_message
    coda_message = coda.get_message(coda_dataset_config.coda_dataset_id, engagement_db_message.coda_id)

    # If the message exists in Coda, update the database message based on the labels assigned in Coda
    if coda_message is not None:
        log.debug("Message already exists in Coda")
        _update_engagement_db_message_from_coda_message(engagement_db, engagement_db_message, coda_message, coda_config,
                                                        transaction=transaction)
        return engagement_db_message

    # The message isn't in Coda, so add it
    _add_message_to_coda(coda, coda_dataset_config, coda_config.ws_correct_dataset_code_scheme, engagement_db_message)

    return engagement_db_message


def sync_engagement_db_to_coda(engagement_db, coda, coda_config):
    """
    Syncs messages from an engagement database to Coda.

    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param coda: Coda instance to sync the message to.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param coda_config: Coda sync configuration.
    :type coda_config: src.engagement_db_to_coda.configuration.CodaSyncConfiguration
    """
    # TODO: Cache the last seen message so we can do incremental updates
    last_seen_message = None
    synced_messages = 0
    synced_message_ids = set()

    first_run = True
    while first_run or last_seen_message is not None:
        first_run = False

        # TODO: Sync by dataset rather than by database. This would reduce costs by removing the need to download
        #       irrelevant datasets and by removing the need to clear the cache every time we add a new dataset,
        #       as well as making this consistent with how the rapid pro and analysis scripts work.
        last_seen_message = _sync_next_message_to_coda(
            engagement_db.transaction(), engagement_db, coda, coda_config, last_seen_message
        )

        if last_seen_message is not None:
            synced_messages += 1
            synced_message_ids.add(last_seen_message.message_id)
            # We can see the same message twice in a run if we need to set a coda id, labels ,or do WS correction,
            # because in these cases we'll write back to one of the retrieved documents.
            # Log both the number of message objects processed and the number of unique message ids seen so we can
            # monitor both.
            log.info(f"Synced {synced_messages} message objects ({len(synced_message_ids)} unique message ids)")
