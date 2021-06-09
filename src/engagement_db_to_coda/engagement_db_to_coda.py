from core_data_modules.cleaners.cleaning_utils import CleaningUtils
from core_data_modules.data_models import Message as CodaMessage
from core_data_modules.logging import Logger
from core_data_modules.util import SHAUtils, TimeUtils
from engagement_database.data_models import HistoryEntryOrigin, MessageStatuses
from google.cloud import firestore

from src.engagement_db_to_coda.cache import CodaSyncCache

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
    coda.add_message_to_dataset(coda_dataset_config.coda_dataset_id, coda_message)


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
def _sync_next_engagement_db_message_to_coda(transaction, engagement_db, coda, coda_config, dataset_config, last_seen_message):
    """
    Syncs a message from an engagement database to Coda.

    This method:
     - Gets the least recently updated message that was last updated after `last_seen_message`.
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
    :param dataset_config: Configuration for the dataset to sync.
    :type dataset_config: src.engagement_db_to_coda.configuration.CodaDatasetConfiguration
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
            .where("dataset", "==", dataset_config.engagement_db_dataset) \
            .order_by("last_updated") \
            .order_by("message_id") \
            .limit(1)
    else:
        # Get the next message modified at or later than the `last_seen_message`, excluding the `last_seen_message`.
        messages_filter = lambda q: q \
            .where("status", "in", [MessageStatuses.LIVE, MessageStatuses.STALE]) \
            .where("dataset", "==", dataset_config.engagement_db_dataset) \
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
    coda_message = coda.get_dataset_message(dataset_config.coda_dataset_id, engagement_db_message.coda_id)

    # If the message exists in Coda, update the database message based on the labels assigned in Coda
    if coda_message is not None:
        log.debug("Message already exists in Coda")
        _update_engagement_db_message_from_coda_message(engagement_db, engagement_db_message, coda_message, coda_config,
                                                        transaction=transaction)
        return engagement_db_message

    # The message isn't in Coda, so add it
    _add_message_to_coda(coda, dataset_config, coda_config.ws_correct_dataset_code_scheme, engagement_db_message)

    return engagement_db_message


@firestore.transactional
def _sync_coda_message_to_engagement_db(transaction, coda_message, engagement_db, engagement_db_dataset, coda_config):
    """
    Syncs a coda message to an engagement database, by downloading all the engagement database messages which match the
    coda message's id and dataset, and making sure the labels match.

    :param transaction: Transaction in the engagement database to perform the update in.
    :type transaction: google.cloud.firestore.Transaction
    :param coda_message: Coda Message to sync.
    :type coda_message: core_data_modules.data_models.Message
    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param engagement_db_dataset: Dataset in the engagement database to update.
    :type engagement_db_dataset: str
    :param coda_config: Configuration for the update.
    :type coda_config: src.engagement_db_to_coda.configuration.CodaSyncConfiguration
    """
    # Get the messages in the engagement database that match this dataset and coda message id
    engagement_db_messages = engagement_db.get_messages(
        filter=lambda q: q
            .where("dataset", "==", engagement_db_dataset)
            .where("coda_id", "==", coda_message.message_id),
        transaction=transaction
    )
    log.info(f"{len(engagement_db_messages)} engagement db message(s) match Coda message {coda_message.message_id}")

    # Update each of the matching messages with the labels currently in Coda.
    for i, engagement_db_message in enumerate(engagement_db_messages):
        log.info(f"Processing matching engagement message {i + 1}/{len(engagement_db_messages)}: "
                 f"{engagement_db_message.message_id}...")
        _update_engagement_db_message_from_coda_message(
            engagement_db, engagement_db_message, coda_message, coda_config, transaction=transaction)


def _sync_engagement_db_dataset_to_coda(engagement_db, coda, coda_config, dataset_config, cache):
    """
    Syncs messages from one engagement database dataset to Coda.

    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param coda: Coda instance to sync the message to.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param coda_config: Coda sync configuration.
    :type coda_config: src.engagement_db_to_coda.configuration.CodaSyncConfiguration
    :param dataset_config: Configuration for the dataset to sync.
    :type dataset_config: src.engagement_db_to_coda.configuration.CodaDatasetConfiguration
    """
    last_seen_message = None if cache is None else cache.get_last_seen_message(dataset_config.engagement_db_dataset)
    synced_messages = 0
    synced_message_ids = set()

    first_run = True
    while first_run or last_seen_message is not None:
        first_run = False

        last_seen_message = _sync_next_engagement_db_message_to_coda(
            engagement_db.transaction(), engagement_db, coda, coda_config, dataset_config, last_seen_message
        )

        if last_seen_message is not None:
            synced_messages += 1
            synced_message_ids.add(last_seen_message.message_id)
            if cache is not None:
                cache.set_last_seen_message(dataset_config.engagement_db_dataset, last_seen_message)

            # We can see the same message twice in a run if we need to set a coda id, labels, or do WS correction,
            # because in these cases we'll write back to one of the retrieved documents.
            # Log both the number of message objects processed and the number of unique message ids seen so we can
            # monitor both.
            log.info(f"Synced {synced_messages} message objects ({len(synced_message_ids)} unique message ids) in "
                     f"dataset {dataset_config.engagement_db_dataset}")
        else:
            log.info(f"No more new messages in dataset {dataset_config.engagement_db_dataset}")


def sync_engagement_db_to_coda(engagement_db, coda, coda_config, cache_path=None):
    """
    Syncs messages from an engagement database to Coda.

    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param coda: Coda instance to sync the message to.
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param coda_config: Coda sync configuration.
    :type coda_config: src.engagement_db_to_coda.configuration.CodaSyncConfiguration
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str | None
    """
    if cache_path is None:
        cache = None
        log.warning(f"No `cache_path` provided. This tool will process all relevant messages from all of time")
    else:
        cache = CodaSyncCache(cache_path)

    for dataset_config in coda_config.dataset_configurations:
        log.info(f"Syncing engagement db dataset {dataset_config.engagement_db_dataset} to Coda dataset "
                 f"{dataset_config.coda_dataset_id}...")
        _sync_engagement_db_dataset_to_coda(engagement_db, coda, coda_config, dataset_config, cache)


def sync_coda_to_engagement_db(coda, engagement_db, coda_configuration):
    """

    :param coda:
    :type coda: coda_v2_python_client.firebase_client_wrapper.CodaV2Client
    :param engagement_db:
    :type engagement_db:
    :param coda_configuration:
    :type coda_configuration:
    :return:
    :rtype:
    """
    for coda_dataset_config in coda_configuration.dataset_configurations:
        log.info(f"Getting messages from Coda dataset {coda_dataset_config.coda_dataset_id}...")
        coda_messages = coda.get_dataset_messages(coda_dataset_config.coda_dataset_id)

        for i, coda_message in enumerate(coda_messages):
            log.info(f"Processing Coda message {i + 1}/{len(coda_messages)}: {coda_message.message_id}...")
            _sync_coda_message_to_engagement_db(
                engagement_db.transaction(), coda_message, engagement_db, coda_configuration, coda_dataset_config)
