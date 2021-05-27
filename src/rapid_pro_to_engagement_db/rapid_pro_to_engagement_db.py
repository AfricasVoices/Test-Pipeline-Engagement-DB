from core_data_modules.cleaners import URNCleaner
from core_data_modules.logging import Logger
from engagement_database.data_models import Message, MessageDirections, MessageStatuses, HistoryEntryOrigin
from rapid_pro_tools.rapid_pro_client import RapidProClient
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)


def init_rapid_pro_client(google_cloud_credentials_file_path, rapid_pro_domain, rapid_pro_token_file_url):
    """
    Initialises a RapidProClient from a Rapid Pro domain and token file url.

    :param google_cloud_credentials_file_path: Path to the google cloud credentials file to use to access the
                                               rapid pro token.
    :type google_cloud_credentials_file_path: str
    :param rapid_pro_domain: Domain which Rapid Pro is running on.
    :type rapid_pro_domain: str
    :param rapid_pro_token_file_url: GS url to the Rapid Pro token.
    :type rapid_pro_token_file_url: str
    :return: RapidProClient for
    :rtype: rapid_pro_tools.rapid_pro_client.RapidProClient
    """
    log.info(f"Initialising Rapid Pro client for domain {rapid_pro_domain} and auth "
             f"url {rapid_pro_token_file_url}...")
    rapid_pro_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, rapid_pro_token_file_url).strip()
    rapid_pro_client = RapidProClient(rapid_pro_domain, rapid_pro_token)
    log.info("Initialised Rapid Pro client")

    return rapid_pro_client


def _engagement_db_has_message(engagement_db, message):
    """
    Checks if an engagement database contains a message with the same text, timestamp, and participant_uuid as the
    given message.

    :param engagement_db: Engagement database to check for the message.
    :type engagement_db: engagement_database.EngagementDatabase
    :param message: Message to check for existence.
    :type message: engagement_database.data_models.Message
    :return: Whether a message with this text, timestamp, and participant_uuid exists in the engagement database.
    :rtype: bool
    """
    # TODO: Rapid Pro has a bug where timestamps occasionally drift by 1us when runs are archived.
    #       Confirm this is resolved before entering production with an '==' check on timestamps.
    filter = lambda q: q \
        .where("text", "==", message.text) \
        .where("timestamp", "==", message.timestamp) \
        .where("participant_uuid", "==", message.participant_uuid)

    matching_messages = engagement_db.get_messages(filter=filter)
    assert len(matching_messages) < 2

    return len(matching_messages) > 0


def _ensure_engagement_db_has_message(engagement_db, message, message_origin_details):
    """
    Ensures that the given message exists in an engagement database.

    This function will only write to the database if a message with the same text, timestamp, and participant_uuid
    doesn't already exist in the database.

    :param engagement_db: Engagement database to use.
    :type engagement_db: engagement_database.EngagementDatabase
    :param message: Message to make sure exists in the engagement database.
    :type message: engagement_database.data_models.Message
    :param message_origin_details: Message origin details, to be logged in the HistoryEntryOrigin.details.
    :type message_origin_details: dict
    """
    if _engagement_db_has_message(engagement_db, message):
        log.debug(f"Message already in engagement database")
        return

    log.debug(f"Adding message to engagement database")
    engagement_db.set_message(
        message,
        HistoryEntryOrigin(origin_name="Rapid Pro -> Database Sync", details=message_origin_details)
    )


def sync_rapid_pro_to_engagement_db(google_cloud_credentials_file_path, rapid_pro_to_engagement_db_configuration,
                                    engagement_db, uuid_table):
    """
    Synchronises runs from a Rapid Pro workspace to an engagement database.

    :param google_cloud_credentials_file_path: Path to a Google Cloud service account credentials file to use to access
                                               the credentials bucket.
    :type google_cloud_credentials_file_path: str
    :param rapid_pro_to_engagement_db_configuration: Configuration for the sync operation.
    :type rapid_pro_to_engagement_db_configuration: src.rapid_pro_to_engagement_db.configuration.RapidProToEngagementDBConfiguration
    :param engagement_db: Engagement database to sync to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    """
    # This implementation is WIP. It shows how we can non-incrementally synchronise a workspace to the database.
    # To enter production, we still need the following:
    # TODO: Support incremental update of runs and contacts.
    # TODO: Handle deleted contacts.
    # TODO: Handle contacts that have runs but haven't been fetched locally yet.
    # TODO: Optimise fetching fields from the same flows, so we don't have to download the same runs multiple times.

    rapid_pro = init_rapid_pro_client(google_cloud_credentials_file_path,
                                      rapid_pro_to_engagement_db_configuration.domain,
                                      rapid_pro_to_engagement_db_configuration.token_file_url)
    workspace_name = rapid_pro.get_workspace_name()

    # Build a look-up table of Rapid Pro contact uuid -> Rapid Pro contact so we can look up the urn for each run later.
    contacts = rapid_pro.get_raw_contacts()
    contacts_lut = {c.uuid: c for c in contacts}

    for flow_config in rapid_pro_to_engagement_db_configuration.flow_result_configurations:
        # Get the latest runs for this flow.
        flow_id = rapid_pro.get_flow_id(flow_config.flow_name)
        runs = rapid_pro.get_raw_runs(flow_id)

        for i, run in enumerate(runs):
            log.debug(f"Processing run {i + 1}/{len(runs)}, id {run.id}...")

            # Get the relevant result from this run, if it exists.
            rapid_pro_result = run.values.get(flow_config.flow_result_field)
            if rapid_pro_result is None:
                log.debug("No relevant run result")
                continue

            # De-identify the contact's urn.
            # Note (1) this de-identifies the full urn, so we can handle multiple channel types simultaneously.
            #      (2) we only get this far if there was a valid run result, which ensures we only add de-identification
            #          entries for participants who messaged us.
            contact_urn = contacts_lut[run.contact.uuid].urns[0]
            if contact_urn.startswith("tel:"):
                # TODO: This is known to fail for golis numbers via Shaqodoon. Leaving as a fail-safe for now
                #       until we're ready to test with golis numbers.
                assert contact_urn.startswith("tel:+")
            participant_uuid = uuid_table.data_to_uuid(contact_urn)

            # Create a message and origin objects for this result and ensure it's in the engagement database.
            msg = Message(
                participant_uuid=participant_uuid,
                text=rapid_pro_result.input,  # Raw text received from a participant
                timestamp=rapid_pro_result.time,  # Time at which Rapid Pro processed this message in the flow.
                direction=MessageDirections.IN,
                channel_operator=URNCleaner.clean_operator(contact_urn),
                status=MessageStatuses.LIVE,
                dataset=flow_config.engagement_db_dataset,
                labels=[]
            )
            message_origin_details = {
                "rapid_pro_workspace": workspace_name,
                "run_id": run.id,
                "run_value": rapid_pro_result.serialize()
            }
            _ensure_engagement_db_has_message(engagement_db, msg, message_origin_details)

