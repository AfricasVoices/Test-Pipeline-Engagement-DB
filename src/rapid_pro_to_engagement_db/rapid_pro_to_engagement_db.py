from datetime import timedelta

from core_data_modules.cleaners import URNCleaner
from core_data_modules.logging import Logger
from engagement_database.data_models import Message, MessageDirections, MessageStatuses, HistoryEntryOrigin

from src.rapid_pro_to_engagement_db.cache import RapidProSyncCache

log = Logger(__name__)


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


def sync_rapid_pro_to_engagement_db(rapid_pro, engagement_db, uuid_table, flow_result_configs, cache_path=None):
    """
    Synchronises runs from a Rapid Pro workspace to an engagement database.

    :param rapid_pro: Rapid Pro client to sync from.
    :type rapid_pro: rapid_pro_tools.rapid_pro_client.RapidProClient
    :param engagement_db: Engagement database to sync to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param flow_result_configs: Configuration for data to sync.
    :type flow_result_configs: list of rapid_pro_to_engagement_db.FlowResultConfiguration
    """
    # This implementation is WIP. It shows how we can non-incrementally synchronise a workspace to the database.
    # To enter production, we still need the following:
    # TODO: Support incremental update of runs and contacts.
    # TODO: Handle deleted contacts.
    # TODO: Handle contacts that have runs but haven't been fetched locally yet.
    # TODO: Optimise fetching fields from the same flows, so we don't have to download the same runs multiple times.

    workspace_name = rapid_pro.get_workspace_name()

    cache = None
    if cache_path is not None:
        cache = RapidProSyncCache(f"{cache_path}/{workspace_name}")

    # Load all the contacts in the Rapid Pro workspace, either from the cache or from Rapid Pro
    if cache is None:
        contacts = rapid_pro.get_raw_contacts()
    else:
        contacts = cache.get_contacts()
        if contacts is None:
            contacts = rapid_pro.get_raw_contacts()

    for flow_config in flow_result_configs:
        # Get the latest runs for this flow.
        flow_id = rapid_pro.get_flow_id(flow_config.flow_name)
        flow_last_updated = None
        if cache is not None:
            flow_last_updated = cache.get_flow_last_updated(flow_id, flow_config.flow_result_field)
        fetch_since = None
        if flow_last_updated is not None:
            fetch_since = flow_last_updated + timedelta(microseconds=1)
        runs = rapid_pro.get_raw_runs(flow_id, last_modified_after_inclusive=fetch_since)

        # Get any contacts that have been updated since we last asked, in case of the downloaded runs are for very
        # new contacts.
        contacts = rapid_pro.update_raw_contacts_with_latest_modified(contacts)
        if cache is not None:
            cache.set_contacts(contacts)
        contacts_lut = {c.uuid: c for c in contacts}

        for i, run in enumerate(runs):
            log.debug(f"Processing run {i + 1}/{len(runs)}, id {run.id}...")

            if cache is not None and (flow_last_updated is None or run.modified_on > flow_last_updated):
                flow_last_updated = run.modified_on

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
            if contact_urn.startswith("telegram:"):
                # Sometimes a telegram urn ends with an optional #<username> e.g. telegram:123456#testuser
                # To ensure we always get the same urn for the same telegram user, normalise telegram urns to exclude
                # this #<username>
                contact_urn = contact_urn.split("#")[0]
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

            if cache is not None:
                cache.set_flow_last_updated(flow_id, flow_config.flow_result_field, flow_last_updated)
