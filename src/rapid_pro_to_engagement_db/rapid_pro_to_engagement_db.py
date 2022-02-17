import json
from datetime import timedelta

from core_data_modules.cleaners import URNCleaner
from core_data_modules.logging import Logger
from engagement_database.data_models import (Message, MessageDirections, MessageStatuses, HistoryEntryOrigin,
                                             MessageOrigin)
from storage.google_cloud import google_cloud_utils

from src.rapid_pro_to_engagement_db.cache import RapidProSyncCache
from src.rapid_pro_to_engagement_db.sync_stats import RapidProToEngagementDBSyncStats, RapidProSyncEvents

log = Logger(__name__)


def _get_contacts_from_cache(cache=None):
    """
    :param cache: Cache to check for contacts. If None, returns None.
    :type cache: src.rapid_pro_to_engagement_db.cache.RapidProSyncCache | None
    :return: Contacts from a cache, if the cache exists and a previous contacts file exists in the cache, else None.
    :rtype: list of temba_client.v2.Contact | None
    """
    if cache is None:
        return None
    else:
        return cache.get_contacts()


def _get_new_runs(rapid_pro, flow_id, flow_result_field, cache=None):
    """
    Gets new runs from Rapid Pro for the given flow.

    If a cache is provided and it contains a timestamp of a previous export, only returns runs that have been modified
    since the last export.

    :param rapid_pro: Rapid Pro client to use to download new runs.
    :type rapid_pro: rapid_pro_tools.rapid_pro_client.RapidProClient
    :param flow_id: Flow id to download runs for.
    :type flow_id: str
    :param flow_result_field: Result field in the flow.
    :type flow_result_field: str
    :param cache: Cache to check for a timestamp of a previous export. If None, downloads all runs.
    :type cache: src.rapid_pro_to_engagement_db.cache.RapidProSyncCache | None
    :return: Runs modified for the given flow since the cache was last updated, if possible, else from all of time.
    :rtype: list of temba_client.v2.Run
    """
    # Try to get the last modified timestamp from the cache
    flow_last_updated = None
    if cache is not None:
        flow_last_updated = cache.get_latest_run_timestamp(flow_id, flow_result_field)

    # If there is a last updated timestamp in the cache, only download and return runs that have been modified since.
    filter_last_modified_after = None
    if flow_last_updated is not None:
        filter_last_modified_after = flow_last_updated + timedelta(microseconds=1)

    return rapid_pro.get_raw_runs(flow_id, last_modified_after_inclusive=filter_last_modified_after)


def _normalise_and_validate_contact_urn(contact_urn):
    """
    Normalises and validates the given URN.

    Fails with an AssertionError if the given URN is invalid.

    :param contact_urn: URN to de-identify.
    :type contact_urn: str
    :return: Normalised contact urn.
    :rtype: str
    """
    if contact_urn.startswith("tel:"):
        # TODO: This is known to fail for golis numbers via Shaqodoon. Leaving as a fail-safe for now
        #       until we're ready to test with golis numbers.
        assert contact_urn.startswith("tel:+")

    if contact_urn.startswith("telegram:"):
        # Sometimes a telegram urn ends with an optional #<username> e.g. telegram:123456#testuser
        # To ensure we always get the same urn for the same telegram user, normalise telegram urns to exclude
        # this #<username>
        contact_urn = contact_urn.split("#")[0]

    return contact_urn


def _engagement_db_has_message(engagement_db, message):
    """
    Checks if an engagement database contains a message with the same origin id as the given message.

    :param engagement_db: Engagement database to check for the message.
    :type engagement_db: engagement_database.EngagementDatabase
    :param message: Message to check for existence.
    :type message: engagement_database.data_models.Message
    :return: Whether a message with this text, timestamp, and participant_uuid exists in the engagement database.
    :rtype: bool
    """
    matching_messages_filter = lambda q: q.where("origin.origin_id", "==", message.origin.origin_id)
    matching_messages = engagement_db.get_messages(firestore_query_filter=matching_messages_filter)
    assert len(matching_messages) < 2

    return len(matching_messages) > 0


def _ensure_engagement_db_has_message(engagement_db, message, message_origin_details, dry_run=False):
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
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return sync_events: Sync event.
    :rtype string
    """
    if _engagement_db_has_message(engagement_db, message):
        log.debug(f"Message already in engagement database")
        return RapidProSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB

    log.debug(f"Adding message to engagement database")
    if not dry_run:
        engagement_db.set_message(
            message,
            HistoryEntryOrigin(origin_name="Rapid Pro -> Database Sync", details=message_origin_details)
        )
    return RapidProSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB


def sync_rapid_pro_to_engagement_db(rapid_pro, engagement_db, uuid_table, rapid_pro_config, google_cloud_credentials_file_path, cache_path=None, dry_run=False):
    """
    Synchronises runs from a Rapid Pro workspace to an engagement database.

    :param rapid_pro: Rapid Pro client to sync from.
    :type rapid_pro: rapid_pro_tools.rapid_pro_client.RapidProClient
    :param engagement_db: Engagement database to sync to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param rapid_pro_config: Configuration for the sync.
    :type rapid_pro_config: src.rapid_pro_to_engagement_db.configuration.RapidProToEngagementDBConfiguration
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode
    :type cache_path: str | None
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    """
    # This implementation is WIP. It shows how we can non-incrementally synchronise a workspace to the database.
    # To enter production, we still need the following:
    # TODO: Handle deleted contacts.
    # TODO: Optimise fetching fields from the same flows, so we don't have to download the same runs multiple times.
    workspace_name = rapid_pro.get_workspace_name()
    workspace_uuid = rapid_pro.get_workspace_uuid()

    if rapid_pro_config.uuid_filter is not None:
        valid_participant_uuids = set(json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path,
            rapid_pro_config.uuid_filter.uuid_file_url
        )))
        log.info(f"Loaded {len(valid_participant_uuids)} valid contacts to filter for")

    if cache_path is not None:
        log.info(f"Initialising Rapid Pro sync cache at '{cache_path}/rapid_pro_to_engagement_db/{workspace_name}'")
        cache = RapidProSyncCache(f"{cache_path}/rapid_pro_to_engagement_db/{workspace_name}")
    else:
        log.warning("No `cache_path` provided. This tool will process all relevant runs from Rapid Pro from all of time")
        cache = None

    # Load contacts from the cache if possible.
    # (If the cache or a contacts file for this workspace don't exist, `contacts` will be `None` for now)
    contacts = _get_contacts_from_cache(cache)

    dataset_to_sync_stats = dict()  # of '{flow_name}.{flow_result_field}' -> RapidProToEngagementDBSyncStats
    for flow_config in rapid_pro_config.flow_result_configurations:
        sync_stats = RapidProToEngagementDBSyncStats()
        # Get the latest runs for this flow.
        flow_id = rapid_pro.get_flow_id(flow_config.flow_name)
        runs = _get_new_runs(rapid_pro, flow_id, flow_config.flow_result_field, cache)

        for _ in runs:
            sync_stats.add_event(RapidProSyncEvents.READ_RUN_FROM_RAPID_PRO)

        # Get any contacts that have been updated since we last asked, in case any of the downloaded runs are for very
        # new contacts.
        contacts = rapid_pro.update_raw_contacts_with_latest_modified(contacts)
        if cache is not None and not dry_run:
            cache.set_contacts(contacts)
        contacts_lut = {c.uuid: c for c in contacts}

        # Process each run in turn, adding it the engagement database if it contains a message relevant to this flow
        # config and the message hasn't already been added to the engagement database.
        log.info(f"Processing {len(runs)} new runs for flow '{flow_config.flow_name}'")
        for i, run in enumerate(runs):
            log.debug(f"Processing run {i + 1}/{len(runs)}, id {run.id}...")

            # Get the relevant result from this run, if it exists.
            rapid_pro_result = run.values.get(flow_config.flow_result_field)
            if rapid_pro_result is None:
                log.debug("No relevant run result; skipping")
                sync_stats.add_event(RapidProSyncEvents.RUN_EMPTY)
                # Update the cache so we know not to check this run again in this flow + result field context.
                if cache is not None and not dry_run:
                    cache.set_latest_run_timestamp(flow_id, flow_config.flow_result_field, run.modified_on)
                continue

            # De-identify the contact's full urn.
            if run.contact.uuid not in contacts_lut:
                log.warning(f"Found a run from a contact that isn't present in the contacts export; skipping. "
                            f"This is most likely because the contact was deleted, but could suggest a more serious "
                            f"problem.")
                sync_stats.add_event(RapidProSyncEvents.RUN_CONTACT_UUID_NOT_IN_CONTACTS)
                if cache is not None and not dry_run:
                    cache.set_latest_run_timestamp(flow_id, flow_config.flow_result_field, run.modified_on)
                continue
            contact = contacts_lut[run.contact.uuid]
            assert len(contact.urns) == 1, len(contact.urns)
            contact_urn = _normalise_and_validate_contact_urn(contact.urns[0])

            if rapid_pro_config.uuid_filter is not None:
                # If a uuid filter exists, then only add this message if the sender's uuid exists in the uuid table
                # and in the valid uuids. The check for presence in the uuid table is to ensure we don't add a uuid
                # table entry for people who didn't consent for us to continue to keep their data.
                if not uuid_table.has_data(contact_urn):
                    log.info("A uuid filter was specified but the message is not from a participant in the "
                             "uuid_table; skipping")
                    sync_stats.add_event(RapidProSyncEvents.UUID_FILTER_CONTACT_NOT_IN_UUID_TABLE)
                    if cache is not None and not dry_run:
                        cache.set_latest_run_timestamp(flow_id, flow_config.flow_result_field, run.modified_on)
                    continue
                if uuid_table.data_to_uuid(contact_urn) not in valid_participant_uuids:
                    log.info("A uuid filter was specified and the message is from a participant in the "
                             "uuid_table but is not in the uuid filter; skipping")
                    sync_stats.add_event(RapidProSyncEvents.CONTACT_NOT_IN_UUID_FILTER)
                    if cache is not None and not dry_run:
                        cache.set_latest_run_timestamp(flow_id, flow_config.flow_result_field, run.modified_on)
                    continue

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
                labels=[],
                origin=MessageOrigin(
                    origin_id=f"rapid_pro.workspace_{workspace_uuid}.flow_{flow_id}.run_{run.id}.result_{rapid_pro_result.name}",
                    origin_type="rapid_pro"
                )
            )
            message_origin_details = {
                "rapid_pro_workspace": workspace_name,
                "run_id": run.id,
                "flow_id": flow_id,
                "flow_name": flow_config.flow_name,
                "run_value": rapid_pro_result.serialize()
            }
            sync_event = _ensure_engagement_db_has_message(engagement_db, msg, message_origin_details, dry_run)
            sync_stats.add_event(sync_event)

            # Update the cache so we know not to check this run again in this flow + result field context.
            if cache is not None and not dry_run:
                cache.set_latest_run_timestamp(flow_id, flow_config.flow_result_field, run.modified_on)

        dataset_to_sync_stats[f"{flow_config.flow_name}.{flow_config.flow_result_field}"] = sync_stats

    # Log the summaries of actions taken for each dataset then for all datasets combined.
    all_sync_stats = RapidProToEngagementDBSyncStats()
    for flow_config in rapid_pro_config.flow_result_configurations:
        result_field = f"{flow_config.flow_name}.{flow_config.flow_result_field}"
        log.info(f"Summary of actions for flow result field '{result_field}':")
        dataset_to_sync_stats[result_field].print_summary()
        all_sync_stats.add_stats(dataset_to_sync_stats[result_field])

    dry_run_text = "(dry run)" if dry_run else ""
    log.info(f"Summary of actions for all flow result fields {dry_run_text}:")
    all_sync_stats.print_summary()
