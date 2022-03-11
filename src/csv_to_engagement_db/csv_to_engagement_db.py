import csv
import urllib.parse
from datetime import datetime
from io import StringIO

import pytz
from core_data_modules.cleaners import URNCleaner
from core_data_modules.logging import Logger
from core_data_modules.util import SHAUtils
from engagement_database.data_models import (Message, MessageDirections, MessageOrigin, MessageStatuses,
                                             HistoryEntryOrigin)
from storage.google_cloud import google_cloud_utils

from src.common.cache import Cache
from src.csv_to_engagement_db.sync_stats import CSVSyncEvents, CSVToEngagementDBSyncStats

log = Logger(__name__)


def _parse_date_string(date_string, timezone):
    """
    :param date_string: Date string to parse.
    :type date_string: str
    :param timezone: Timezone to interpret the date_string in, e.g. 'Africa/Nairobi'.
    :type timezone: str
    :return: Parsed datetime, in the given timezone.
    :rtype: datetime.datetime
    """
    # Try parsing using a list of all the variants we've seen for expressing timestamps.
    for date_format in ["%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"]:
        try:
            parsed_raw_date = datetime.strptime(date_string, date_format)
            break
        except ValueError:
            pass
    else:
        raise ValueError(f"Could not parse date {date_string}")
    return pytz.timezone(timezone).localize(parsed_raw_date)


def _csv_message_to_engagement_db_message(csv_message, uuid_table, origin_id, csv_source):
    """
    Converts a CSV message to an engagement database message.

    If there is no valid dataset for this converted message, returns None.

    :param csv_message: Dictionary containing the headers: 'Sender', 'Message', and 'ReceivedOn'.
    :type csv_message: dict
    :param uuid_table: UUID table to use to re-identify the URNs so we can set the channel operator.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param origin_id: Origin id, for the message origin field.
    :type origin_id: str
    :param csv_source:
    :type csv_source: src.csv_to_engagement_db.configuration.CSVSource
    :return: `csv_message` as an engagement db message.
    :rtype: engagement_database.data_models.Message | None
    """
    participant_uuid = csv_message["Sender"]
    assert participant_uuid.startswith(uuid_table._uuid_prefix), f"Sender uuid does not start with uuid prefix " \
                                                                 f"'{uuid_table._uuid_prefix}'"
    participant_urn = uuid_table.uuid_to_data(participant_uuid)
    channel_operator = URNCleaner.clean_operator(participant_urn)

    timestamp = _parse_date_string(csv_message["ReceivedOn"], csv_source.timezone)

    try:
        dataset = csv_source.get_dataset_for_timestamp(timestamp)
    except LookupError:
        return None

    return Message(
        participant_uuid=participant_uuid,
        text=csv_message["Message"],
        timestamp=timestamp,
        direction=MessageDirections.IN,
        channel_operator=channel_operator,
        status=MessageStatuses.LIVE,
        dataset=dataset,
        labels=[],
        origin=MessageOrigin(
            origin_id=origin_id,
            origin_type="csv"
        )
    )


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

    This function will only write to the database if a message with the same origin_id doesn't already exist in the
    database.

    :param engagement_db: Engagement database to use.
    :type engagement_db: engagement_database.EngagementDatabase
    :param message: Message to make sure exists in the engagement database.
    :type message: engagement_database.data_models.Message
    :param message_origin_details: Message origin details, to be logged in the HistoryEntryOrigin.details.
    :type message_origin_details: dict
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return sync_events: Sync event.
    :rtype str
    """
    if _engagement_db_has_message(engagement_db, message):
        log.debug(f"Message already in engagement database")
        return CSVSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB

    log.debug(f"Adding message to engagement database dataset {message.dataset}...")
    if not dry_run:
        engagement_db.set_message(
            message,
            HistoryEntryOrigin(origin_name="CSV -> Database Sync", details=message_origin_details)
        )
    return CSVSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB


def _sync_csv_to_engagement_db(google_cloud_credentials_file_path, csv_source, engagement_db, uuid_table, cache=None,
                               dry_run=False):
    """
    Syncs a CSV to an engagement database.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use when
                                               downloading the CSV.
    :type google_cloud_credentials_file_path: str
    :param csv_source: CSV source to sync.
    :type csv_source: src.csv_to_engagement_db.configuration.CSVSource
    :param engagement_db: Engagement database to sync the CSV to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to re-identify the URNs so we can set the channel operator.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param cache:
    :type cache: src.common.cache.Cache
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return: Sync stats for the sync.
    :rtype: CSVToEngagementDBSyncStats
    """
    sync_stats = CSVToEngagementDBSyncStats()

    log.info(f"Downloading csv from '{csv_source.gs_url}'...")
    raw_csv_string = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, csv_source.gs_url)
    csv_hash = SHAUtils.sha_string(raw_csv_string)
    raw_data = list(csv.DictReader(StringIO(raw_csv_string)))
    log.info(f"Downloaded {len(raw_data)} messages in csv '{csv_source.gs_url}'")

    # Convert the gs_url to a format that is safe to use as a cache entry name.
    escaped_csv_url = urllib.parse.quote_plus(csv_source.gs_url)
    if cache is None:
        prev_csv_hash = None
    else:
        prev_csv_hash = cache.get_string(escaped_csv_url)

    if prev_csv_hash is not None:
        assert csv_hash == prev_csv_hash, f"CSV '{csv_source.gs_url}' differs since the last time it was requested. " \
                                          f"To avoid accidental duplication, please inspect the problem, then clear " \
                                          f"the cache and re-run when it is safe to proceed."
        log.info("This file matches a previous version of the file that was processed in the past.")
        log.info("Returning without reprocessing any of the messages in this file.")
        return sync_stats

    for i, csv_msg in enumerate(raw_data):
        log.info(f"Processing message {i + 1}/{len(raw_data)}...")
        sync_stats.add_event(CSVSyncEvents.READ_ROW_FROM_CSV)
        engagement_db_message = _csv_message_to_engagement_db_message(
            csv_msg, uuid_table, f"csv_{csv_hash}.row_{i}", csv_source
        )

        if engagement_db_message is None:
            log.info(f"No matching dataset for this message, sent at time '{csv_msg['ReceivedOn']}'")
            sync_stats.add_event(CSVSyncEvents.MESSAGE_SKIPPED_NO_MATCHING_TIMESTAMP)
            continue

        message_origin_details = {
            "csv_row_number": i,
            "csv_row_data": csv_msg,
            "csv_sync_configuration": csv_source.to_dict(),
            "csv_hash": csv_hash
        }
        sync_event = _ensure_engagement_db_has_message(engagement_db, engagement_db_message, message_origin_details, dry_run)
        sync_stats.add_event(sync_event)

    if cache is not None and not dry_run:
        cache.set_string(escaped_csv_url, csv_hash)

    return sync_stats


def sync_csvs_to_engagement_db(google_cloud_credentials_file_path, csv_sources, engagement_db, uuid_table,
                               cache_path=None, dry_run=False):
    """
    Syncs CSVs to an engagement database.

    The CSVs must contain the headers 'Sender', 'Message', and 'ReceivedOn'.

    Messages are synced using the file hash and row index in the CSV as the message origin_ids. This means a CSV
    can't be edited after it has been synced without first removing the messages from the original sync.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use when
                                               downloading the CSVs.
    :type google_cloud_credentials_file_path: str
    :param csv_sources: CSV sources to sync to the engagement database.
    :type csv_sources: list of src.csv_to_engagement_db.configuration.CSVSource
    :param engagement_db: Engagement database to sync the CSVs to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to re-identify the URNs so we can set the channel operator.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str | None
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    """
    if cache_path is None:
        cache = None
    else:
        cache = Cache(cache_path)

    source_to_sync_stats = dict()
    for i, csv_source in enumerate(csv_sources):
        log.info(f"Syncing csv {i + 1}/{len(csv_sources)}: {csv_source.gs_url}...")
        source_sync_stats = _sync_csv_to_engagement_db(
            google_cloud_credentials_file_path, csv_source, engagement_db, uuid_table, cache, dry_run
        )
        source_to_sync_stats[csv_source.gs_url] = source_sync_stats

    # Log the summaries of actions taken for each dataset then for all datasets combined.
    all_sync_stats = CSVToEngagementDBSyncStats()
    for csv_source in csv_sources:
        log.info(f"Summary of actions for csv source '{csv_source.gs_url}':")
        source_to_sync_stats[csv_source.gs_url].print_summary()
        all_sync_stats.add_stats(source_to_sync_stats[csv_source.gs_url])

    dry_run_text = " (dry run)" if dry_run else ""
    log.info(f"Summary of actions for all {len(csv_sources)} csv source(s){dry_run_text}:")
    all_sync_stats.print_summary()
