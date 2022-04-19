from datetime import datetime
import json

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import (PeerChannel)

from storage.google_cloud import google_cloud_utils
from core_data_modules.logging import Logger

from engagement_database.data_models import (Message, MessageDirections, MessageOrigin, MessageStatuses,
                                             HistoryEntryOrigin)

from src.telegram_to_engagement_db.cache import TelegramGroupSyncCache

log = Logger(__name__)

# TODO: move to social media tools
async def _initialize_telegram_client(telegram_token_file_url, google_cloud_credentials_file_path, pipeline_name):
    """
    :param telegram_token_file_url: Path to the Google Cloud file path that contains telegram tokens api_id
                                                api_hash and telegram app admin phone number.
    :type telegram_token_file_url: str
    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use when
                                               downloading facebook page token.
    :type google_cloud_credentials_file_path: str
    :param pipeline: Name of the pipeline for this telegram session.
    :type pipeline: str
    :return telegram_client
    :rtype: telethon.client.telegramclient.TelegramClient

    """
    log.info('Downloading telegram access tokens...')
    telegram_tokens = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, telegram_token_file_url).strip())

    api_id = telegram_tokens['api_id']
    api_hash = telegram_tokens['api_hash']
    phone_number = telegram_tokens['phone_number'] #TODO Accept more than one phone number

    log.info('Initializing telegram client...')
    telegram = TelegramClient(f'{pipeline_name}_telegram_session_name', api_id, api_hash)
    await telegram.start()

    # Ensure the client is authorized
    # While authenticating the first time the client will send an auth code to the application phone number.
    # The authentication details are stored in a session file to enable you authenticate automatically the next time the script runs.
    # To re-authenticate delete the session file.
    if not await telegram.is_user_authorized():
        await telegram.send_code_request(phone_number)
        try:
            await telegram.sign_in(phone_number, input(f"Enter the authorization code sent to "
                                                              f"telegram a/c for {phone_number}: "))
        # If the a/c has 2FA enabled sign_in() will raise a SessionPasswordNeededError.
        # Input your telegram password to proceed.
        except SessionPasswordNeededError:
            await telegram.sign_in(password=input('Password: '))
    log.info('Initialized telegram client...')

    return telegram


async def _fetch_message_from_group(telegram, group_id, end_date=None, start_message_id=None):
    """
    :param telegram: Instance of telegram app to use to download the group messages from.
    :type telegram: telethon.client.telegramclient.TelegramClient
    :param group_id: Id of the telegram group to fetch messages from
    :type group_id: str
    :param end_date: Offset datetime, messages previous to this date will be retrieved. Exclusive
    :type end_date: datetime | None
    :param start_message_id: All the messages with a lower (older) ID or equal to this will be excluded.
    :type start_message_id: int | None
    :yields: Instances of telethon.tl.custom.message.Message
    """
    # Get group/channel entity
    group_entity = await telegram.get_entity(PeerChannel(int(group_id)))

    # Fetch messages messages based on dataset_offset_date and/or min_id filters if specified.
    if end_date is None and start_message_id is None:
        log.info(f"Fetching all messages from group {group_id}")
        return telegram.iter_messages(group_entity)

    elif end_date is not None and start_message_id is None:
        log.info(f"Fetching messages from group {group_id} sent before {end_date}, exclusive")
        return telegram.iter_messages(group_entity, offset_date=end_date)

    elif end_date is None and start_message_id is not None:
        log.info(f"Fetching messages from group {group_id} with message.id greater than "
                 f"{start_message_id}, exclusive")
        return telegram.iter_messages(group_entity, min_id=int(start_message_id))

    elif end_date is not None and start_message_id is not None:
        log.info(f"Fetching messages from group {group_id} sent before {end_date} "
                 f"and with message.id greater than {start_message_id}, both exclusive")
        return telegram.iter_messages(group_entity, offset_date=end_date,
                                      min_id=int(start_message_id))


def _is_avf_message(telegram_message):
    """
    :param telegram_message: A telegram message object to check if it was sent by an admin or is a channel broadcast
                             Returns True if a message.from_id is None because admins send messages anonymously in the group
                             and/or is of the type PeerChannel i.e channel broadcast messages.
    :type telegram_message: telethon.tl.custom.message.Message
    """
    # Skip messages sent by AVF group admins / channel broadcasts
    return (type(telegram_message.from_id) == PeerChannel or telegram_message.from_id is None)

def _telegram_message_to_engagement_db_message(telegram_message, dataset, uuid_table):
    """
    Converts a telegram message to an engagement database message.

    :param telegram_message: A telegram message object.
    :type telegram_message: telethon.tl.custom.message.Message
    :param dataset: Name of dataset to assign this message to in the engagement database.
    :type dataset: str
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :return: `telegram_message` as an engagement db message.
    :rtype: engagement_database.data_models.Message
    """
    participant_uuid = uuid_table.data_to_uuid(telegram_message.sender_id)
    channel_operator = 'telegram'  # TODO move to core as a CONSTANT

    return Message(
        participant_uuid=participant_uuid,
        text=telegram_message.message,
        timestamp=telegram_message.date,
        direction=MessageDirections.IN,
        channel_operator=channel_operator,
        status=MessageStatuses.LIVE,
        dataset=dataset,
        labels=[],
        origin=MessageOrigin(
            # Message id is reusable if the message is deleted.
            # Use a combination of message id and datetime for the origin id to make it unique
            origin_id=f"message_id_{telegram_message.id}_timestamp_{telegram_message.date.isoformat()}",
            origin_type="telegram_group"
        )
    )


def _engagement_db_has_message(engagement_db, message):
    """
    Checks if an engagement database contains a comment with the same origin id as the given comment.

    :param engagement_db: Engagement database to check for the comment.
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


def _ensure_engagement_db_has_message(engagement_db, telegram_message, message_origin_details):
    """
    Ensures that the given telegram message exists in an engagement database.
    This function will only write to the database if a message with the same origin_id doesn't already exist in the
    database.

    :param engagement_db: Engagement database to use.
    :type engagement_db: engagement_database.EngagementDatabase
    :param telegram_message: Message to make sure exists in the engagement database.
    :type telegram_message: engagement_database.data_models.Message
    :param message_origin_details: Comment origin details, to be logged in the HistoryEntryOrigin.details.
    :type message_origin_details: dict
    """
    if _engagement_db_has_message(engagement_db, telegram_message):
        log.debug(f"message already in engagement database")
        return

    log.debug(f"Adding message to engagement database")
    engagement_db.set_message(
        telegram_message,
        HistoryEntryOrigin(origin_name="Telegram Group -> Database Sync", details=message_origin_details)
    )


async def sync_messages_from_groups_to_engagement_db(telegram_group_source, telegram,
                                                     engagement_db, uuid_table, cache_path):
    """
    :param telegram_group_source: Telegram sources to sync to the engagement database.
    :type telegram_group_source: List of src.telegram_to_engagement_db.configuration.TelegramGroupSource
    :param telegram: Instance of telegram app to use to download the group messages from.
    :type telegram: telethon.client.telegramclient.TelegramClient
    :param uuid_table: UUID table to use to re-identify the URNs so we can set the channel operator.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str | None
    """
    if cache_path is None:
        cache = None
    else:
        log.info(f"Initialising TelegramSyncCache at '{cache_path}/telegram_group_to_engagement_db'")
        cache = TelegramGroupSyncCache(f"{cache_path}/telegram_group_to_engagement_db")

    for dataset in telegram_group_source.datasets:
        log.info(f"Fetching messages for {dataset.engagement_db_dataset}...")
        dataset_start_date = datetime.fromisoformat(dataset.search.start_date)
        dataset_end_date = datetime.fromisoformat(dataset.search.end_date)

        for group_id in dataset.search.group_ids:
            group_cache_entry_name = f"{dataset.engagement_db_dataset}_{group_id}"
            dataset_group_latest_seen_message_id = None if cache is None else cache.get_latest_group_message_id(group_cache_entry_name)

            # Fetch group messages sent before the dataset_end_date and/or contain message.id greater than min_id in
            # cache if available.
            group_messages = await _fetch_message_from_group(telegram, group_id, dataset_end_date, dataset_group_latest_seen_message_id)

            broadcast_admin_messages = 0
            async for telegram_message in group_messages:
                if _is_avf_message(telegram_message):
                    broadcast_admin_messages += 1
                    continue

                # Filter messages sent between this dataset start and end_time.
                if not (dataset_start_date <= telegram_message.date < dataset_end_date):
                    continue

                message_origin_details = {"message_id": telegram_message.id,
                                          "group_id": telegram_message.peer_id.channel_id,
                                          "timestamp": telegram_message.date,
                                          "text": telegram_message.message,}

                message = _telegram_message_to_engagement_db_message(telegram_message, dataset.engagement_db_dataset,
                                                                         uuid_table)
                _ensure_engagement_db_has_message(engagement_db, message, message_origin_details)

                # The api returns messages from newest to oldest, cache the id of the newest seen message for this search
                if dataset_group_latest_seen_message_id is None:
                    dataset_group_latest_seen_message_id = telegram_message.id

            # Cache only if all the available group messages have been added to engagement db
            if cache is not None and dataset_group_latest_seen_message_id is not None:
                cache.set_latest_group_message_id(group_cache_entry_name, dataset_group_latest_seen_message_id)

            log.info(f"Skipped {broadcast_admin_messages} channel broadcast and admin reply messages ...")
