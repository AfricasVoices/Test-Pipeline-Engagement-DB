from datetime import datetime

import json

from storage.google_cloud import google_cloud_utils
from core_data_modules.logging import Logger

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import (PeerChannel)

from engagement_database.data_models import (Message, MessageDirections, MessageOrigin, MessageStatuses,
                                             HistoryEntryOrigin)

from src.telegram_to_engagement_db.cache import TelegramGroupSyncCache


log = Logger(__name__)


#TODO: move to social media tools?
async def _initialize_telegram_client(telegram_group_source, pipeline_name, google_cloud_credentials_file_path):

    log.info('Initializing telegram client...')
    log.info('Downloading telegram access tokens...')
    telegram_tokens = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, telegram_group_source.token_file_url).strip())

    api_id = telegram_tokens['api_id']
    api_hash = telegram_tokens['api_hash']
    phone_number = telegram_tokens['phone_number']

    client = TelegramClient(f'{pipeline_name}_telegram_session_name', api_id, api_hash)
    await client.start()

    # Ensure the client is authorized
    if await client.is_user_authorized() == False:
        await client.send_code_request(phone_number)
        try:
            await client.sign_in(phone_number, input(f"Enter the authorization code sent to telegram a/c for {phone_number}: "))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    log.info('Initialized telegram client...')

    return client


async def _fetch_message_from_group(telegram, group_id, dataset_offset_date=None, min_id=None):
    # Get group entities
    group_entity = await telegram.get_entity(PeerChannel(int(group_id)))

    #Fetch messages messages based on dataset_offset_date and/or max_id filters
    if dataset_offset_date is None and min_id is None:
        log.info(f"Fetching all messages from group {group_id}")
        return telegram.iter_messages(group_entity)

    elif dataset_offset_date is not None and min_id is None:
        log.info(f"Fetching messages from group {group_id} sent before {dataset_offset_date}, exclusive")
        return telegram.iter_messages(group_entity, offset_date=dataset_offset_date)

    elif dataset_offset_date is None and min_id is not None:
        log.info(f"Fetching messages from group {group_id} with message.id greater than {min_id}, exclusive")
        return telegram.iter_messages(group_entity, max_id=int(min_id))

    elif dataset_offset_date is not None and min_id is not None:
        log.info(f"Fetching messages from group {group_id} sent before {dataset_offset_date} "
                 f"and with message.id greater than {min_id}, both exclusive")
        return telegram.iter_messages(group_entity, offset_date=dataset_offset_date, min_id=int(min_id))


def _is_avf_messages(message):
    """
    :param message:
    :type message:
    """

    # Skip messages sent by AVF group admins / channel broadcasts
    if (type(message.from_id) == PeerChannel or message.from_id is None):
        return True
    else:
        return False


def _telegram_message_to_engagement_db_message(telegram_message, dataset, uuid_table):
    """
    Converts a telegram comment to an engagement database message.

    :param telegram_message: A telegram message object.
    :type telegram_message: telethon.tl.custom.message.Message
    :param dataset: Initial dataset to assign this message to in the engagement database.
    :type dataset: str
    :param origin_id: Origin id, for the comment origin field.
    :type origin_id: str
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :return: `message` as an engagement db message.
    :rtype: engagement_database.data_models.Message
    """

    participant_uuid = uuid_table.data_to_uuid(telegram_message.sender_id)
    channel_operator = 'telegram'  # TODO move to core as a CONSTANT

    return Message(
        participant_uuid=participant_uuid,
        text=telegram_message.message,
        timestamp=telegram_message.date.isoformat(),
        direction=MessageDirections.IN,
        channel_operator=channel_operator,  #TODO move to core as a CONSTANT in core,
        status=MessageStatuses.LIVE,
        dataset=dataset,
        labels=[],
        origin=MessageOrigin(
            origin_id=telegram_message.id,
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


async def sync_messages_from_groups_to_engagement_db(cache_path, telegram_group_source, telegram,
                                                     engagement_db, uuid_table):
    """
    :param
    :type

    """

    if cache_path is None:
        cache = None
    else:
        log.info(f"Initialising TelegramSyncCache at '{cache_path}/telegram_group_to_engagement_db'")
        cache = TelegramGroupSyncCache(f"{cache_path}/telegram_group_to_engagement_db")

    all_messages = []
    for dataset in telegram_group_source.datasets:
        log.info(f"Fetching messages for {dataset.engagement_db_dataset}...")
        dataset_start_date = datetime.fromisoformat(dataset.search.start_date)
        dataset_end_date = datetime.fromisoformat(dataset.search.end_date)

        for group_id in dataset.search.group_ids:
            group_cache_file_name = f"{dataset.engagement_db_dataset}_{group_id}"
            group_min_id = None if cache is None else cache.get_latest_group_min_id(group_cache_file_name)

            # Fetch group messages sent before the dataset_end_date and/or contain message.id greater than min_id in
            # cache if available.
            group_messages = await _fetch_message_from_group(telegram, group_id, dataset_end_date, group_min_id)

            broad_cast_admin_messages = 0
            cache_synced = False
            async for telegram_message in group_messages:
                if _is_avf_messages(telegram_message):
                    broad_cast_admin_messages +=1
                    continue

                # Filter messages sent between this dataset start and end_time.
                if dataset_start_date <= telegram_message.date < dataset_end_date:
                    message_origin_details = {"message_id": telegram_message.id, "group_id":
                                              telegram_message.peer_id.channel_id}
                    message = _telegram_message_to_engagement_db_message(telegram_message, dataset.engagement_db_dataset,
                                                                         uuid_table)
                    all_messages.append(message.to_dict())
                    _ensure_engagement_db_has_message(engagement_db, message, message_origin_details)

                    # The api returns messages from newest to oldest, cache the id of the newest seen message for this search
                    if cache is not None and cache_synced is False:
                        cache.set_latest_group_min_id(group_cache_file_name, telegram_message.id)
                        cache_synced = True

            log.info(f"Skipped {broad_cast_admin_messages} channel broadcast and admin reply messages ...")

        with open(f'all_messages.json', 'w') as outfile:
            json.dump(all_messages, outfile)