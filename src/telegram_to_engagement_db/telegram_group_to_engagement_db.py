from dateutil.parser import isoparse

from storage.google_cloud import google_cloud_utils
from core_data_modules.logging import Logger

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import (PeerChannel)

from engagement_database.data_models import (Message, MessageDirections, MessageOrigin, MessageStatuses,
                                             HistoryEntryOrigin)



log = Logger(__name__)


def _initialize_telegram_client(pipeline_config, google_cloud_credentials_file_path, telegram_source):
    log.info('Initializing telegram client...')
    log.info('Downloading telegram access tokens...')
    telegram_tokens = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, telegram_source.token_file_url).strip()

    api_id = telegram_tokens['api_id']
    api_hash = telegram_tokens['api_hash']
    phone_number = telegram_tokens['phone_number']

    client = TelegramClient(f'{pipeline_config.pipeline_name}_telegram_session_name', api_id, api_hash)
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


def _telegram_comment_to_engagement_db_message(telegram_message, dataset, origin_id, uuid_table):
    """
    Converts a telegram comment to an engagement database message.

    :param telegram_message: Dictionary containing the telegram message data values.
    :type telegram_message: dict
    :param dataset: Initial dataset to assign this message to in the engagement database.
    :type dataset: str
    :param origin_id: Origin id, for the comment origin field.
    :type origin_id: str
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :return: `message` as an engagement db message.
    :rtype: engagement_database.data_models.Message
    """


    return Message(
        participant_uuid=telegram_message.sender_id,
        text=telegram_message.message,
        timestamp=isoparse(telegram_message.date).isoformat(),
        direction=MessageDirections.IN,
        channel_operator='telegram',  #TODO move to core as a CONSTANT in core,
        status=MessageStatuses.LIVE,
        dataset=dataset,
        labels=[],
        origin=MessageOrigin(
            origin_id=origin_id,
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
    :param telegram_message: Comment to make sure exists in the engagement database.
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

