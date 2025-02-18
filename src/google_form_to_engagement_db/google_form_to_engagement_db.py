from collections import OrderedDict

from core_data_modules.cleaners import PhoneCleaner
from core_data_modules.logging import Logger
from dateutil.parser import isoparse
from engagement_database.data_models import (Message, MessageDirections, MessageStatuses, MessageOrigin,
                                             HistoryEntryOrigin)
from google.cloud.firestore_v1 import FieldFilter

from src.common.cache import Cache
from src.google_form_to_engagement_db.configuration import GoogleFormParticipantIdTypes
from src.google_form_to_engagement_db.sync_stats import GoogleFormToEngagementDBSyncStats, GoogleFormSyncEvents

log = Logger(__name__)


def _validate_configuration_against_form_structure(form_question_ids, form_config):
    """
    Validates the configuration of a Google Form against its structure.

    This function checks for the following conditions:
    - Ensures that there are no duplicated question IDs in the configuration.
    - Verifies that all questions specified in the configuration exist in the form.
    - Logs a warning for any questions present in the form that are not specified in the configuration.

    :param form_question_ids: A set of question IDs present in the Google Form.
    :type form_question_ids: set
    :param form_config: Configuration to use for the validation.
    :type form_config: src.google_form_to_engagement_db.configuration.GoogleFormToEngagementDBConfiguration
    """
    config_question_ids = set()
    if form_config.participant_id_configuration is not None:
        config_question_ids.add(form_config.participant_id_configuration.question_id)

    for question_config in form_config.question_configurations:
        for question_id in question_config.question_ids:
            assert question_id not in config_question_ids, \
                f"Question '{question_id} specified in configuration for form {form_config.form_id} twice"
            config_question_ids.add(question_id)

    # Ensure that all questions requested in the configuration exist in the form.
    config_question_ids_not_in_form = config_question_ids - form_question_ids
    assert len(config_question_ids_not_in_form) == 0,\
        f"Some questions requested in the configuration do not exist in form " \
        f"{form_config.form_id}: {config_question_ids_not_in_form}"

    # Check if there were any questions in the form that do not exist in the configuration.
    # Warn about these cases, but don't fail because it's possible not all questions asked are to be analysed.
    form_question_ids_not_in_config = form_question_ids - config_question_ids
    if len(form_question_ids_not_in_config) != 0:
        log.warning(f"Found some questions in the form that aren't set in the configuration: "
                    f"{form_question_ids_not_in_config}")


def _validate_phone_number_and_format_as_urn(phone_number, country_code, valid_length, valid_prefixes=None):
    """
    :param phone_number: Phone number to validate and format. This may be just the phone number or the phone number
                         and country code, and may contain punctuation or alpha characters e.g. tel:+ or (0123) 70-40
    :type phone_number: str
    :param country_code: Expected country code. This method ensures the phone number begins with this country code,
                         or adds it if not.
    :type country_code: str
    :param valid_length: Valid length of the phone number, including the country code.
                         This function will fail with a value error if it sees a phone number that doesn't have
                         this length.
    :type valid_length: int
    :param valid_prefixes: Optional list of prefixes to check. If provided, this function will ensure every phone
                           number starts with one of these prefixes. For example, this could be used to ensure
                           this is a mobile number, or to ensure it belongs to a valid network.
    :type valid_prefixes: set of str | None
    :return: Phone number as urn e.g. 'tel:+254700123123' or None.
    :rtype: str | None
    """
    # Normalise the phone number (removes spaces, non-numeric, and leading 0s).
    phone_number = PhoneCleaner.normalise_phone(phone_number)

    if len(phone_number) == 0:
        raise ValueError("Invalid phone number")

    if phone_number.startswith(country_code):
        if valid_prefixes is not None:
            if not len([p for p in valid_prefixes if phone_number.replace(country_code, "").startswith(p)]) == 1:
                raise ValueError(f"Phone number must contain a valid prefix; Valid prefixes specified: {','.join(valid_prefixes)}")
    else:
        if valid_prefixes is not None:
            if not len([p for p in valid_prefixes if phone_number.startswith(p)]) == 1:
                raise ValueError(f"Phone number must contain a valid prefix; Valid prefixes specified: {','.join(valid_prefixes)}")
        phone_number = f"{country_code}{phone_number}"

    if not len(phone_number) == valid_length:
        raise ValueError("Invalid phone number length")  

    urn = f"tel:+{phone_number}"
    return urn


def _get_participant_uuid_for_response(response, id_type, participant_id_question_id, uuid_table, form_config):
    """
    Gets the participant_uuid for the given response.

    If the response contains an answer to a question with id `participant_id_question_id`, validates the contact
    info given on the form and formats it as a URN.

    If no answer or question_id is provided or an invalid answer is provided, uses the response id as the participant_uuid 
    instead. In this case, the response id is not de-identified via the uuid table.

    :param response: Response to get the participant uuid for.
    :type response: dict
    :param id_type: A GoogleFormIdType
    :type id_type: str
    :param participant_id_question_id: Id of the participant_id question.
    :type participant_id_question_id: str | None
    :param uuid_table: UUID table to use to de-identify the urn
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param form_config: Configuration for the form to sync.
    :type form_config: src.google_form_to_engagement_db.configuration.GoogleFormToEngagementDBConfiguration
    :return: Participant uuid for this response.
    :rtype: str
    """
    participant_id_answers = response["answers"].get(participant_id_question_id, None)
    if participant_id_answers is None:
        participant_uuid = response["responseId"]
    else:
        participant_id_answers_count = len(participant_id_answers["textAnswers"]["answers"])
        assert participant_id_answers_count == 1, f"Expected one answer for participant id, " \
            f"but found {participant_id_answers_count} answers"
        participant_id = participant_id_answers["textAnswers"]["answers"][0]["value"]

        assert id_type == GoogleFormParticipantIdTypes.KENYA_MOBILE_NUMBER, \
            f"Participant id type {id_type} not recognised."
        
        try:
            participant_urn = _validate_phone_number_and_format_as_urn(
                phone_number=participant_id, country_code="254", valid_length=12, valid_prefixes={"10", "11", "7"}
            )
            participant_uuid = uuid_table.data_to_uuid(participant_urn)
        except ValueError as e:
            if form_config.ignore_invalid_mobile_numbers:
                log.warning(f"{e}, using the response id as the participant_uuid instead")
                participant_uuid = response["responseId"]
            else:
                raise e

    return participant_uuid


def _form_answer_to_engagement_db_message(form_answer, form_id, form_response, participant_uuid,
                                          question_id_to_engagement_db_dataset):
    """
    Converts a Form answer to an engagement database message.

    :param form_answer: Answer to convert, in Google Forms' answer dictionary format.
    :type form_answer: dict
    :param form_id: Id of the form this answer is for.
    :type form_id: str
    :param form_response: The form response that this answer was given as part of, in Google Forms' response dictionary
                          format
    :type form_response: dict
    :param question_id_to_engagement_db_dataset: Dictionary of Google Form question id -> engagement db dataset to
                                                 use for that question.
    :type question_id_to_engagement_db_dataset: dict of str -> str
    :return: `form_answer` as an engagement db message.
    :rtype: engagement_database.data_models.Message
    """
    text = ", ".join([answer["value"] for answer in form_answer["textAnswers"]["answers"]])

    return Message(
        participant_uuid=participant_uuid,
        text=text,
        timestamp=isoparse(form_response["createTime"]),
        direction=MessageDirections.IN,
        channel_operator="google_form",  # TODO: Move google_form to core_data_modules.Codes
        status=MessageStatuses.LIVE,
        dataset=question_id_to_engagement_db_dataset[form_answer["questionId"]],
        labels=[],
        origin=MessageOrigin(
            origin_id=f"google_form_id_{form_id}.response_id_{form_response['responseId']}.question_id_{form_answer['questionId']}",
            origin_type="google_form"
        )
    )


def _merge_engagement_db_messages(messages_with_origin_details, answers_delimeter):
    assert len(messages_with_origin_details) > 1, \
        f"Expected at least 2 messages with origin details, but found {len(messages_with_origin_details)}."

    participant_uuid, dataset = None, None
    texts, timestamps, origin_ids, messages_origin_details = [], [], [], []
    for index, message_with_origin_details in enumerate(messages_with_origin_details):
        msg, origin_details = message_with_origin_details

        texts.append(msg.text)
        timestamps.append(msg.timestamp)
        origin_ids.append(msg.origin.origin_id)
        messages_origin_details.append(origin_details)

        if index == 0:
            participant_uuid, dataset = msg.participant_uuid, msg.dataset
            continue

        assert participant_uuid is not None and msg.participant_uuid == participant_uuid, \
            f"Attempted merging messages where the participant uuid is None or the messages are not from the same participant"

        assert dataset is not None and msg.dataset == dataset, \
            f"Attempted merging messages where the dataset is None or the messages are not from the same dataset"

    text, timestamp = answers_delimeter.join(texts), min(timestamps, key=lambda x: x.timestamp())
    message = Message(
        participant_uuid=participant_uuid,
        text=text,
        timestamp=timestamp,
        direction=MessageDirections.IN,
        channel_operator="google_form",
        status=MessageStatuses.LIVE,
        dataset=dataset,
        labels=[],
        origin=MessageOrigin(
            origin_id=origin_ids,
            origin_type="google_form"
        )
    )

    message_origin_details = {
        "formId": messages_origin_details[0]["formId"],
        "answer": [msg["answer"] for msg in messages_origin_details],
    }

    return message, message_origin_details


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
    matching_messages_filter = lambda q: q.where(filter=FieldFilter("origin.origin_id", "==", message.origin.origin_id))
    matching_messages = engagement_db.get_messages(firestore_query_filter=matching_messages_filter)
    assert len(matching_messages) < 2, f"Expected at most 1 matching message in database, but found {len(matching_messages)}."

    return len(matching_messages) > 0


def _ensure_engagement_db_has_message(engagement_db, message_with_origin_details, dry_run=False):
    """
    Ensures that the given message exists in an engagement database.

    This function will only write to the database if a message with the same origin_id doesn't already exist in the
    database.

    :param engagement_db: Engagement database to use.
    :type engagement_db: engagement_database.EngagementDatabase
    :param message_with_origin_details: Tuple of message to make sure exists in the engagement database and message origin details, 
                                        to be logged in the HistoryEntryOrigin.details.
    :type message_with_origin_details: (engagement_database.data_models.Message, dict)
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return: Sync event.
    :rtype: str
    """
    message, message_origin_details = message_with_origin_details
    if _engagement_db_has_message(engagement_db, message):
        log.debug(f"Message already in engagement database")
        return GoogleFormSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB

    log.debug(f"Adding message to engagement database dataset {message.dataset}...")
    if not dry_run:
        engagement_db.set_message(
            message,
            HistoryEntryOrigin(origin_name="Google Form -> Database Sync", details=message_origin_details)
        )
    return GoogleFormSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB


def _sync_google_form_to_engagement_db(google_form_client, engagement_db, form_config, uuid_table, cache=None, dry_run=False):
    """
    Syncs a Google Form to an engagement database.

    Adds an engagement database message for every answer to a question specified in the form_config.

    :param google_form_client: Google forms client to use to download the form and responses.
    :type google_form_client: src.google_form_to_engagement_db.google_forms_client.GoogleFormsClient
    :param engagement_db: Engagement database to sync the Google Form to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param form_config: Configuration for the form to sync.
    :type form_config: src.google_form_to_engagement_db.configuration.GoogleFormToEngagementDBConfiguration
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param cache: Cache to use, or None. If None, downloads all form responses. If a cache is specified, only fetches
                  responses last submitted after this function was last run.
    :type cache: src.common.cache.Cache | None
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return: sync_stats
    :rtype: src.google_form_to_engagement_db.sync_stats.GoogleFormToEngagementDBSyncStats
    """
    log.info(f"Downloading form question ids of form {form_config.form_id}...")
    form_question_ids = google_form_client.get_question_ids(form_config.form_id)

    try:
        log.info(f"Validating question configurations...")
        _validate_configuration_against_form_structure(form_question_ids, form_config)
    except AssertionError as e:
        log.warning(f"Assertion error in _validate_configuration_against_form_structure: {e}")

    question_id_to_engagement_db_dataset = dict()
    for question_config in form_config.question_configurations:
        for question_id in question_config.question_ids:
            question_id_to_engagement_db_dataset[question_id] = question_config.engagement_db_dataset

    # Download responses
    last_seen_response_time = None if cache is None else cache.get_date_time(form_config.form_id)
    responses = google_form_client.get_form_responses(
        form_config.form_id, submitted_after_exclusive=last_seen_response_time
    )
    log.info(f"Downloaded {len(responses)} response(s)")

    # Process each response and ensure its answers are all in the engagement database.
    responses.sort(key=lambda resp: resp["lastSubmittedTime"])
    sync_stats = GoogleFormToEngagementDBSyncStats()
    for i, response in enumerate(responses):
        question_id_to_engagement_db_message, question_id_to_message_origin_details = dict(), dict()
        log.info(f"Processing response {i + 1}/{len(responses)}...")
        sync_stats.add_event(GoogleFormSyncEvents.READ_RESPONSE_FROM_GOOGLE_FORM)

        participant_id_type, participant_id_question_id = None, None
        if form_config.participant_id_configuration is not None:
            participant_id_type = form_config.participant_id_configuration.id_type
            participant_id_question_id = form_config.participant_id_configuration.question_id
        participant_uuid = _get_participant_uuid_for_response(
            response, participant_id_type, participant_id_question_id, uuid_table, form_config
        )

        answers = response["answers"].values()
        for j, answer in enumerate(answers):
            log.info(f"Processing answer {j + 1}/{len(answers)} for response {i + 1}/{len(responses)}...")
            if answer["questionId"] == participant_id_question_id:
                log.info(f"This answer is to the participant id question, skipping")
                continue

            sync_stats.add_event(GoogleFormSyncEvents.READ_ANSWER_FROM_RESPONSE)
            if answer["questionId"] not in question_id_to_engagement_db_dataset:
                log.info(f"This answer is to question {answer['questionId']}, which isn't configured in this sync")
                continue

            engagement_db_message = _form_answer_to_engagement_db_message(
                answer, form_config.form_id, response, participant_uuid, question_id_to_engagement_db_dataset
            )
            engagement_db_message_origin_details = {
                "formId": form_config.form_id,
                "answer": answer,
            }
            question_id_to_engagement_db_message[answer["questionId"]] = (engagement_db_message, engagement_db_message_origin_details)

        for question_config in form_config.question_configurations:
            assert len(question_config.question_ids) > 0, "No question ids found in the question configuration."

            if len(question_config.question_ids) == 1:
                question_id = question_config.question_ids[0]
                if question_id not in question_id_to_engagement_db_message:
                    continue
                message_with_origin_details = question_id_to_engagement_db_message[question_id]
            else:
                list_of_messages_with_origin_details = []
                for question_id in question_config.question_ids:
                    if question_id not in question_id_to_engagement_db_message:
                        continue
                    list_of_messages_with_origin_details.append(question_id_to_engagement_db_message[question_id])

                if len(list_of_messages_with_origin_details) == 0:
                    continue
                elif len(list_of_messages_with_origin_details) == 1:
                    message_with_origin_details = list_of_messages_with_origin_details[0]
                else:
                    message_with_origin_details = _merge_engagement_db_messages(list_of_messages_with_origin_details, question_config.answers_delimeter)

            sync_event = _ensure_engagement_db_has_message(engagement_db, message_with_origin_details, dry_run)
            sync_stats.add_event(sync_event)

        if not dry_run and cache is not None:
            if i == len(responses) - 1 or \
                    isoparse(responses[i + 1]["lastSubmittedTime"]) > isoparse(response["lastSubmittedTime"]):
                cache.set_date_time(form_config.form_id, isoparse(response["lastSubmittedTime"]))

    return sync_stats


def _sync_google_form_source_to_engagement_db(google_cloud_credentials_file_path, form_source, engagement_db,
                                              uuid_table, cache=None, dry_run=False):
    """
    Syncs a Google Form source to an engagement database.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               download Google Form credentials.
    :type google_cloud_credentials_file_path: str
    :param form_source: Configuration for the Google Form to sync.
    :type form_source: src.google_form_to_engagement_db.configuration.GoogleFormSource
    :param engagement_db: Engagement database to sync
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param cache: Cache to use, or None. If None, downloads all form responses. If a cache is specified, only fetches
                  responses last submitted after this function was last run.
    :type cache: src.common.cache.Cache | None
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    :return: sync_stats
    :rtype: src.google_form_to_engagement_db.sync_stats.GoogleFormToEngagementDBSyncStats
    """
    google_form_client = form_source.google_form_client.init_google_forms_client(google_cloud_credentials_file_path)
    return _sync_google_form_to_engagement_db(google_form_client, engagement_db, form_source.sync_config, uuid_table, cache, dry_run)


def sync_google_form_sources_to_engagement_db(google_cloud_credentials_file_path, form_sources, engagement_db,
                                              uuid_table, cache_path=None, dry_run=False):
    """
    Syncs Google Forms to an engagement database.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               download Google Form credentials.
    :type google_cloud_credentials_file_path: str
    :param form_sources: Configuration for the Google Forms to sync.
    :type form_sources: list of src.google_form_to_engagement_db.configuration.GoogleFormSource
    :param engagement_db: Engagement database to sync
    :type engagement_db: engagement_database.EngagementDatabase
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str | None
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    """
    cache = None
    if cache_path is not None:
        cache = Cache(cache_path)

    form_id_to_sync_stats = OrderedDict()
    all_sync_stats = GoogleFormToEngagementDBSyncStats()
    for i, form_source in enumerate(form_sources):
        log.info(f"Processing form configuration {i + 1}/{len(form_sources)}...")
        form_id = form_source.sync_config.form_id
        sync_stats = _sync_google_form_source_to_engagement_db(
            google_cloud_credentials_file_path, form_source, engagement_db, uuid_table, cache, dry_run
        )
        form_id_to_sync_stats[form_id] = sync_stats
        all_sync_stats.add_stats(sync_stats)

    for form_id, sync_stats in form_id_to_sync_stats.items():
        log.info(f"Summary of actions for Google Form '{form_id}':")
        sync_stats.print_summary()

    log.info(f"Summary of actions for all Google Forms:")
    all_sync_stats.print_summary()
