from collections import OrderedDict

from core_data_modules.logging import Logger
from dateutil.parser import isoparse
from engagement_database.data_models import (Message, MessageDirections, MessageStatuses, MessageOrigin,
                                             HistoryEntryOrigin)

from src.common.cache import Cache
from src.google_form_to_engagement_db.sync_stats import GoogleFormToEngagementDBSyncStats, GoogleFormSyncEvents

log = Logger(__name__)


def _validate_configuration_against_form_structure(form, form_config):
    """
    Validates a Google Form dictionary against a form configuration.

    Fails with an AssertionError if there are duplicated questions, in either the form or in the configuration.
    Fails with an AssertionError if there are any questions requested by the configuration which aren't available
    in the form.
    Logs a warning if there are any questions asked in the form which aren't requested by the configuration.

    :param form: The Google Form to be validated, in Google Forms' form dictionary format.
    :type form: dict
    :param form_config: Configuration to use for the validation.
    :type form_config: src.google_form_to_engagement_db.configuration.GoogleFormToEngagementDBConfiguration
    """
    form_questions = set()
    for item in form["items"]:
        title = item["title"]
        assert title not in form_questions, f"Question '{title}' specified in form {form['formId']} twice"
        form_questions.add(title)

    config_questions = set()
    for question_config in form_config.question_configurations:
        assert question_config.question_title not in config_questions, \
            f"Question '{question_config.question_title} specified in configuration for form {form_config.form_id} twice"
        config_questions.add(question_config.question_title)

    # Ensure that all questions requested in the configuration exist in the form.
    config_questions_not_in_form = config_questions - form_questions
    assert len(config_questions_not_in_form) == 0,\
        f"Some questions requested in the configuration do not exist in form " \
        f"{form_config.form_id}: {config_questions_not_in_form}"

    # Check if there were any questions in the form that do not exist in the configuration.
    # Warn about these cases, but don't fail because it's possible not all questions asked are to be analysed.
    form_questions_not_in_config = form_questions - config_questions
    if len(form_questions_not_in_config) != 0:
        log.warning(f"Found some questions in the form that aren't set in the configuration: "
                    f"{form_questions_not_in_config}")


def _form_answer_to_engagement_db_message(form_answer, form_id, form_response, question_id_to_engagement_db_dataset):
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
    # Validate structure of free text response
    # TODO: Handle other types of questions too
    free_text_answers = form_answer["textAnswers"]["answers"]
    assert len(free_text_answers) == 1, len(free_text_answers)
    free_text_answer = free_text_answers[0]["value"]

    return Message(
        # TODO: participant_uuid here is set simply to the response id for now.
        #       If we want to connect responses to a means of communicating back to someone, then this would need
        #       to be updated to use the uuid table.
        participant_uuid=form_response["responseId"],
        text=free_text_answer,
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


def _ensure_engagement_db_has_message(engagement_db, message, message_origin_details):
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
    :return: Sync event.
    :rtype: str
    """
    if _engagement_db_has_message(engagement_db, message):
        log.debug(f"Message already in engagement database")
        return GoogleFormSyncEvents.MESSAGE_ALREADY_IN_ENGAGEMENT_DB

    log.debug(f"Adding message to engagement database dataset {message.dataset}...")
    engagement_db.set_message(
        message,
        HistoryEntryOrigin(origin_name="Google Form -> Database Sync", details=message_origin_details)
    )
    return GoogleFormSyncEvents.ADD_MESSAGE_TO_ENGAGEMENT_DB


def _sync_google_form_to_engagement_db(google_form_client, engagement_db, form_config, cache=None):
    """
    Syncs a Google Form to an engagement database.

    Adds an engagement database message for every answer to a question specified in the form_config.

    :param google_form_client: Google forms client to use to download the form and responses.
    :type google_form_client: src.google_form_to_engagement_db.google_forms_client.GoogleFormsClient
    :param engagement_db: Engagement database to sync the Google Form to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param form_config: Configuration for the form to sync.
    :type form_config: src.google_form_to_engagement_db.configuration.GoogleFormToEngagementDBConfiguration
    :param cache: Cache to use, or None. If None, downloads all form responses. If a cache is specified, only fetches
                  responses last submitted after this function was last run.
    :type cache: src.common.cache.Cache
    :return: sync_stats
    :rtype: src.google_form_to_engagement_db.sync_stats.GoogleFormToEngagementDBSyncStats
    """
    log.info(f"Downloading structure of form {form_config.form_id}...")
    form = google_form_client.get_form(form_config.form_id)

    log.info(f"Validating question configurations...")
    _validate_configuration_against_form_structure(form, form_config)

    log.info("Linking question ids to the form configuration...")
    question_title_to_engagement_db_dataset = dict()
    for question_config in form_config.question_configurations:
        question_title_to_engagement_db_dataset[question_config.question_title] = question_config.engagement_db_dataset

    question_id_to_engagement_db_dataset = dict()
    for item in form["items"]:
        question_id = item["questionItem"]["question"]["questionId"]
        question_title = item["title"]
        if question_title not in question_title_to_engagement_db_dataset:
            continue
        engagement_db_dataset = question_title_to_engagement_db_dataset[question_title]

        question_id_to_engagement_db_dataset[question_id] = engagement_db_dataset

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
        log.info(f"Processing response {i + 1}/{len(responses)}...")
        sync_stats.add_event(GoogleFormSyncEvents.READ_RESPONSE_FROM_GOOGLE_FORM)
        answers = response["answers"].values()
        for j, answer in enumerate(answers):
            log.info(f"Processing answer {j + 1}/{len(answers)} for response {i + 1}/{len(responses)}...")
            sync_stats.add_event(GoogleFormSyncEvents.READ_ANSWER_FROM_RESPONSE)
            if answer["questionId"] not in question_id_to_engagement_db_dataset:
                log.info(f"This answer is to question {answer['questionId']}, which isn't configured in this sync")
                continue

            message = _form_answer_to_engagement_db_message(
                answer, form_config.form_id, response, question_id_to_engagement_db_dataset
            )
            message_origin_details = {
                "formId": form_config.form_id,
                "answer": answer,
            }
            sync_event = _ensure_engagement_db_has_message(engagement_db, message, message_origin_details)
            sync_stats.add_event(sync_event)

        if cache is not None:
            if i == len(responses) - 1 or \
                    isoparse(responses[i + 1]["lastSubmittedTime"]) > isoparse(response["lastSubmittedTime"]):
                cache.set_date_time(form_config.form_id, isoparse(response["lastSubmittedTime"]))

    return sync_stats


def _sync_google_form_source_to_engagement_db(google_cloud_credentials_file_path, form_source, engagement_db,
                                              cache=None):
    """
    Syncs a Google Form source to an engagement database.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               download Google Form credentials.
    :type google_cloud_credentials_file_path: str
    :param form_source: Configuration for the Google Form to sync.
    :type form_source: src.google_form_to_engagement_db.configuration.GoogleFormSource
    :param engagement_db: Engagement database to sync
    :type engagement_db: engagement_database.EngagementDatabase
    :param cache: Cache to use, or None. If None, downloads all form responses. If a cache is specified, only fetches
                  responses last submitted after this function was last run.
    :type cache: src.common.cache.Cache
    :return: sync_stats
    :rtype: src.google_form_to_engagement_db.sync_stats.GoogleFormToEngagementDBSyncStats
    """
    google_form_client = form_source.google_form_client.init_google_forms_client(google_cloud_credentials_file_path)
    return _sync_google_form_to_engagement_db(google_form_client, engagement_db, form_source.sync_config, cache)


def sync_google_form_sources_to_engagement_db(google_cloud_credentials_file_path, form_sources, engagement_db,
                                              cache_path=None):
    """
    Syncs Google Forms to an engagement database.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               download Google Form credentials.
    :type google_cloud_credentials_file_path: str
    :param form_sources: Configuration for the Google Forms to sync.
    :type form_sources: list of src.google_form_to_engagement_db.configuration.GoogleFormSource
    :param engagement_db: Engagement database to sync
    :type engagement_db: engagement_database.EngagementDatabase
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str | None
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
            google_cloud_credentials_file_path, form_source, engagement_db, cache
        )
        form_id_to_sync_stats[form_id] = sync_stats
        all_sync_stats.add_stats(sync_stats)

    for form_id, sync_stats in form_id_to_sync_stats.items():
        log.info(f"Summary of actions for Google Form '{form_id}':")
        sync_stats.print_summary()

    log.info(f"Summary of actions for all Google Forms:")
    all_sync_stats.print_summary()
