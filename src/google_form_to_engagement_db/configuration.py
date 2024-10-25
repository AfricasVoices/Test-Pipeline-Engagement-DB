import json

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils

from src.google_form_to_engagement_db.google_forms_client import GoogleFormsClient

log = Logger(__name__)


class GoogleFormsClientConfiguration:
    def __init__(self, credentials_file_url):
        self.credentials_file_url = credentials_file_url

    def init_google_forms_client(self, google_cloud_credentials_file_path):
        log.info(f"Initialising Google Forms client with credentials file {self.credentials_file_url}...")
        form_credentials = json.loads(
            google_cloud_utils.download_blob_to_string(google_cloud_credentials_file_path, self.credentials_file_url)
        )
        google_forms_client = GoogleFormsClient(form_credentials)
        log.info("Initialised Google Forms client")

        return google_forms_client


class GoogleFormParticipantIdTypes:
    # TODO: Consider moving this to core
    KENYA_MOBILE_NUMBER = "kenya_mobile_number"


class ParticipantIdConfiguration:
    def __init__(self, question_id, id_type):
        """
        Configuration for a participant uuid question.

        :param question_id: Question id. This is the id for this question
        :type question_id: str
        :param id_type: See `GoogleFormParticipantIdTypes`.
        :type id_type: str
        """
        self.question_id = question_id
        self.id_type = id_type


class QuestionConfiguration:
    def __init__(self, question_ids, engagement_db_dataset, answers_delimeter="; "):
        """
        :param question_ids: Question ids.
        :type question_ids: list of str
        :param engagement_db_dataset: Name of the dataset to use in the engagement database.
        :type engagement_db_dataset: str
        :param answers_delimeter: a character for specifying the boundary between the answers given for multiple
                                `question_ids` 
        :type answers_delimeter: str
        """
        self.question_ids = question_ids
        self.engagement_db_dataset = engagement_db_dataset
        self.answers_delimeter = answers_delimeter


class GoogleFormToEngagementDBConfiguration:
    def __init__(self, form_id, question_configurations, participant_id_configuration=None, ignore_invalid_mobile_numbers=False):
        """
        :param form_id: Id of Google Form to sync.
        :type form_id: str
        :param question_configurations: Configuration for each question on the Google Form to sync.
        :type question_configurations: list of QuestionConfiguration
        :param participant_id_configuration: Optional configuration for the participant uuid.
                                               If set, the participant uuid will be derived from the answer to an
                                               id question, otherwise it will be set to the form response id.
        :type participant_id_configuration: ParticipantIdConfiguration | None
        ignore_invalid_mobile_numbers: bool = False
        ignore_invalid_mobile_numbers: Whether to ignore invalid mobile numbers during validation.
                                    If a participant provides an invalid mobile number, instead of the pipeline terminating with a valueError
                                    the participant uuid will be derived from the form response id.                               
        """
        self.form_id = form_id
        self.question_configurations = question_configurations
        self.participant_id_configuration = participant_id_configuration
        self.ignore_invalid_mobile_numbers = ignore_invalid_mobile_numbers

        if participant_id_configuration is not None and participant_id_configuration.id_type not in \
            [GoogleFormParticipantIdTypes.KENYA_MOBILE_NUMBER]:
            assert ignore_invalid_mobile_numbers == False, f"`ignore_invalid_mobile_numbers` cannot be set to True " \
                f"if participant id type is {participant_id_configuration.id_type}. See `GoogleFormToEngagementDBConfiguration`"


class GoogleFormSource:
    def __init__(self, google_form_client, sync_config):
        """
        :param google_form_client: Google Form client configuration
        :type google_form_client: GoogleFormsClientConfiguration
        :param sync_config: Sync configuration
        :type sync_config: GoogleFormToEngagementDBConfiguration
        """
        self.google_form_client = google_form_client
        self.sync_config = sync_config
