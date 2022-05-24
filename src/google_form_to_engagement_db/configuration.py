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
        rapid_pro_client = GoogleFormsClient(form_credentials)
        log.info("Initialised Google Forms client")

        return rapid_pro_client


class GoogleFormUuidTypes:
    KENYA_MOBILE_NUMBER = "kenya_mobile_number"


class ParticipantUuidConfiguration:
    def __init__(self, question_title, uuid_type):
        """
        Configuration for a participant uuid question.

        :param question_title: Question title. This is the text presented to the form user for this question
                               e.g. "What is your phone number?"
        :type question_title: str
        :param uuid_type: See `GoogleFormUuidTypes`.
        :type uuid_type: str
        """
        self.question_title = question_title
        self.uuid_type = uuid_type


class QuestionConfiguration:
    def __init__(self, question_title, engagement_db_dataset):
        """
        :param question_title: Question title. This is the text presented to the form user for this question
                               e.g. "Do you live in a town/city?"
        :type question_title: str
        :param engagement_db_dataset: Name of the dataset to use in the engagement database.
        :type engagement_db_dataset: str
        """
        self.question_title = question_title
        self.engagement_db_dataset = engagement_db_dataset


class GoogleFormToEngagementDBConfiguration:
    def __init__(self, form_id, question_configurations, participant_uuid_configuration=None):
        """
        :param form_id: Id of Google Form to sync.
        :type form_id: str
        :param question_configurations: Configuration for each question on the Google Form to sync.
        :type question_configurations: list of QuestionConfiguration
        :param participant_uuid_configuration: Optional configuration for the participant uuid.
                                               If set, the participant uuid will be derived from the answer to an
                                               id question, otherwise it will be set to the form response id.
        :type participant_uuid_configuration: ParticipantUuidConfiguration | None
        """
        self.form_id = form_id
        self.question_configurations = question_configurations
        self.participant_uuid_configuration = participant_uuid_configuration


class GoogleFormSource:
    def __init__(self, google_form_client, sync_config):
        """
        :param google_form_client: Google Form client configuration
        :type google_form_client: GoogleFormsClientConfiguration
        :param sync_config: Sync configuration.s
        :type sync_config: GoogleFormToEngagementDBConfiguration
        """
        self.google_form_client = google_form_client
        self.sync_config = sync_config
