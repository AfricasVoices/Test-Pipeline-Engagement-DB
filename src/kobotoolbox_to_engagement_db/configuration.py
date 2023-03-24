import json

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils


log = Logger(__name__)


class KoboToolBoxParticipantIdTypes:
    # TODO: Consider moving this to core
    KENYA_MOBILE_NUMBER = "kenya_mobile_number"


class KoboToolBoxParticipantIdConfiguration:
    def __init__(self, data_column_name, id_type):
        """
        Configuration for a participant uuid question.

        :param data_column_name:  This is the variable name that stores to the form user for this question
                               e.g. "What is your phone number?"
        :type data_column_name: str
        :param id_type: See `KoboToolBoxParticipantIdTypes`.
        :type id_type: str
        """
        self.data_column_name = data_column_name
        self.id_type = id_type


class KoboToolBoxQuestionConfiguration:
    def __init__(self, data_column_name, engagement_db_dataset):
        """
        :param data_column_name: This are the variable names that store response for a question.
        :type data_column_name: str
        :param engagement_db_dataset: Name of the dataset to use in the engagement database.
        :type engagement_db_dataset: str
        """
        self.data_column_name = data_column_name
        self.engagement_db_dataset = engagement_db_dataset


class KoboToolBoxToEngagementDBConfiguration:
    def __init__(self, asset_uid, question_configurations, participant_id_configuration=None, ignore_invalid_mobile_numbers=False):
        """
        :param asset_uid: Id of KobotoolBox Form to sync.
        :type asset_uid: str
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
        self.asset_uid = asset_uid
        self.question_configurations = question_configurations
        self.participant_id_configuration = participant_id_configuration
        self.ignore_invalid_mobile_numbers = ignore_invalid_mobile_numbers

        if participant_id_configuration is not None and participant_id_configuration.id_type not in \
            [KoboToolBoxParticipantIdTypes.KENYA_MOBILE_NUMBER]:
            assert ignore_invalid_mobile_numbers == False, f"`ignore_invalid_mobile_numbers` cannot be set to True " \
                f"if participant id type is {participant_id_configuration.id_type}. See `KoboToolBoxToEngagementDBConfiguration`"


class KoboToolBoxSource:
    def __init__(self, token_file_url, sync_config):
        """
        :param token_file_url: GS url path to the kobotoolbox api token file
        :type token_file_url: GS url
        :param sync_config: Sync configuration
        :type sync_config: KoboToolBoxToEngagementDBConfiguration
        """
        self.token_file_url = token_file_url
        self.sync_config = sync_config
