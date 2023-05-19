import json

from core_data_modules.data_models import CodeScheme
from core_data_modules.analysis.traffic_analysis import TrafficLabel

from src.common.configuration import (RapidProClientConfiguration, CodaClientConfiguration, UUIDTableClientConfiguration,
                                      EngagementDatabaseClientConfiguration, OperationsDashboardConfiguration,
                                      ArchiveConfiguration)
from src.csv_to_engagement_db.configuration import (CSVSource, CSVDatasetConfiguration)

from src.engagement_db_coda_sync.configuration import (CodaSyncConfiguration, CodaDatasetConfiguration,
                                                       CodeSchemeConfiguration)
from src.engagement_db_to_rapid_pro.configuration import (EngagementDBToRapidProConfiguration, DatasetConfiguration,
                                                          WriteModes, ContactField)
from src.google_form_to_engagement_db.configuration import (GoogleFormToEngagementDBConfiguration,
                                                            GoogleFormsClientConfiguration,
                                                            ParticipantIdConfiguration, GoogleFormParticipantIdTypes,
                                                            QuestionConfiguration, GoogleFormSource)
from src.rapid_pro_to_engagement_db.configuration import (FlowResultConfiguration, UuidFilter,
                                                          RapidProToEngagementDBConfiguration)
from src.engagement_db_to_analysis.configuration import (AnalysisDatasetConfiguration, OperatorDatasetConfiguration,
                                                         DatasetTypes, AgeCategoryConfiguration, AnalysisLocations,
                                                         CodingConfiguration, GoogleDriveUploadConfiguration,
                                                         AnalysisDashboardUploadConfiguration, SeriesConfiguration,
                                                         MembershipGroupConfiguration, AnalysisConfiguration)
from src.facebook_to_engagement_db.configuration import (FacebookSource, FacebookDataset, FacebookSearch)
from src.telegram_to_engagement_db.configuration import (TelegramGroupSource, TelegramGroupDataset, TelegramGroupSearch)


def load_code_scheme(fname):
    """
    Loads a code scheme from the code_schemes folder.

    :param fname: Filename of the code scheme to load.
    :type fname: str
    :return: Loaded CodeScheme.
    :rtype: core_data_modules.data_models.CodeScheme
    """
    with open(f"code_schemes/{fname}.json") as f:
        return CodeScheme.from_firebase_map(json.load(f))


class RapidProSource:
    def __init__(self, rapid_pro, sync_config):
        """
        Configuration for a Rapid Pro Source. Configures which Rapid Pro workspace to fetch data from, and how this
        data should be processed and inserted into an engagement database.

        :param rapid_pro: Configuration for the Rapid Pro client to use to fetch the data from.
        :type rapid_pro: src.common.configuration.RapidProClientConfiguration
        :param sync_config: Configuration for the sync itself, e.g. which responses to sync to which datasets.
        :type sync_config: src.rapid_pro_to_engagement_db.configuration.RapidProToEngagementDBConfiguration
        """
        self.rapid_pro = rapid_pro
        self.sync_config = sync_config


class CodaConfiguration:
    def __init__(self, coda, sync_config):
        """
        Configuration for syncing between a Coda instance and an engagement database.

        This configuration is bidirectional i.e. both the engagement_db_to_coda and coda_to_engagement_db stages
        use the same configuration.

        :param coda: Configuration for the Coda instance to sync to/from.
        :type coda: src.common.configuration.CodaClientConfiguration
        :param sync_config: Configuration for which data to sync e.g. which engagement_db_datasets to which Coda
                            datasets.
        :type sync_config: src.engagement_db_coda_sync.configuration.CodaSyncConfiguration
        """
        self.coda = coda
        self.sync_config = sync_config


class RapidProTarget:
    def __init__(self, rapid_pro, sync_config):
        """
        Configuration for syncing from an engagement database to a Rapid Pro workspace.

        :param rapid_pro: Configuration for the Rapid Pro client to use to sync data from an engagement database to.
        :type rapid_pro: src.common.configuration.RapidProClientConfiguration
        :param sync_config: Configuration for the sync itself, e.g. which data to sync back to where in Rapid Pro.
        :type sync_config: src.engagement_db_to_rapid_pro.configuration.EngagementDBToRapidProConfiguration
        """
        self.rapid_pro = rapid_pro
        self.sync_config = sync_config


class PipelineConfiguration:
    def __init__(self, pipeline_name, engagement_database, uuid_table, operations_dashboard, archive_configuration,
                 description=None, project_start_date=None, project_end_date=None, test_participant_uuids=None,
                 rapid_pro_sources=None, facebook_sources=None, telegram_group_sources=None, csv_sources=None,
                 google_form_sources=None, coda_sync=None, rapid_pro_target=None, analysis=None):
        """
        Configuration for an Engagement-Data-Pipeline. An Engagement-Data-Pipeline is composed of a sequence of stages
        which each synchronise data, either from another tool to an engagement database, or from an engagement database
        to another tool.

        To configure a pipeline, provide the common, compulsory configuration, then entries for each of the stages
        needed by the pipeline. When the pipeline runs, each possible stage will load this configuration, then run a
        sync if a sync is specified, or quickly exit if not to allow the next stage to run.

        :param pipeline_name: The name to identify this pipeline configuration with, in places like logging outputs
                              and the OperationsDashboard.
        :type pipeline_name: str
        :param engagement_database: Configuration for the engagement database to use.
                                    This engagement database will be the one used by all stages in this pipeline.
        :type engagement_database: src.common.configuration.EngagementDatabaseClientConfiguration
        :param uuid_table: Configuration for the remote uuid table.
                           Stages that add messages to the engagement database will de-identify participant urns
                           into participant uuids using this table as early as possible.
                           Stages that need to sync participant urns back to another source will use this table
                           to re-identify participants as late as possible.
                           All other stages will only use participant uuids, and will not even load the uuid table.
        :type uuid_table: src.common.configuration.UUIDTableClientConfiguration
        :param operations_dashboard: Configuration for the OperationsDashboard. Pipeline start and end events will
                                     be logged to this dashboard.
        :type operations_dashboard: src.common.configuration.OperationsDashboardConfiguration
        :param archive_configuration: Configuration for uploading the pipeline run archives.
                                      Under normal operation, analysis outputs, traced data, and caches will be uploaded
                                      to a Google Cloud Storage bucket once per day.
        :type archive_configuration: src.common.configuration.ArchiveConfiguration
        :param description: Optional natural language description of what this configuration is for.
                            This field is not used by any pipeline stage, it exists solely to encourage good
                            documentation of pipeline configuration files.
        :type description: str | None
        :param project_start_date: Messages sent before this date will not be synced from Rapid Pro to the engagement
                                   database.
                                   TODO: Only the Rapid Pro -> DB stage uses this. Either move this configuration to
                                         that stage, or update the other stages to use this too.
        :type project_start_date: datetime.datetime | None
        :param project_end_date: Messages sent on or after this date will not be synced from Rapid Pro to the
                                 engagement database.
                                 TODO: Only the Rapid Pro -> DB stage uses this. Either move this configuration to
                                         that stage, or update the other stages to use this too.
        :type project_end_date: datetime.datetime | None
        :param test_participant_uuids: List of participant uuids that identify test users.
                                       These test users will be excluded from analysis.
                                       TODO: Only the DB -> analysis stage uses this. Either move this configuration to
                                             that stage, or update the other stages to use this too.
        :type test_participant_uuids: (list of str) | None
        :param rapid_pro_sources: Configuration for Rapid Pro -> engagement database syncs.
        :type rapid_pro_sources: (list of RapidProSource) | None
        :param facebook_sources: Configuration for Facebook -> engagement database syncs.
        :type facebook_sources: (list of src.facebook_to_engagement_db.configuration.FacebookSource) | None
        :param telegram_group_sources: Configuration for Telegram group -> engagement database syncs.
        :type telegram_group_sources: (list of src.telegram_to_engagement_db.configuration.TelegramGroupSource) | None
        :param csv_sources: Configuration for the CSV -> engagement db sync.
        :type csv_sources: (list of src.csv_to_engagement_db.configuration.CSVToEngagementDBSync) | None
        :param google_form_sources: Configuration for the Google Forms -> engagement db sync.
        :type google_form_sources: (list of src.google_form_to_engagement_db.configuration.GoogleFormSource) | None
        :param coda_sync: Configuration for the bidirectional Coda <-> engagement db sync.
        :type coda_sync: CodaConfiguration
        :param rapid_pro_target: Configuration for the engagement db -> Rapid Pro sync.
        :type rapid_pro_target: RapidProTarget
        :param analysis: Configuration for engagement db -> analysis.
        :type analysis: src.engagement_db_to_analysis.configuration.AnalysisConfiguration
        """
        self.pipeline_name = pipeline_name
        self.engagement_database = engagement_database
        self.uuid_table = uuid_table
        self.operations_dashboard = operations_dashboard
        self.archive_configuration = archive_configuration
        self.description = description
        self.project_start_date = project_start_date
        self.project_end_date = project_end_date
        self.test_participant_uuids = test_participant_uuids
        self.rapid_pro_sources = rapid_pro_sources
        self.facebook_sources = facebook_sources
        self.telegram_group_sources = telegram_group_sources
        self.csv_sources = csv_sources
        self.google_form_sources = google_form_sources
        self.coda_sync = coda_sync
        self.rapid_pro_target = rapid_pro_target
        self.analysis = analysis
