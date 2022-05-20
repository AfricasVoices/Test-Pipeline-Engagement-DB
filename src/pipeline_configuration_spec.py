import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

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
                                                            ParticipantUuidConfiguration,
                                                            QuestionConfiguration, GoogleFormSource)
from src.rapid_pro_to_engagement_db.configuration import (FlowResultConfiguration, UuidFilter,
                                                          RapidProToEngagementDBConfiguration)
from src.engagement_db_to_analysis.configuration import (AnalysisDatasetConfiguration, OperatorDatasetConfiguration,
                                                         DatasetTypes, AgeCategoryConfiguration, AnalysisLocations,
                                                         CodingConfiguration, GoogleDriveUploadConfiguration,
                                                         MembershipGroupConfiguration, AnalysisConfiguration)
from src.facebook_to_engagement_db.configuration import (FacebookSource, FacebookDataset, FacebookSearch)
from src.telegram_to_engagement_db.configuration import (TelegramGroupSource, TelegramGroupDataset, TelegramGroupSearch)


def load_code_scheme(fname):
    with open(f"code_schemes/{fname}.json") as f:
        return CodeScheme.from_firebase_map(json.load(f))


@dataclass
class RapidProSource:
    rapid_pro: RapidProClientConfiguration
    sync_config: RapidProToEngagementDBConfiguration


@dataclass
class CodaConfiguration:
    coda: CodaClientConfiguration
    sync_config: CodaSyncConfiguration


@dataclass
class RapidProTarget:
    rapid_pro: RapidProClientConfiguration
    sync_config: EngagementDBToRapidProConfiguration


@dataclass
class PipelineConfiguration:
    pipeline_name: str

    engagement_database: EngagementDatabaseClientConfiguration
    uuid_table: UUIDTableClientConfiguration
    operations_dashboard: OperationsDashboardConfiguration
    archive_configuration: ArchiveConfiguration
    project_start_date: datetime = None
    project_end_date: datetime = None
    test_participant_uuids: [] = None
    description: str = None
    rapid_pro_sources: [RapidProSource] = None
    facebook_sources: Optional[List[FacebookSource]] = None
    telegram_group_sources: Optional[List[TelegramGroupSource]] = None
    csv_sources: Optional[List[CSVSource]] = None
    google_form_sources: Optional[List[GoogleFormSource]] = None
    coda_sync: CodaConfiguration = None
    rapid_pro_target: RapidProTarget = None
    analysis: AnalysisConfiguration = None
