import json
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from core_data_modules.analysis.traffic_analysis import TrafficLabel
from core_data_modules.data_models import CodeScheme

from src.common.configuration import (ArchiveConfiguration, CodaClientConfiguration, EngagementDatabaseClientConfiguration,
                                      OperationsDashboardConfiguration, RapidProClientConfiguration, UUIDTableClientConfiguration)
from src.csv_to_engagement_db.configuration import CSVDatasetConfiguration, CSVSource
from src.engagement_db_coda_sync.configuration import CodaDatasetConfiguration, CodaSyncConfiguration, CodeSchemeConfiguration
from src.engagement_db_to_analysis.configuration import (AgeCategoryConfiguration, AnalysisConfiguration,
                                                         AnalysisDatasetConfiguration, AnalysisLocations, CodingConfiguration,
                                                         DatasetTypes, GoogleDriveUploadConfiguration, MembershipGroupConfiguration,
                                                         OperatorDatasetConfiguration)
from src.engagement_db_to_rapid_pro.configuration import (ContactField, DatasetConfiguration, EngagementDBToRapidProConfiguration,
                                                          WriteModes)
from src.facebook_to_engagement_db.configuration import FacebookDataset, FacebookSearch, FacebookSource
from src.google_form_to_engagement_db.configuration import (GoogleFormsClientConfiguration, GoogleFormSource, 
                                                            GoogleFormToEngagementDBConfiguration, QuestionConfiguration)
from src.rapid_pro_to_engagement_db.configuration import FlowResultConfiguration, RapidProToEngagementDBConfiguration, UuidFilter
from src.telegram_to_engagement_db.configuration import TelegramGroupDataset, TelegramGroupSearch, TelegramGroupSource


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
