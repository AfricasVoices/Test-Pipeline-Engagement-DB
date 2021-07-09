import json
from dataclasses import dataclass
from datetime import datetime

from core_data_modules.data_models import CodeScheme

from src.common.configuration import (RapidProClientConfiguration, CodaClientConfiguration, UUIDTableClientConfiguration,
                                      EngagementDatabaseClientConfiguration)
from src.engagement_db_coda_sync.configuration import (CodaSyncConfiguration, CodaDatasetConfiguration,
                                                       CodeSchemeConfiguration)
from src.engagement_db_to_rapid_pro.configuration import (EngagementDBToRapidProConfiguration, DatasetConfiguration,
                                                          WriteModes, ContactField)
from src.rapid_pro_to_engagement_db.configuration import FlowResultConfiguration

from src.engagement_db_to_analysis.configuration import AnalysisDatasetConfiguration, DatasetTypes


def load_code_scheme(fname):
    with open(f"code_schemes/{fname}.json") as f:
        return CodeScheme.from_firebase_map(json.load(f))


@dataclass
class RapidProSource:
    rapid_pro: RapidProClientConfiguration
    flow_results: [FlowResultConfiguration]


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
    project_start_date: datetime
    project_end_date: datetime
    filter_test_messages: bool
    engagement_database: EngagementDatabaseClientConfiguration
    uuid_table: UUIDTableClientConfiguration
    description: str = None
    rapid_pro_sources: [RapidProSource] = None
    coda_sync: CodaConfiguration = None
    rapid_pro_target: RapidProTarget = None
    analysis_config: [AnalysisDatasetConfiguration] = None
