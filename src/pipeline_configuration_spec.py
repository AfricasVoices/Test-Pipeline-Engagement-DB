from dataclasses import dataclass

from src.common.configuration import RapidProClientConfiguration, EngagementDatabaseClientConfiguration, \
    UUIDTableClientConfiguration, CodaClientConfiguration
from src.engagement_db_to_coda.configuration import CodaSyncConfiguration
from src.engagement_db_to_rapid_pro.configuration import EngagementDBToRapidProConfiguration
from src.rapid_pro_to_engagement_db.configuration import FlowResultConfiguration


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
    engagement_database: EngagementDatabaseClientConfiguration
    uuid_table: UUIDTableClientConfiguration
    rapid_pro_sources: [RapidProSource] = None
    coda_sync: CodaConfiguration = None
    rapid_pro_target: RapidProTarget = None
