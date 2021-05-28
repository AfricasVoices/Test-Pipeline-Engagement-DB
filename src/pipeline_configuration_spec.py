from dataclasses import dataclass

from src.common.configuration import EngagementDatabaseConfiguration, UUIDTableConfiguration, RapidProConfiguration
from src.rapid_pro_to_engagement_db.configuration import FlowResultConfiguration


@dataclass
class RapidProSource:
    rapid_pro: RapidProConfiguration
    flow_results: [FlowResultConfiguration]


@dataclass
class PipelineConfiguration:
    pipeline_name: str
    engagement_database: EngagementDatabaseConfiguration
    uuid_table: UUIDTableConfiguration
    rapid_pro_sources: [RapidProSource] = None
