from dataclasses import dataclass

from src.common.configuration import EngagementDatabaseClientConfiguration, UUIDTableClientConfiguration, RapidProClientConfiguration
from src.rapid_pro_to_engagement_db.configuration import FlowResultConfiguration


@dataclass
class RapidProSource:
    rapid_pro: RapidProClientConfiguration
    flow_results: [FlowResultConfiguration]


@dataclass
class PipelineConfiguration:
    pipeline_name: str
    engagement_database: EngagementDatabaseClientConfiguration
    uuid_table: UUIDTableClientConfiguration
    rapid_pro_sources: [RapidProSource] = None
