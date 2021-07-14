from dataclasses import dataclass
from typing import Optional


@dataclass
class FlowResultConfiguration:
    flow_name: str
    flow_result_field: str
    engagement_db_dataset: str


@dataclass
class UuidFilter:
    uuid_file_url: str


@dataclass
class RapidProToEngagementDBConfiguration:
    flow_result_configurations: [FlowResultConfiguration]
    uuid_filter: Optional[UuidFilter] = None
