from dataclasses import dataclass


@dataclass
class FlowResultConfiguration:
    flow_name: str
    flow_result_field: str
    engagement_db_dataset: str


@dataclass
class RapidProToEngagementDBConfiguration:
    domain: str
    token_file_url: str
    flow_result_configurations: [FlowResultConfiguration]
