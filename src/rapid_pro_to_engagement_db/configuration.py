from dataclasses import dataclass


@dataclass
class FlowResultConfiguration:
    flow_name: str
    flow_result_field: str
    engagement_db_dataset: str
