from typing import Optional, Dict


class FlowResultConfiguration:
    """_summary_
    """

    def __init__(self, flow_name: str, flow_result_field: str, engagement_db_dataset: str) :
        self.flow_name = flow_name
        self.flow_result_field = flow_result_field
        self.engagement_db_dataset = engagement_db_dataset

    def to_dict(self) -> Dict[str, str]:
        return {
            "flow_name": self.flow_name,
            "flow_result_field": self.flow_result_field,
            "engagement_db_dataset": self.engagement_db_dataset
        }

    @classmethod
    def from_dict(cls, d: Dict[str, str]) -> FlowResultConfiguration:
        flow_name = d["flow_name"]
        flow_result_field = d["flow_result_field"]
        engagement_db_dataset = d["engagement_db_dataset"]

        return cls(flow_name, flow_result_field, engagement_db_dataset)


class UuidFilter:
    """_summary_
    """

    def __init__(self, uuid_file_url: str):
        self.uuid_file_url = uuid_file_url


class RapidProToEngagementDBConfiguration:
    """_summary_
    """

    def __init__(self, flow_result_configurations: [FlowResultConfiguration], uuid_filter: Optional[UuidFilter] = None):
        self.flow_result_configurations = flow_result_configurations
        self.uuid_filter = uuid_filter
