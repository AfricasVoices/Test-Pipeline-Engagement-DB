from dataclasses import dataclass
from typing import Optional, Iterable

from core_data_modules.analysis.traffic_analysis import TrafficLabel
from core_data_modules.data_models import CodeScheme


@dataclass
class DatasetTypes:
    DEMOGRAPHIC = "demographic"
    RESEARCH_QUESTION_ANSWER = "research_question_answer"


@dataclass
class AgeCategoryConfiguration:
    age_analysis_dataset: str
    categories: dict


@dataclass
class AnalysisLocations:
    KENYA_COUNTY = "kenya_county"
    KENYA_CONSTITUENCY = "kenya_constituency"

    MOGADISHU_SUB_DISTRICT = "mogadishu_sub_district"
    SOMALIA_DISTRICT = "somalia_district"
    SOMALIA_REGION = "somalia_region"
    SOMALIA_STATE = "somalia_state"
    SOMALIA_ZONE = "somalia_zone"
    SOMALIA_OPERATOR = "somalia_operator"


@dataclass
class CodingConfiguration:
    code_scheme: CodeScheme
    analysis_dataset: str
    age_category_config: Optional[AgeCategoryConfiguration] = None
    analysis_location: Optional[AnalysisLocations] = None


@dataclass
class AnalysisDatasetConfiguration:
    engagement_db_datasets: [str]
    dataset_type: DatasetTypes
    raw_dataset: str
    coding_configs: [CodingConfiguration]


class OperatorDatasetConfiguration(AnalysisDatasetConfiguration):
    def __init__(self, raw_dataset: str, coding_configs: [CodingConfiguration]):
        super().__init__([], DatasetTypes.DEMOGRAPHIC, raw_dataset, coding_configs)


@dataclass
class GoogleDriveUploadConfiguration:
    credentials_file_url: str
    drive_dir: str


@dataclass
class MembershipGroupConfiguration:
    membership_group_csv_urls: {}


@dataclass
class AnalysisConfiguration:
    dataset_configurations: [AnalysisDatasetConfiguration]
    ws_correct_dataset_code_scheme: CodeScheme
    traffic_labels: Optional[Iterable[TrafficLabel]] = None
    google_drive_upload: Optional[GoogleDriveUploadConfiguration] = None
    membership_group_configuration: Optional[MembershipGroupConfiguration] = None
