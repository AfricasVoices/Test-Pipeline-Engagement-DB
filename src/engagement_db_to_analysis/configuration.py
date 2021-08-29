from dataclasses import dataclass
from typing import Optional

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
class KenyaAnalysisLocations:
    COUNTY = "county"
    CONSTITUENCY = "constituency"


@dataclass
class CodingConfiguration:
    code_scheme: CodeScheme
    analysis_dataset: str
    age_category_config: Optional[AgeCategoryConfiguration] = None
    kenya_analysis_location: Optional[KenyaAnalysisLocations] = None


@dataclass
class AnalysisDatasetConfiguration:
    engagement_db_datasets: [str]
    dataset_type: DatasetTypes
    raw_dataset: str
    coding_configs: [CodingConfiguration]


@dataclass
class GoogleDriveUploadConfiguration:
    credentials_file_url: str
    drive_dir: str


@dataclass
class AnalysisConfiguration:
    dataset_configurations: [AnalysisDatasetConfiguration]
    google_drive_upload: Optional[GoogleDriveUploadConfiguration] = None
