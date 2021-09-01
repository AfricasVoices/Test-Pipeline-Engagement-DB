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
class AnalysisLocations:
    KENYA_COUNTY = "kenya_county"
    KENYA_CONSTITUENCY = "kenya_constituency"


@dataclass
class CodingConfiguration:
    code_scheme: CodeScheme
    analysis_dataset: str
    age_category_config: Optional[AgeCategoryConfiguration] = None
    kenya_analysis_location: Optional[AnalysisLocations] = None


@dataclass
class AnalysisDatasetConfiguration:
    engagement_db_datasets: [str]
    dataset_type: DatasetTypes
    raw_dataset: str
    coding_configs: [CodingConfiguration]


@dataclass
class AnalysisConfiguration:
    dataset_configurations: [AnalysisDatasetConfiguration]
