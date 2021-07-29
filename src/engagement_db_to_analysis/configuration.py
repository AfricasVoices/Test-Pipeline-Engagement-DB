from dataclasses import dataclass
from typing import Optional

from core_data_modules.data_models import CodeScheme


@dataclass
class DatasetTypes:
    DEMOGRAPHIC = "demographic"
    RESEARCH_QUESTION_ANSWER = "research_question_answer"

@dataclass
class CodingConfiguration:
    code_scheme: CodeScheme
    analysis_dataset: str
    age_categories: Optional[dict] = None

@dataclass
class AnalysisDatasetConfiguration:
    engagement_db_datasets: [str]
    dataset_type: DatasetTypes
    raw_dataset: str
    coding_configs: [CodingConfiguration]

@dataclass
class AnalysisConfiguration:
    dataset_configurations: [AnalysisDatasetConfiguration]
