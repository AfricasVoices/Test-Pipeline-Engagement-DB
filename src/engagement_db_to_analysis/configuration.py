from dataclasses import dataclass

from core_data_modules.data_models import CodeScheme


@dataclass
class DatasetTypes:
    DEMOGRAPHIC = "demographic"
    RESEARCH_QUESTION_ANSWER = "research_question_answer"

@dataclass
class CodingConfiguration:
    code_scheme: CodeScheme
    analysis_file_key: str


@dataclass
class AnalysisDatasetConfiguration:
    engagement_db_datasets: [str]
    dataset_type: DatasetTypes
    analysis_dataset: str
    coding_configs: [CodingConfiguration]
