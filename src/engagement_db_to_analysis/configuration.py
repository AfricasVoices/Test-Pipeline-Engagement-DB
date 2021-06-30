from dataclasses import dataclass


@dataclass()
class DatasetTypes:
    DEMOGRAPHIC = "demographic"
    RESEARCH_QUESTION_ANSWER = "research_question_answer"


@dataclass
class AnalysisDatasetConfiguration:
    engagement_db_dataset: str
    dataset_type: DatasetTypes
