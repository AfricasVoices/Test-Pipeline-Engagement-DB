from dataclasses import dataclass
from typing import Callable, Optional

from core_data_modules.data_models import CodeScheme


@dataclass
class CodeSchemeConfiguration:
    code_scheme: CodeScheme
    auto_coder: Optional[Callable[[str], str]]


@dataclass
class CodaDatasetConfiguration:
    coda_dataset_id: str
    engagement_db_dataset: str
    code_scheme_configurations: [CodeSchemeConfiguration]
    ws_code_string_value: str


@dataclass
class CodaSyncConfiguration:
    dataset_configurations: [CodaDatasetConfiguration]
    ws_correct_dataset_code_scheme: CodeScheme

    def get_dataset_config_by_engagement_db_dataset(self, dataset):
        for config in self.dataset_configurations:
            if config.engagement_db_dataset == dataset:
                return config
        raise ValueError(f"Coda configuration does not contain a dataset_configuration with dataset '{dataset}'")

    def get_dataset_config_by_ws_code_string_value(self, string_value):
        for config in self.dataset_configurations:
            if config.ws_code_string_value == string_value:
                return config
        raise ValueError(f"Coda configuration does not contain a dateset_configuration with ws_code_string_value "
                         f"'{string_value}'")
