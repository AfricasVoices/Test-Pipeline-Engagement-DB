from dataclasses import dataclass
from typing import Callable, Optional

from core_data_modules.data_models import CodeScheme


@dataclass
class CodeSchemeConfiguration:
    code_scheme: CodeScheme
    auto_coder: Optional[Callable[[str], str]]
    coda_code_schemes_count: Optional[int] = 3


@dataclass
class CodaDatasetConfiguration:
    coda_dataset_id: str
    engagement_db_dataset: str
    code_scheme_configurations: [CodeSchemeConfiguration]
    ws_code_string_value: str
    dataset_users_file_url: Optional[str] = None


@dataclass
class CodaSyncConfiguration:
    dataset_configurations: [CodaDatasetConfiguration]
    ws_correct_dataset_code_scheme: CodeScheme
    project_users_file_url: Optional[str] = None
    default_ws_dataset: Optional[str] = None  # Engagement db dataset to move messages to if there is no dataset
                                              # configuration for a particular ws_code_string_value. If None, crashes if
                                              # a message is found with a WS label with a string value not in
                                              # dataset_configurations. In most circumstances, this should be None as matching
                                              # cases where there are no datasets usually indicates a missing piece of configuration.

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
