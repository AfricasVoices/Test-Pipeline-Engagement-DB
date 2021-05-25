import json
from dataclasses import dataclass
from typing import Callable, Optional

from core_data_modules.data_models import CodeScheme
from storage.google_cloud import google_cloud_utils
from coda_v2_python_client.firebase_client_wrapper import CodaV2Client


@dataclass
class CodaDatasetConfiguration:
    coda_dataset_id: str
    engagement_db_dataset: str
    code_scheme: CodeScheme
    auto_coder: Optional[Callable[[str], str]]
    ws_code_string_value: str


@dataclass
class CodaConfiguration:
    credentials_file_url: str
    dataset_configurations: [CodaDatasetConfiguration]
    ws_correct_dataset_code_scheme: CodeScheme

    def init_coda(self, google_cloud_credentials_file_path):
        # log.info("Initialising engagement database...")
        credentials = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path,
            self.credentials_file_url
        ))

        coda = CodaV2Client.init_client(
            credentials
        )
        # log.info("Initialised engagement database")

        return coda

    def get_dataset_config_by_engagement_db_dataset(self, dataset):
        for config in self.dataset_configurations:
            if config.engagement_db_dataset == dataset:
                return config
        raise ValueError(f"Coda configuration does not contain dataset '{dataset}'")

    def get_dataset_config_by_ws_code_string_value(self, string_value):
        for config in self.dataset_configurations:
            if config.ws_code_string_value == string_value:
                return config
        raise ValueError(f"Coda configuration does not contain ws_code string_value '{string_value}'")
