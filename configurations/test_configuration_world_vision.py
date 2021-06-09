import json

from core_data_modules.cleaners import swahili
from core_data_modules.data_models import CodeScheme

from src.common.configuration import RapidProClientConfiguration, CodaClientConfiguration, UUIDTableClientConfiguration, \
    EngagementDatabaseClientConfiguration
from src.engagement_db_to_coda.configuration import CodaSyncConfiguration, CodaDatasetConfiguration, \
    CodeSchemeConfiguration
from src.pipeline_configuration_spec import PipelineConfiguration, RapidProSource, CodaConfiguration
from src.rapid_pro_to_engagement_db.configuration import FlowResultConfiguration
from test.mock_uuid_table import MockUuidTableConfiguration


def load_code_scheme(fname):
    with open(f"code_schemes/{fname}.json") as f:
        return CodeScheme.from_firebase_map(json.load(f))


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="engagement-db-test",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/test_world_vision"
    ),
    uuid_table=MockUuidTableConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_test",
        uuid_prefix="avf-participant-uuid-"
    ),
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/world-vision-textit-token.txt"
            ),
            flow_results=[
                FlowResultConfiguration("worldvision_s01e01_activation", "rqa_s01e01", "world_vision_s01e01"),
            ]
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-staging.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldVision_s01e01",
                    engagement_db_dataset="world_vision_s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("world_vision_s01e01"), auto_coder=None)
                    ],
                    ws_code_string_value="s01e01"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset")
        )
    )
)
