import json

from core_data_modules.cleaners import swahili
from core_data_modules.data_models import CodeScheme

from src.common.configuration import RapidProClientConfiguration, CodaClientConfiguration, UUIDTableClientConfiguration, \
    EngagementDatabaseClientConfiguration
from src.engagement_db_to_coda.configuration import CodaSyncConfiguration, CodaDatasetConfiguration, \
    CodeSchemeConfiguration
from src.engagement_db_to_rapid_pro.configuration import EngagementDBToRapidProConfiguration, DatasetConfiguration, \
    WriteModes
from src.pipeline_configuration_spec import PipelineConfiguration, RapidProSource, CodaConfiguration, RapidProTarget
from src.rapid_pro_to_engagement_db.configuration import FlowResultConfiguration


def load_code_scheme(fname):
    with open(f"code_schemes/{fname}.json") as f:
        return CodeScheme.from_firebase_map(json.load(f))


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="engagement-db-test",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/experimental_test"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_test",
        uuid_prefix="avf-participant-uuid-"
    ),
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/experimental-test-text-it-token.txt"
            ),
            flow_results=[
                FlowResultConfiguration("test_pipeline_daniel_activation", "rqa_s01e01", "s01e01"),
                FlowResultConfiguration("test_pipeline_daniel_demog", "constituency", "location"),
                FlowResultConfiguration("test_pipeline_daniel_demog", "age", "age"),
                FlowResultConfiguration("test_pipeline_daniel_demog", "gender", "gender"),
            ]
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-staging.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_gender",
                    engagement_db_dataset="gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("gender"), auto_coder=swahili.DemographicCleaner.clean_gender)
                    ],
                    ws_code_string_value="gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_location",
                    engagement_db_dataset="location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("kenya_constituency"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("kenya_county"), auto_coder=None)
                    ],
                    ws_code_string_value="location"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_s01e01",
                    engagement_db_dataset="s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("s01e01"), auto_coder=None)
                    ],
                    ws_code_string_value="s01e01"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset")
        )
    ),
    rapid_pro_target=RapidProTarget(
        rapid_pro=RapidProClientConfiguration(
            domain="textit.com",
            token_file_url="gs://avf-credentials/experimental-sync-test-textit-token.txt"
        ),
        sync_config=EngagementDBToRapidProConfiguration(
            # Note this performs continuous sync of all datasets for the purpose of testing.
            # In practice, we'd only want to continuously sync consent_withdrawn.
            normal_datasets=[
                DatasetConfiguration(engagement_db_datasets=["gender"], rapid_pro_contact_field="gender"),
                DatasetConfiguration(engagement_db_datasets=["location"], rapid_pro_contact_field="location"),
                DatasetConfiguration(engagement_db_datasets=["age"], rapid_pro_contact_field="age"),
                DatasetConfiguration(engagement_db_datasets=["s01e01"], rapid_pro_contact_field="s01e01")
            ],
            write_mode=WriteModes.SHOW_PRESENCE
        )
    )
)
