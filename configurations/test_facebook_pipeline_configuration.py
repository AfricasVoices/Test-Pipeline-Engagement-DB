from core_data_modules.cleaners import swahili
from dateutil.parser import isoparse

from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="engagement-db-facebook-test",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/experimental_test"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_test",
        uuid_prefix="avf-facebook-uuid-"
    ),
    operations_dashboard=OperationsDashboardConfiguration(
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
    ),
    facebook_sources=[
        FacebookSource(
            page_id="AbdirizakHAtosh",
            token_file_url="gs://avf-credentials/AbdirizakHAtosh-facebook-token.txt",
            datasets= [
                FacebookDataset(
                    engagement_db_dataset="test_facebook_s01e01",
                    search=FacebookSearch(
                        match="#doorashoNabadeed",
                        start_date="2021-09-22T00:00+03:00",
                        end_date="2021-09-24T00:00+03:00"
                    )
                ),
                FacebookDataset(
                    engagement_db_dataset="test_facebook_s01e02",
                    search=FacebookSearch(
                        match="#doorashoNabadeed",
                        start_date="2021-09-24T00:00+03:00",
                        end_date="2021-09-26T00:00+03:00"
                    )
                )
            ],
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-staging.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_Facebook_s01e01",
                    engagement_db_dataset="test_facebook_s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("test_facebook_s01e01"), auto_coder=None,
                                                coda_code_schemes_count=3)
                    ],
                    ws_code_string_value="test_facebook_s01e01"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_Facebook_s01e02",
                    engagement_db_dataset="test_facebook_s01e02",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("test_facebook_s01e02"), auto_coder=None,
                                                coda_code_schemes_count=3)
                    ],
                    ws_code_string_value="test_facebook_s01e02"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/coda_users.json"
        )
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="pipeline_upload_test/facebook"
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["test_facebook_s01e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="test_facebook_s01e01_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("test_facebook_s01e01"),
                        analysis_dataset="test_facebook_s01e01"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["test_facebook_s01e02"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="test_facebook_s01e02_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("test_facebook_s01e02"),
                        analysis_dataset="test_facebook_s01e02"
                    )
                ]
            ),
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
    ),
    archive_configuration = ArchiveConfiguration(
        archive_upload_bucket = "gs://pipeline-execution-backup-archive",
        bucket_dir_path =  "2021/TEST-PIPELINE_DB"
    )
)
