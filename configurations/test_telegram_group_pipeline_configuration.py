from core_data_modules.cleaners import swahili
from dateutil.parser import isoparse

from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="engagement-db-telegram-group-test",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/telegram_group_test"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_test",
        uuid_prefix="avf-participant-uuid-"
    ),
    operations_dashboard=OperationsDashboardConfiguration(
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
    ),
    telegram_group_sources=[
        TelegramGroupSource(
            token_file_url="gs://avf-credentials/dev-telegram-credentials.json",
            datasets=[
                TelegramGroupDataset(
                    engagement_db_dataset="test_telegram_group_s01e01",
                    search=TelegramGroupSearch(
                        start_date="2022-03-20T00:00+03:00",
                        end_date="2022-03-30T00:00+03:00",
                        group_ids=[1589865544]
                    )
                ),
                TelegramGroupDataset(
                    engagement_db_dataset="test_telegram_group_s01e02",
                    search=TelegramGroupSearch(
                        start_date="2022-03-30T00:00+03:00",
                        end_date="2022-04-06T00:00+03:00",
                        group_ids=[1589865544]
                    )
                ),
                TelegramGroupDataset(
                    engagement_db_dataset="test_telegram_group_s01e03",
                    search=TelegramGroupSearch(
                        start_date="2022-04-06T00:00+03:00",
                        end_date="2022-04-08T00:00+03:00",
                        group_ids=[1589865544]
                    )
                ),
            ],
        )
    ],
    rapid_pro_target=RapidProTarget(
        rapid_pro=RapidProClientConfiguration(
            domain="textit.com",
            token_file_url="gs://avf-credentials/wusc-leap-kalobeyei-textit-token.txt"  #For testing as other workspaces are suspended
        ),
        sync_config=EngagementDBToRapidProConfiguration(
            sync_channel_operator_dataset=DatasetConfiguration(
                engagement_db_datasets=["test_telegram_group_s01e01", "test_telegram_group_s01e02", "test_telegram_group_s01e03"],
                rapid_pro_contact_field=ContactField(key="channel_operator", label="channel operator"),
                create_new_contacts = True # Whether to create a new rapidpro contact e.g lg/fgd/telegram group contacts
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS,
            # allow_clearing_fields is set somewhat arbitrarily here because this data isn't being used in flows.
            # A pipeline that has continuous sync back in production will need to consider the options carefully.
            allow_clearing_fields=True,
            weekly_advert_contact_field=ContactField(key="test_pipeline_weekly_advert_contacts",
                                                     label="test pipeline weekly advert contacts"),
            sync_advert_contacts = True,
        )
    ),
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-staging.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_Telegram_group_s01e01",
                    engagement_db_dataset="test_telegram_s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("test_telegram_group_s01e01"), auto_coder=None,
                                                coda_code_schemes_count=3)
                    ],
                    ws_code_string_value="test_telegram_s01e01"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_Telegram_group_s01e02",
                    engagement_db_dataset="test_telegram_s01e02",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("test_telegram_group_s01e02"), auto_coder=None,
                                                coda_code_schemes_count=3)
                    ],
                    ws_code_string_value="test_telegram_s01e02"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/coda_users.json"
        )
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="pipeline_upload_test/telegram"
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["test_facebook_s01e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="test_telegram_group_s01e01_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("test_telegram_group_s01e01"),
                        analysis_dataset="test_telegram_group_s01e01"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["test_telegram_group_s01e02"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="test_telegram_group_s01e02_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("test_telegram_group_s01e02"),
                        analysis_dataset="test_telegram_group_s01e02"
                    )
                ]
            ),
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2021/TEST-PIPELINE_DB"
    )
)
