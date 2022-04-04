from core_data_modules.cleaners import swahili
from dateutil.parser import isoparse

from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="engagement-db-test",
    # TODO: store in messages and individuals_filter list of functions.
    project_start_date=isoparse("2021-03-01T10:30:00+03:00"),
    project_end_date=isoparse("2100-01-01T00:00:00+03:00"),
    test_participant_uuids=[
        "avf-participant-uuid-51c15546-58a0-4ab1-b465-e65b71462a8f"
    ],
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/experimental_test"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_test",
        uuid_prefix="avf-participant-uuid-"
    ),
    operations_dashboard=OperationsDashboardConfiguration(
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
    ),
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/experimental-test-text-it-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("test_pipeline_daniel_activation", "rqa_s01e01", "s01e01"),
                    FlowResultConfiguration("test_pipeline_daniel_demog", "constituency", "location"),
                    FlowResultConfiguration("test_pipeline_daniel_demog", "age", "age"),
                    FlowResultConfiguration("test_pipeline_daniel_demog", "gender", "gender"),
                ]
            )
        )
    ],
    csv_sources=[
        CSVSource(
            "gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/test_recovery.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("s01e01", end_date=isoparse("2021-12-31T24:00:00+03:00")),
                CSVDatasetConfiguration("age", start_date=isoparse("2022-01-01T00:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        )
    ],
    google_form_sources=[
        GoogleFormSource(
            google_form_client=GoogleFormsClientConfiguration(
                credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json"
            ),
            sync_config=GoogleFormToEngagementDBConfiguration(
                form_id="17q1yu1rb-gE9sdXnnRKPIAqGU27-uXm_xGVkfI5rudA",
                question_configurations=[
                    QuestionConfiguration(question_title="Test Question 1", engagement_db_dataset="s01e01"),
                    QuestionConfiguration(question_title="Test Question 2", engagement_db_dataset="s01e02")
                ]
            )
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
                        CodeSchemeConfiguration(
                            code_scheme=load_code_scheme("gender"),
                            auto_coder=swahili.DemographicCleaner.clean_gender
                        )
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
                    coda_dataset_id="TEST_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(
                            code_scheme=load_code_scheme("age"),
                            auto_coder=lambda x: str(swahili.DemographicCleaner.clean_age_within_range(x)),
                            coda_code_schemes_count=2
                        )
                    ],
                    ws_code_string_value="age",
                    dataset_users_file_url=f"gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/TEST_age_coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="TEST_s01e01",
                    engagement_db_dataset="s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("s01e01"), auto_coder=None, coda_code_schemes_count=3)
                    ],
                    ws_code_string_value="s01e01"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/coda_users.json"
        )
    ),
    rapid_pro_target=RapidProTarget(
        rapid_pro=RapidProClientConfiguration(
            domain="textit.com",
            token_file_url="gs://avf-credentials/experimental-sync-test-textit-token.txt"
        ),
        sync_config=EngagementDBToRapidProConfiguration(
            consent_withdrawn_dataset=DatasetConfiguration(
                engagement_db_datasets=["gender", "location", "age", "s01e01"],
                rapid_pro_contact_field=ContactField(key="engagement_db_consent_withdrawn", label="Engagement DB Consent Withdrawn")
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS,
            # allow_clearing_fields is set somewhat arbitrarily here because this data isn't being used in flows.
            # A pipeline that has continuous sync back in production will need to consider the options carefully.
            allow_clearing_fields=True
        )
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="pipeline_upload_test"
        ),
        membership_group_configuration=MembershipGroupConfiguration(
            membership_group_csv_urls={ "listening_group": [
                "gs://avf-project-datasets/2021/TEST-PIPELINE-ENGAGEMENT-DB/test-pipeline-engagement-db-listening-group.csv"
            ]
            },
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["s01e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01e01_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("s01e01"),
                        analysis_dataset="s01e01"
                    )
                ]
            ),
            OperatorDatasetConfiguration(
                raw_dataset="operator_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("operator"),
                        analysis_dataset="operator"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["gender"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="gender_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("gender"),
                        analysis_dataset="gender"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["location"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="location_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("kenya_county"),
                        analysis_dataset="kenya_county",
                        analysis_location=AnalysisLocations.KENYA_COUNTY
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("kenya_constituency"),
                        analysis_dataset="kenya_constituency",
                        analysis_location=AnalysisLocations.KENYA_CONSTITUENCY
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["age"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="age_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("age"),
                        analysis_dataset="age"
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("age_category"),
                        analysis_dataset="age_category",
                        age_category_config=AgeCategoryConfiguration(
                            age_analysis_dataset="age",
                            categories={
                                (10, 14): "10 to 14",
                                (15, 17): "15 to 17",
                                (18, 35): "18 to 35",
                                (36, 54): "36 to 54",
                                (55, 99): "55 to 99"
                            }
                        )
                    ),
                ],
            )
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
        traffic_labels=[
            TrafficLabel(isoparse("2021-04-01T00:00+03:00"), isoparse("2021-05-01T00:00+03:00"), "April"),
            TrafficLabel(isoparse("2021-05-01T00:00+03:00"), isoparse("2021-06-01T00:00+03:00"), "May")
        ]
    ),
    archive_configuration = ArchiveConfiguration(
        archive_upload_bucket = "gs://pipeline-execution-backup-archive",
        bucket_dir_path =  "2021/TEST-PIPELINE_DB"
    )
)
