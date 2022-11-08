from core_data_modules.cleaners import swahili

from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="GIZ",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-engagement-databases-firebase-credentials-file.json",
        database_path="engagement_databases/POOL-KENYA"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-id-infrastructure-firebase-adminsdk-6xps8-b9173f2bfd.json",
        table_name="avf-global-urn-to-participant-uuid",
        uuid_prefix="avf-participant-uuid-"
    ),
    operations_dashboard=OperationsDashboardConfiguration(
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
    ),
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/pool-kenya-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("giz_s01_demogs", "pool_kenya_location", "location"),
                    FlowResultConfiguration("giz_s01_demogs", "pool_kenya_gender", "gender"),
                    FlowResultConfiguration("giz_s01_demogs", "pool_kenya_age", "age"),
                    FlowResultConfiguration("giz_s01_demogs", "pool_kenya_disabled", "disabled"),

                    FlowResultConfiguration("giz_s01e01_activation",
                                            "rqa_s01e01", "giz_s01e01")
                ],
            )
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="Kenya_Pool_location",
                    engagement_db_dataset="location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/kenya_ward"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/kenya_constituency"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/kenya_county"), auto_coder=None)
                    ],
                    ws_code_match_value="location"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="Kenya_Pool_gender",
                    engagement_db_dataset="gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/gender"),
                                                auto_coder=swahili.DemographicCleaner.clean_gender)
                    ],
                    ws_code_match_value="gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="Kenya_Pool_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/age"), auto_coder=lambda x:
                                                str(swahili.DemographicCleaner.clean_age_within_range(x)))
                    ],
                    ws_code_match_value="age"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="Kenya_Pool_disabled",
                    engagement_db_dataset="disabled",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/disabled"), auto_coder=None)
                    ],
                    ws_code_match_value="disabled"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="GIZ_s01e01",
                    engagement_db_dataset="giz_s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/giz/s01e01"), auto_coder=None)
                    ],
                    ws_code_match_value="giz_s01e01"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2022/POOL-KENYA/pool-kenya-users.json",
            default_ws_dataset="kenya_pool_old_rqa_datasets"
        )
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="giz_analysis_output"
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["giz_s01e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01e01_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/giz/s01e01"),
                        analysis_dataset="s01e01"
                    )
                ],
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["gender"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="gender_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/gender"),
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
                         code_scheme=load_code_scheme("demographics/kenya_ward"),
                         analysis_dataset="kenya_ward",
                         analysis_location=AnalysisLocations.KENYA_WARD

                     ),
                     CodingConfiguration(
                         code_scheme=load_code_scheme("demographics/kenya_county"),
                         analysis_dataset="kenya_county",
                         analysis_location=AnalysisLocations.KENYA_COUNTY
                     ),
                     CodingConfiguration(
                         code_scheme=load_code_scheme("demographics/kenya_constituency"),
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
                        code_scheme=load_code_scheme("demographics/age"),
                        analysis_dataset="age"
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/age_category"),
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
    ),
    rapid_pro_target=RapidProTarget(
        rapid_pro=RapidProClientConfiguration(
            domain="textit.com",
            token_file_url="gs://avf-credentials/pool-kenya-textit-token.txt"
        ),
        sync_config=EngagementDBToRapidProConfiguration(
            normal_datasets=[
                DatasetConfiguration(
                    engagement_db_datasets=["age"],
                    rapid_pro_contact_field=ContactField(key="pool_kenya_age", label="pool kenya age")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["gender"],
                    rapid_pro_contact_field=ContactField(key="pool_kenya_gender", label="pool kenya gender")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["location"],
                    rapid_pro_contact_field=ContactField(key="pool_kenya_location", label="pool kenya location")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["disabled"],
                    rapid_pro_contact_field=ContactField(key="pool_kenya_disabled", label="pool kenya disabled")
                ),
            ],
            consent_withdrawn_dataset=DatasetConfiguration(
                engagement_db_datasets=["age", "gender", "location", "disabled", "giz_s01e01"],
                rapid_pro_contact_field=ContactField(key="pool_kenya_consent_withdrawn", label="pool kenya consent withdrawn")
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS,
            allow_clearing_fields=False,
            weekly_advert_contact_field=ContactField(key="giz_pool_advert_contact",
                                                     label="giz pool advert contact"),
            sync_advert_contacts=True,
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/GIZ/"
    )
)
