from core_data_modules.cleaners import swahili

from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="WorldVision",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/world_vision_test"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_world_vision_test",
        uuid_prefix="avf-participant-uuid-"
    ),
    operations_dashboard=OperationsDashboardConfiguration(
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
    ),
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/world-vision-textit-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("worldvision_s01e01_activation", "rqa_s01e01", "world_vision_s01e01"),
                    FlowResultConfiguration("worldvision_s01e02_activation", "rqa_s01e02", "world_vision_s01e02"),
                    FlowResultConfiguration("worldvision_s01e03_activation", "rqa_s01e03", "world_vision_s01e03"),
                    FlowResultConfiguration("worldvision_s01_demog", "age", "world_vision_age"),
                    FlowResultConfiguration("worldvision_s01_demog", "gender", "world_vision_gender"),
                    FlowResultConfiguration("worldvision_s01_demog", "constituency", "world_vision_location")
                ]
            )
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
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldVision_s01e02",
                    engagement_db_dataset="world_vision_s01e02",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("world_vision_s01e02"), auto_coder=None)
                    ],
                    ws_code_string_value="s01e02"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldVision_s01e03",
                    engagement_db_dataset="world_vision_s01e03",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("world_vision_s01e03"), auto_coder=None)
                    ],
                    ws_code_string_value="s01e03"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldVision_age",
                    engagement_db_dataset="world_vision_age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("age"), auto_coder=None)
                    ],
                    ws_code_string_value="age"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldVision_gender",
                    engagement_db_dataset="world_vision_gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("gender"), auto_coder=swahili.DemographicCleaner.clean_gender)
                    ],
                    ws_code_string_value="gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldVision_location",
                    engagement_db_dataset="world_vision_location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("kenya_constituency"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("kenya_county"), auto_coder=None)
                    ],
                    ws_code_string_value="location"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("world_vision_ws_correct_dataset")
        )
    ),
    analysis=AnalysisConfiguration(
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["world_vision_s01e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01e01_raw",
                coding_configs=[
                    CodingConfiguration(code_scheme=load_code_scheme("world_vision_s01e01"), analysis_dataset="s01e01")
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["world_vision_s01e02"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01e02_raw",
                coding_configs=[
                    CodingConfiguration(code_scheme=load_code_scheme("world_vision_s01e02"), analysis_dataset="s01e02")
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["world_vision_s01e03"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01e03_raw",
                coding_configs=[
                    CodingConfiguration(code_scheme=load_code_scheme("world_vision_s01e03"), analysis_dataset="s01e03")
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["world_vision_age"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="age_raw",
                coding_configs=[
                    CodingConfiguration(code_scheme=load_code_scheme("age"), analysis_dataset="age"),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("age_category"),
                        analysis_dataset="age_category",
                        age_category_config=AgeCategoryConfiguration("age", categories={
                            (10, 14): "10 to 14",
                            (15, 17): "15 to 17",
                            (18, 35): "18 to 35",
                            (36, 54): "36 to 54",
                            (55, 99): "55 to 99"
                        })
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["world_vision_gender"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="gender_raw",
                coding_configs=[
                    CodingConfiguration(code_scheme=load_code_scheme("gender"), analysis_dataset="gender")
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["world_vision_location"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="location_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("kenya_constituency"),
                        analysis_dataset="constituency",
                        kenya_analysis_location=AnalysisLocations.KENYA_CONSTITUENCY
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("kenya_county"),
                        analysis_dataset="county",
                        kenya_analysis_location=AnalysisLocations.KENYA_COUNTY
                    )
                ]
            )
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("world_vision_ws_correct_dataset")
    ),
    archive_configurations = ArchiveConfiguration(
        archive_upload_bucket = "gs://pipeline-execution-backup-archive",
        bucket_dir_path =  "2020/WorldVision-Test"
    )
)
