from core_data_modules.cleaners import swahili

from dateutil.parser import isoparse


from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="engagement-db-test",
    # TODO: store in messages and individuals_filter list of functions.
    project_start_date = isoparse("2021-03-01T10:30:00+03:00"),
    project_end_date = isoparse("2100-01-01T00:00:00+03:00"),
    filter_test_messages = False,
    test_participant_uids = [
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
            consent_withdrawn_dataset=DatasetConfiguration(
                engagement_db_datasets=["gender", "location", "age", "s01e01"],
                rapid_pro_contact_field=ContactField(key="engagement_db_consent_withdrawn", label="Engagement DB Consent Withdrawn")
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS
        )
    ),
    analysis_config=[
        AnalysisDatasetConfiguration(
            engagement_db_datasets = ["s01e01"],
            dataset_type = DatasetTypes.RESEARCH_QUESTION_ANSWER
        ),
        AnalysisDatasetConfiguration(
            engagement_db_datasets = ["gender"],
            dataset_type = DatasetTypes.RESEARCH_QUESTION_ANSWER
        ),
        AnalysisDatasetConfiguration(
            engagement_db_datasets = ["location"],
            dataset_type = DatasetTypes.RESEARCH_QUESTION_ANSWER
        ),
    ]
)
