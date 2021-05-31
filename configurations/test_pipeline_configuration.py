from src.pipeline_configuration_spec import PipelineConfiguration, RapidProSource
from src.common.configuration import EngagementDatabaseClientConfiguration, UUIDTableClientConfiguration, RapidProClientConfiguration
from src.rapid_pro_to_engagement_db.configuration import FlowResultConfiguration

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
    ]
)
