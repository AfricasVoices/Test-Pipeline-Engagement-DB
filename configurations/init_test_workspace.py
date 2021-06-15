from src.pipeline_configuration_spec import *


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="init-test-workspace",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/experimental_test"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_test",
        uuid_prefix="avf-participant-uuid-"
    ),
    rapid_pro_target=RapidProTarget(
        rapid_pro=RapidProClientConfiguration(
            domain="textit.com",
            token_file_url="gs://avf-credentials/experimental-sync-test-textit-token.txt"
        ),
        sync_config=EngagementDBToRapidProConfiguration(
            normal_datasets=[
                DatasetConfiguration(engagement_db_datasets=["gender"],   rapid_pro_contact_field=ContactField(key="gender",   label="Gender")),
                DatasetConfiguration(engagement_db_datasets=["location"], rapid_pro_contact_field=ContactField(key="location", label="Location")),
                DatasetConfiguration(engagement_db_datasets=["age"],      rapid_pro_contact_field=ContactField(key="age",      label="Age")),
                DatasetConfiguration(engagement_db_datasets=["s01e01"],   rapid_pro_contact_field=ContactField(key="s01e01",   label="Test S01E01"))
            ],
            consent_withdrawn_dataset=DatasetConfiguration(
                engagement_db_datasets=["gender", "location", "age", "s01e01"],
                rapid_pro_contact_field=ContactField(key="engagement_db_consent_withdrawn", label="Engagement DB Consent Withdrawn")
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS,
            allow_clearing_fields=True
        )
    )
)
