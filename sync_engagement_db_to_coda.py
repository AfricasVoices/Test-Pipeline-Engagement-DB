import argparse
import json
import subprocess

from core_data_modules.cleaners import swahili
from core_data_modules.data_models import CodeScheme
from core_data_modules.logging import Logger
from engagement_database.data_models import HistoryEntryOrigin

from src.common.configuration import UUIDTableConfiguration, EngagementDatabaseConfiguration
from src.engagament_db_to_coda.configuration import CodaConfiguration, CodaDatasetConfiguration, CodeSchemeConfiguration
from src.engagament_db_to_coda.engagement_db_to_coda import sync_engagement_db_to_coda

log = Logger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Syncs data from an engagement database to Coda")

    parser.add_argument("user", help="Identifier of the user launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")

    args = parser.parse_args()

    user = args.user
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path

    pipeline = "engagement-db-test"
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
    project = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()

    HistoryEntryOrigin.set_defaults(user, project, pipeline, commit)

    uuid_table_configuration = UUIDTableConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        table_name="_engagement_db_test",
        uuid_prefix="avf-participant-uuid-"
    )

    engagement_db_configuration = EngagementDatabaseConfiguration(
        credentials_file_url="gs://avf-credentials/firebase-test.json",
        database_path="engagement_db_experiments/experimental_test"
    )

    def load_scheme(scheme):
        with open(f"code_schemes/{scheme}.json") as f:
            return CodeScheme.from_firebase_map(json.load(f))

    coda_config = CodaConfiguration(
        credentials_file_url="gs://avf-credentials/coda-staging.json",
        dataset_configurations=[
            CodaDatasetConfiguration(
                coda_dataset_id="TEST_gender",
                engagement_db_dataset="gender",
                code_scheme_configurations=[
                    CodeSchemeConfiguration(code_scheme=load_scheme("gender"), auto_coder=swahili.DemographicCleaner.clean_gender)
                ],
                ws_code_string_value="gender"
            ),
            CodaDatasetConfiguration(
                coda_dataset_id="TEST_location",
                engagement_db_dataset="location",
                code_scheme_configurations=[
                    CodeSchemeConfiguration(code_scheme=load_scheme("kenya_constituency"), auto_coder=None),
                    CodeSchemeConfiguration(code_scheme=load_scheme("kenya_county"), auto_coder=None)
                ],
                ws_code_string_value="location"
            ),

            CodaDatasetConfiguration(
                coda_dataset_id="TEST_s01e01",
                engagement_db_dataset="s01e01",
                code_scheme_configurations=[
                    CodeSchemeConfiguration(code_scheme=load_scheme("s01e01"), auto_coder=None)
                ],
                ws_code_string_value="s01e01"
            ),
        ],
        ws_correct_dataset_code_scheme=load_scheme("ws_correct_dataset")
    )

    uuid_table = uuid_table_configuration.init_uuid_table(google_cloud_credentials_file_path)
    engagement_db = engagement_db_configuration.init_engagement_db(google_cloud_credentials_file_path)

    sync_engagement_db_to_coda(google_cloud_credentials_file_path, coda_config, engagement_db)
