import os
from google.api_core.exceptions import NotFound
import csv

from core_data_modules.logging import Logger
from core_data_modules.util import IOUtils
from storage.google_cloud import google_cloud_utils
from core_data_modules.util import TimeUtils
from core_data_modules.traced_data import Metadata

log = Logger(__name__)


def get_membership_groups_csvs(google_cloud_credentials_file_path, membership_group_csv_urls, membership_group_dir_path):
    """
    Downloads de-identified membership groups CSVs from g-cloud.
    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               access the credentials bucket.
    :type google_cloud_credentials_file_path: str
    :param membership_group_csv_urls: Dict of membership group name to group g-cloud csv url(s).
    :type membership_group_csv_urls: Dict
    :param membership_group_dir_path: Path to directory containing de-identified membership groups CSVs containing membership groups data
                        stored as `avf-participant-uuid` column.
    """

    for membership_group, membership_group_csv_url in membership_group_csv_urls:
        for i, membership_group_csv_url in enumerate(membership_group_csv_url):
            membership_group_csv = membership_group_csv_url.split("/")[-1]

            export_file_path = f'{membership_group_dir_path}/{membership_group_csv}'

            if os.path.exists(export_file_path):
                log.info(f"File '{membership_group_csv}' already exists, skipping download")
                continue

            try:
                log.info(f"Saving '{membership_group_csv}' to directory f'{membership_group_dir_path}...")
                IOUtils.ensure_dirs_exist_for_file(export_file_path)
                with open(export_file_path, "wb") as membership_group_csv_file:
                    google_cloud_utils.download_blob_to_file(
                        google_cloud_credentials_file_path, membership_group_csv_url, membership_group_csv_file)

            except NotFound:
                log.warning(f"{membership_group_csv}' not found in google cloud, skipping download")


def tag_membership_groups_participants(user, column_view_traced_data, membership_group_csv_urls, membership_group_dir_path):
    """
    This tags uids who participated in projects membership groups.
    :param user: Identifier of the user running this program, for TracedData Metadata.
    :type user: str
    :param column_view_traced_data: Messages/Participants TracedData organised into column-view format.
    :type: list of core_data_modules.traced_data.TracedData
    :param membership_group_dir_path: Path to directory containing de-identified membership groups CSVs containing membership groups data
                        stored as `avf-phone-uuid` and `Name` columns.
    :type membership_group_dir_path: str
    :param membership_group_csv_urls: Dict of membership group name to group g-cloud csv url(s).
    :type membership_group_csv_urls: Dict
    """

    membership_group_participants = dict() # of group name to group avf-participant-uuid(s)

    # Read listening group participants CSVs and add their uids to the respective group
    for membership_group, csv_urls in membership_group_csv_urls:
        membership_group_participants[membership_group] = set()
        for i, csv_url in enumerate(csv_urls):
            membership_group_csv = csv_url.split("/")[-1]

            membership_group_csv_file_path = f'{membership_group_dir_path}/{membership_group_csv}'

            if os.path.exists(membership_group_csv_file_path):
                with open(membership_group_csv_file_path, "r", encoding='utf-8-sig') as f:
                    membership_group_data = list(csv.DictReader(f))
                    for row in membership_group_data:
                        membership_group_participants[membership_group].add(row['avf-participant-uuid'])
            else:
                log.warning(f"{membership_group_csv} does not exist in {membership_group_dir_path} skipping!")

        log.info(f'Loaded {len(membership_group_participants[membership_group])} {membership_group} uids')

    # Tag a participant based on the membership group type they belong to
    for td in column_view_traced_data:
        membership_group_participation_data = dict()

        for membership_group in membership_group_participants.keys():
            membership_group_participation_data[membership_group] = \
                td['participant_uuid'] in membership_group_participants[membership_group]

        td.append_data(membership_group_participation_data, Metadata(user, Metadata.get_call_location(),
                                                    TimeUtils.utc_now_as_iso_string()))
