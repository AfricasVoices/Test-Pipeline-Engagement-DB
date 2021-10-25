import argparse
import os
import re
import importlib


from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils


log = Logger(__name__)


def get_file_paths(dir_path):
    # search for .gzip archive files only because os.listdir(dir_path)
    # returns all files in the directory
    files_list = [file for file in os.listdir(dir_path) if file.endswith((".gzip"))]
    file_paths = [os.path.join(dir_path, basename) for basename in files_list]

    return file_paths

def get_uploaded_file_dates(uploaded_files_list, date_pattern):
    dates_match = [re.search(date_pattern, file) for file in uploaded_files_list]
    uploaded_file_dates = []
    for date_match in dates_match:
        if date_match == None:
            continue
        uploaded_file_dates.append(date_match.group())

    return uploaded_file_dates

def get_files_by_date(dir_path, uploaded_file_dates):
    file_paths = get_file_paths(dir_path)
    files_by_date = {}
    if len(file_paths) > 0:
        for file in file_paths:
            file_date_match = re.search(date_pattern, file)
            file_date = file_date_match.group()
            if file_date in uploaded_file_dates:
                log.info(f" file already uploaded for {file_date}, skipping...")
            else:
                if file_date not in files_by_date:
                    files_by_date[file_date] = []
                files_by_date[file_date].append(file)
    else:
        log.info(f" No file found in {dir_path}!, skipping...")

    return files_by_date

def delete_old_archive_files(dir_path, uploaded_file_dates):
    archive_file_paths = get_file_paths(dir_path)
    files_for_days_that_upload_failed = {}

    most_recent_file_path = None
    if len(archive_file_paths) > 0:
        most_recent_file_path = max(archive_file_paths, key=os.path.getmtime)

    for file_path in archive_file_paths:
        file_date_match = re.search(date_pattern, file_path)
        file_date = file_date_match.group()

        # Create a list of files for days that failed to upload
        if file_date in uploaded_file_dates:
            if file_path == most_recent_file_path:
                log.info(f"Retaining latest modified file {file_path} for quick retrieval")
                continue

            log.warning(f"Deleting {file_path} because files for {file_date} already uploaded to cloud")
            os.remove(os.path.join(dir_path, file_path))

        # Delete files for days that have a file uploaded in g-cloud
        else:
            log.debug(f'Files for {file_date} not yet uploaded to cloud, '
                      f'will delete other files and retain the latest modified file for upload')
            if file_date not in files_for_days_that_upload_failed:
                files_for_days_that_upload_failed[file_date] = []

            files_for_days_that_upload_failed[file_date].append(file_path)

    # Check for latest modified file path for each day that failed to upload
    # Delete other files for that date
    for file_date in files_for_days_that_upload_failed:
        most_recent_file_path = max(files_for_days_that_upload_failed[file_date], key=os.path.getmtime)
        for file_path in files_for_days_that_upload_failed[file_date]:
            if file_path == most_recent_file_path:
                log.debug(f"Retaining {file_path}")
                continue

            log.warning(f"Deleting old file {file_path} for {file_date}")
            os.remove(os.path.join(dir_path, file_path))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Uploads pipeline archive files to g-cloud")

    parser.add_argument("user", help="User launching this program")
    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("configuration_module",
                        help="Configuration module to import e.g. 'configurations.test_config'. "
                             "This module must contain a PIPELINE_CONFIGURATION property")
    parser.add_argument("archive_dir_path", metavar="archive_dir_path",
                        help="Path to the data archive directory with file to upload")

    args = parser.parse_args()

    user = args.user
    pipeline_config = importlib.import_module(args.configuration_module).PIPELINE_CONFIGURATION
    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    archive_dir_path = args.archive_dir_path

    date_pattern = r'\d{4}-\d{2}-\d{2}'

    uploaded_data_archives = google_cloud_utils.list_blobs(google_cloud_credentials_file_path,
                                                           pipeline_config.archive_configuration.archive_upload_bucket,
                                                           pipeline_config.archive_configuration.bucket_dir_path)
    uploaded_archive_dates = get_uploaded_file_dates(uploaded_data_archives, date_pattern)

    log.warning(f"Deleting old data archives files from local disk...")
    delete_old_archive_files(archive_dir_path, uploaded_archive_dates)

    log.info(f"Uploading archive files...")
    archive_files_by_date = get_files_by_date(archive_dir_path, uploaded_archive_dates)
    for file_date in archive_files_by_date:
        latest_archive_file_path = max(archive_files_by_date[file_date], key=os.path.getmtime)
        archive_upload_location = f"{pipeline_config.archive_configuration.archive_upload_bucket}/" \
            f"{pipeline_config.archive_configuration.bucket_dir_path}/{os.path.basename(latest_archive_file_path)}"
        log.info(f"Uploading data archive from {latest_archive_file_path} to {archive_upload_location}...")
        with open(latest_archive_file_path, "rb") as f:
            google_cloud_utils.upload_file_to_blob(google_cloud_credentials_file_path, archive_upload_location, f)
