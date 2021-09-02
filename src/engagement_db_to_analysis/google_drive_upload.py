import json
import os

from core_data_modules.logging import Logger
from storage.google_cloud import google_cloud_utils
from storage.google_drive import drive_client_wrapper

log = Logger(__name__)


def init_client(google_cloud_credentials_file_path, drive_credentials_file_url):
    """
    Initialises the `drive_client_wrapper`.

    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               access the credentials bucket.
    :type google_cloud_credentials_file_path: str
    :param drive_credentials_file_url: GS URL to the Google Drive service account credentials file in the credentials
                                       bucket.
    :type drive_credentials_file_url: str
    """
    log.info(f"Downloading Google Drive service account credentials...")
    credentials_info = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        drive_credentials_file_url
    ))
    drive_client_wrapper.init_client_from_info(credentials_info)


def upload_file(source_file_path, target_dir):
    """
    Uploads a file from local disk to Google Drive.

    :param source_file_path: Path to a local file on disk to upload.
    :type source_file_path: str
    :param target_dir: Path to target directory on Google Drive.
    :type target_dir: str
    """
    log.info(f"Uploading '{source_file_path}' to Google Drive path '{target_dir}'...")
    drive_client_wrapper.update_or_create(
        source_file_path=source_file_path,
        target_folder_path=target_dir,
        target_file_name=os.path.basename(source_file_path),
        target_folder_is_shared_with_me=True,
        recursive=True,
        fix_duplicates=True
    )


def upload_all_files_in_dir(source_dir_path, target_dir, recursive=False):
    """
    Uploads all files in a directory to Google Drive.

    Note this is non-recursive i.e. files in subdirectories will not be uploaded.

    :param source_dir_path: Path to a local directory on disk to upload files from.
    :type source_dir_path: str
    :param target_dir: Path to target directory on Google Drive.
    :type target_dir: str
    :param recursive: Whether to recursively upload any sub-directories in this directory too.
    :type recursive: bool
    """
    source_dir_contents = [os.path.join(source_dir_path, f) for f in os.listdir(source_dir_path)]

    # Batch upload all the files in this directory
    file_paths = []
    for path in source_dir_contents:
        if os.path.isfile(path):
            file_paths.append(path)
    drive_client_wrapper.update_or_create_batch(
        source_file_paths=file_paths,
        target_folder_path=target_dir,
        target_folder_is_shared_with_me=True,
        recursive=True,
        fix_duplicates=True
    )

    if recursive:
        for path in source_dir_contents:
            if not os.path.isfile(path):
                upload_all_files_in_dir(path, f"{target_dir}/{os.path.basename(path)}", recursive=True)
