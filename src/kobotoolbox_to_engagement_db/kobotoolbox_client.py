import requests
import pandas as pd
import json
from dateutil.parser import isoparse

from storage.google_cloud import google_cloud_utils

log = Logger(__name__)


BASE_URL = "https://kf.kobotoolbox.org/api/v2/assets"

class KoboToolBoxClient:
    def get_authorization_headers(google_cloud_credentials_file_path, token_file_url):
        """
        :param token_file_url: Path to the Google Cloud file path that contains KoboToolBox account api token.
        :type token_file_url: str
        :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use when
                                                downloading api token.
        :type google_cloud_credentials_file_path: str
        :return authorization_headers
        :rtype: 
        """
        log.info('Downloading telegram access tokens...')
        api_token = json.loads(google_cloud_utils.download_blob_to_string(
            google_cloud_credentials_file_path, token_file_url).strip())
        
        authorization_headers = {"Authorization": f'Token {api_token}'}

        return authorization_headers
        

    def get_form_responses(authorization_headers, asset_uid, submitted_after_exclusive=None):
        """
        Gets responses to the requested form.

        :param authorization_headers: 
        :type authorization_headers: 
        :param asset_uid: Form to download responses to.
        :type asset_uid: str
        :param submitted_after_exclusive: Datetime to filter responses for. If set, only downloads responses last
                                        submitted after this datetime. If None, downloads responses from all of time.
        :type submitted_after_exclusive: datetime.datetime | None
        :return: List of dictionaries representing form responses.
        :rtype: list of dict
        """

        timestamp_log = ""
        if submitted_after_exclusive is not None:
            timestamp_log = f", last submitted after {submitted_after_exclusive}"
            query = '{"_submission_time":{"$gt":{submitted_after_exclusive}},}'
            print(f"Downloading responses for Asset '{asset_uid}'{timestamp_log}")
            request = f'{BASE_URL}/{asset_uid}/data/?query={query}/?format=json'
        else:
            print(f"Downloading all responses for Asset '{asset_uid}")
            request = f'{BASE_URL}/{asset_uid}/data/?format=json'

        response = requests.get(request, headers=authorization_headers, verify=False)
        form_responses = response.json()["results"]
        print(f"Downloaded {len(form_responses)} total responses")

        print(json.dumps(form_responses, indent=2))

        return form_responses
