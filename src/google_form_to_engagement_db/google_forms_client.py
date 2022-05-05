import google.oauth2.service_account
from core_data_modules.logging import Logger
from googleapiclient import discovery

log = Logger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/forms.body.readonly",
    "https://www.googleapis.com/auth/forms.responses.readonly"
]
DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"


class GoogleFormsClient:
    def __init__(self, credentials_info):
        """
        Constructs a Google Forms client.

        To connect to an existing form, share the form with a service account from the form's sharing ui, then
        initialise here with that service account's credentials info.

        :param credentials_info: Service account credentials.
        :type credentials_info: dict
        """
        credentials = google.oauth2.service_account.Credentials.from_service_account_info(
            credentials_info, scopes=SCOPES
        )

        self.client = discovery.build(
            "forms", "v1", credentials=credentials, discoveryServiceUrl=DISCOVERY_DOC, static_discovery=False
        )

    def get_form(self, form_id):
        return self.client.forms().get(formId=form_id).execute()

    def get_form_responses(self, form_id):
        # Download the first page of responses
        page_responses = self.client.forms().responses().list(formId=form_id).execute()
        all_responses = page_responses.get("responses", [])
        page_count = 1
        log.info(f"Downloaded 1 page, {len(all_responses)} total responses")

        # Download all the remaining pages of responses
        while "nextPageToken" in page_responses:
            page_responses = self.client.forms().responses().list(
                formId=form_id, pageToken=page_responses["nextPageToken"]
            ).execute()
            page_count += 1
            all_responses.extend(page_responses.get("responses", []))
            log.info(f"Downloaded {page_count} pages, {len(all_responses)} total responses")

        return all_responses
