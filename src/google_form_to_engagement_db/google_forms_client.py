import google.oauth2.service_account
from googleapiclient import discovery

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
        return self.client.forms().responses().list(formId=form_id).execute()
