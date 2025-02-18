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

    def _process_form_items(self, form_id):
        """Helper method to process form items and extract question IDs and titles."""
        form = self.get_form(form_id)
        question_id_to_title_map = {}

        for item in form["items"]:
            if "questionItem" in item:
                question_id = item["questionItem"]["question"]["questionId"]
                question_id_to_title_map[question_id] = item["title"]
            
            elif "questionGroupItem" in item:
                for question in item["questionGroupItem"]["questions"]:
                    question_id = question["questionId"]
                    question_id_to_title_map[question_id] = question["rowQuestion"]["title"]
        
        return question_id_to_title_map

    def get_question_ids(self, form_id):
        """Get set of all question IDs in the form."""
        return set(self._process_form_items(form_id).keys())

    def get_question_id_to_title_map(self, form_id):
        """Get dictionary mapping question IDs to their titles."""
        return self._process_form_items(form_id)

    def get_form_responses(self, form_id, submitted_after_exclusive=None):
        """
        Gets responses to the requested form.

        :param form_id: Form to download responses to.
        :type form_id: str
        :param submitted_after_exclusive: Datetime to filter responses for. If set, only downloads responses last
                                          submitted after this datetime. If None, downloads responses from all of time.
        :type submitted_after_exclusive: datetime.datetime | None
        :return: List of dictionaries representing form responses.
        :rtype: list of dict
        """
        timestamp_filter = None
        timestamp_log = ""
        if submitted_after_exclusive is not None:
            timestamp_filter = f"timestamp > {submitted_after_exclusive.isoformat()}"
            timestamp_log = f", last submitted after {submitted_after_exclusive}"

        log.info(f"Downloading responses to form '{form_id}'{timestamp_log}")

        # Download the first page of responses
        page_responses = self.client.forms().responses().list(formId=form_id, filter=timestamp_filter).execute()
        all_responses = page_responses.get("responses", [])
        page_count = 1
        log.info(f"Downloaded 1 page, {len(all_responses)} total responses")

        # Download all the remaining pages of responses
        while "nextPageToken" in page_responses:
            page_responses = self.client.forms().responses().list(
                formId=form_id, filter=timestamp_filter, pageToken=page_responses["nextPageToken"]
            ).execute()
            page_count += 1
            all_responses.extend(page_responses.get("responses", []))
            log.info(f"Downloaded {page_count} pages, {len(all_responses)} total responses")

        return all_responses
