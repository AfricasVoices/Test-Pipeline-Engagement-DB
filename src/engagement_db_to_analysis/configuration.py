from core_data_modules.data_models import CodeScheme

from src.engagement_db_to_rapid_pro.configuration import ContactField


class DatasetTypes:
    DEMOGRAPHIC = "demographic"
    RESEARCH_QUESTION_ANSWER = "research_question_answer"


class AgeCategoryConfiguration:
    def __init__(self, age_analysis_dataset, categories):
        """
        Configuration for automatic imputation of age-category codes from age codes.

        :param age_analysis_dataset: Name of the `CodingConfiguration.analysis_dataset` containing the labelled age
                                     data.
        :type age_analysis_dataset: str
        :param categories: Dictionary of (inclusive minimum age, inclusive maximum age) -> match_value for the
                           age-category code in the age-category code scheme.
        :type categories: dict of (int, int) -> str
        """
        self.age_analysis_dataset = age_analysis_dataset
        self.categories = categories


class AnalysisLocations:
    KENYA_COUNTY = "kenya_county"
    KENYA_CONSTITUENCY = "kenya_constituency"
    KENYA_WARD = "kenya_ward"

    MOGADISHU_SUB_DISTRICT = "mogadishu_sub_district"
    SOMALIA_DISTRICT = "somalia_district"
    SOMALIA_REGION = "somalia_region"
    SOMALIA_STATE = "somalia_state"
    SOMALIA_ZONE = "somalia_zone"
    SOMALIA_OPERATOR = "somalia_operator"


class CodingConfiguration:
    def __init__(self, code_scheme, analysis_dataset, age_category_config, analysis_location):
        """
        Configuration for coded data.

        :param code_scheme: Code scheme for this coding configuration.
        :type code_scheme: core_data_modules.data_models.CodeScheme
        :param analysis_dataset: Name to give this dataset in analysis.
        :type analysis_dataset: str
        :param age_category_config: Optional configuration for automatic age-categorisation.
                                    If provided, automatically imputes age-category codes into this dataset.
        :type age_category_config: AgeCategoryConfiguration | None
        :param analysis_location: One of `AnalysisLocations`, or None.
                                  If provided, locations will automatically be imputed between this and the other
                                  CodingConfigurations that have an analysis_dataset provided, and, depending on the
                                  location, participation maps will automatically be generated.
        :type analysis_location: str | None
        """
        self.code_scheme = code_scheme
        self.analysis_dataset = analysis_dataset
        self.age_category_config = age_category_config
        self.analysis_location = analysis_location


class AnalysisDatasetConfiguration:
    def __init__(self, engagement_db_datasets, dataset_type, raw_dataset, coding_configs,
                 rapid_pro_non_relevant_field=None):
        """
        Configuration for one dataset in analysis. An analysis dataset contains all the messages in the specified
        engagement database datasets, coded under zero or more coding schemes.

        For example, an analysis dataset  "age" might have messages from "pool_age" and "project_age" datasets in the
        database, be labelled under an age and age category code scheme, specify how to derive the age category from
        the age configuration, and used to generate columns in the analysis files and automated analysis.

        :param engagement_db_datasets: Names of datasets in the engagement database to include in this analysis dataset.
                                       All messages with these datasets will be downloaded from the engagement database
                                       and included in this dataset.
        :type engagement_db_datasets: iterable of str
        :param dataset_type: One of `DatasetTypes`.
        :type dataset_type: str
        :param raw_dataset: Name to use for the raw text field in analysis e.g. "age_raw".
                            This will contain the text properties of the downloaded messages.
        :type raw_dataset: str
        :param coding_configs: Configuration for how this data has been coded. Do not provide the WS - Correct Dataset
                               code scheme here - it can be set globally in the main `AnalysisConfiguration`.
        :type coding_configs: list of CodingConfiguration
        :param rapid_pro_non_relevant_field: Key of the contact field in Rapid Pro to write a non-relevant status
                                             indicator back to. For each participant, if they sent only responses
                                             labelled as non-relevant, sets this contact field to "yes".
                                             If None, doesn't sync any non-relevant status for this dataset.
        :type rapid_pro_non_relevant_field: ContactField | None
        """
        self.engagement_db_datasets = engagement_db_datasets
        self.dataset_type = dataset_type
        self.raw_dataset = raw_dataset
        self.coding_configs = coding_configs
        self.rapid_pro_non_relevant_field = rapid_pro_non_relevant_field


class OperatorDatasetConfiguration(AnalysisDatasetConfiguration):
    def __init__(self, raw_dataset, coding_configs):
        """
        Special configuration for an analysis dataset based on a messages' `channel_operator`.

        Special configuration is required here because the raw data for these datasets comes from a different place
        than it does for a standard `AnalysisDatasetConfiguration`: from a message's `channel_operator` rather than
        from its `text` and `labels`.

        :param raw_dataset: Name to use for the raw text field in analysis e.g. "operator_raw".
                            This will contain the `channel_operator` from each Message.
        :type raw_dataset: str
        :param coding_configs: Configuration for how this data has been coded.
        :type coding_configs: list of CodingConfiguration
        """
        super().__init__([], DatasetTypes.DEMOGRAPHIC, raw_dataset, coding_configs)


class GoogleDriveUploadConfiguration:
    def __init__(self, credentials_file_url, drive_dir):
        """
        Configuration for the upload of analysis to a Google Drive directory.

        :param credentials_file_url: GS URL to a service account credentials file to use to upload the analysis.
        :type credentials_file_url: str
        :param drive_dir: Name of the directory in Google Drive to upload the analysis to.
                          This directory must be in the 'shared with me' category of the service account's Drive.
        :type drive_dir: str
        """
        self.credentials_file_url = credentials_file_url
        self.drive_dir = drive_dir


class MembershipGroupConfiguration:
    def __init__(self, membership_group_csv_urls=None):
        """
        Configuration for membership groups.

        A membership group is a tag that is applied to the specified list of participants in analysis.

        For example, participants in a particular listening group could be tagged with that membership group so that
        these participants can easily be tracked through analysis
        e.g. `membership_group_csv_urls={"s01e01_listening_group: ["gs://..."]}`

        :param membership_group_csv_urls: Dictionary of (membership group name) -> list of GS URls to CSVs containing
                                          membership group data. Each CSV must contain a column 'avf-participant-uuid',
                                          where item in this column is a participant_uuid.
        :type membership_group_csv_urls: (dict of str -> (list of str)) | None
        """
        if membership_group_csv_urls is None:
            membership_group_csv_urls = dict()

        self.membership_group_csv_urls = membership_group_csv_urls


class AnalysisConfiguration:
    def __init__(self, dataset_configurations, ws_correct_dataset_code_scheme, cross_tabs=None, traffic_labels=None,
                 google_drive_upload=None, membership_group_configuration=None):
        """
        Configuration for an analysis of data in an engagement database.

        Analysis downloads all the relevant messages from a database, produces production, messages, and participants
        csvs, runs automated analysis, and optionally uploads to Google Drive and/or syncs data back to Rapid Pro.

        :param dataset_configurations: Analysis dataset configurations.
        :type dataset_configurations: list of AnalysisDatasetConfiguration
        :param ws_correct_dataset_code_scheme: 'WS - Correct Dataset' code scheme.
        :type ws_correct_dataset_code_scheme: CodeScheme
        :param cross_tabs: List of pairs of CodingConfiguration dataset_names to compute cross-tabs between
                           e.g. [("age", "gender"), ...]. Each cross-tab will be exported to a different automated
                           analysis file.
                           If None, no cross-tabs will be generated.
        :type cross_tabs: list of (str, str) | None
        :param traffic_labels: List of TrafficLabels to use to generate a traffic_analysis file.
                               If None, no traffic analysis will be conducted.
        :type traffic_labels: iterable of TrafficLabel | None
        :param google_drive_upload: Configuration to use to upload all the analysis and automated analysis results
                                    to Google Drive.
                                    If None, does not upload any data to Google Drive.
        :type google_drive_upload: GoogleDriveUploadConfiguration | None
        :param membership_group_configuration: Configuration for membership groups. These can be used to tag groups
                                               of participants based on participation in provided datasets.
                                               See `MembershipGroupConfiguration` for more details.
        :type membership_group_configuration: MembershipGroupConfiguration
        """
        self.dataset_configurations = dataset_configurations
        self.ws_correct_dataset_code_scheme = ws_correct_dataset_code_scheme
        self.cross_tabs = cross_tabs
        self.traffic_labels = traffic_labels
        self.google_drive_upload = google_drive_upload
        self.membership_group_configuration = membership_group_configuration
