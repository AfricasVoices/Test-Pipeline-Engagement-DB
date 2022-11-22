class CodeSchemeConfiguration:
    def __init__(self, code_scheme, auto_coder=None, coda_code_schemes_count=1):
        """
        Configures one normal code scheme in a Coda dataset.

        There is no need to create this type of configuration to set WS - Correct Dataset code schemes.
        Use the global configuration option in `CodaSyncConfiguration` for this instead.

        :param code_scheme: A Code scheme that should appear in Coda and which messages can be labelled under.
        :type code_scheme: core_data_modules.data_models.CodeScheme
        :param auto_coder: A function which, given a message text, returns a suggestion for the best code to assign
                           to this message. If this is provided, then when adding new, unlabelled messages to Coda:
                            - This auto_coder will be called with the message text to get a suggested code.
                            - The Code which has this suggestion as a match_value will be retrieved from the
                              `code_scheme`.
                            - A new Label will be created from this Code, and assigned to the message in Coda and in
                              the engagement database. This label will be added in the 'unchecked' state, meaning it
                              will still require human review before it should be used in analysis.
                           If None, messages will be added without creating any new, suggested Labels.
                           If a message already has labels (checked or unchecked), these existing labels will be added
                           to Coda instead.
        :type auto_coder: (function of str -> str) | None
        :param coda_code_schemes_count: The number of copies of this code scheme that should appear in Coda.
                                        (Multiple copies allow multiple different labels to be assigned under this
                                         code scheme).
        :type coda_code_schemes_count: int | None
        """
        self.code_scheme = code_scheme
        self.auto_coder = auto_coder
        self.coda_code_schemes_count = coda_code_schemes_count


class CodaDatasetConfiguration:
    def __init__(self, coda_dataset_id, engagement_db_dataset, code_scheme_configurations, ws_code_match_value,
                 dataset_users_file_url=None, update_users_and_code_schemes=True):
        """
        Configures one Coda dataset.

        :param coda_dataset_id: Id (name) of this dataset in Coda e.g. 'Healthcare_s01e01'
        :type coda_dataset_id: str
        :param engagement_db_dataset: Engagement database dataset to sync to Coda.
        :type engagement_db_dataset: str
        :param code_scheme_configurations: Configuration for all the code_schemes that should appear in Coda and which
                                           can be used for labelling messages in this dataset, *except the WS - Correct
                                           Dataset configuration*. The WS - Correct Dataset configuration should be set
                                           globally from a `CodaSyncConfiguration`.
        :type code_scheme_configurations: list of CodeSchemeConfiguration
        :param ws_code_match_value: Match value of the code in the
                                    `CodaSyncConfiguration.ws_correct_dataset_code_scheme` that identifies this dataset.
                                    If a message in another dataset is labelled as WS, and the WS - Correct Dataset
                                    contains this code, then that message will be automatically moved to this dataset.
        :type ws_code_match_value: str
        :param dataset_users_file_url: GS URL to a json file containing a list of user ids, which each user id is a
                                       string containing the email address of a user who should have permission to
                                       access this dataset.
        :type dataset_users_file_url: str | None
        :param update_users_and_code_schemes: Whether to update the users and code schemes currently in Coda to match
                                              the versions referenced in this configuration.
                                              This is recommended and enabled by default, but can be disabled if needed
                                              e.g. if re-running an old pipeline, updating the users of a dataset to
                                              match this old pipeline might not be desirable.
        :type update_users_and_code_schemes: bool
        """
        self.coda_dataset_id = coda_dataset_id
        self.engagement_db_dataset = engagement_db_dataset
        self.code_scheme_configurations = code_scheme_configurations
        self.ws_code_match_value = ws_code_match_value
        self.dataset_users_file_url = dataset_users_file_url
        self.update_users_and_code_schemes = update_users_and_code_schemes


class CodaSyncConfiguration:
    def __init__(self, dataset_configurations, ws_correct_dataset_code_scheme, set_dataset_from_ws_string_value=False,
                 default_ws_dataset=None, project_users_file_url=None):
        """
        Configuration for bidirectional sync between an engagement database and a Coda instance.

        :param dataset_configurations: Configurations for each of the Coda datasets to sync.
        :type dataset_configurations: list of CodaDatasetConfiguration
        :param ws_correct_dataset_code_scheme: WS - Correct Dataset code scheme.
                                               This will be added to every dataset in Coda, and allows messages that
                                               have been assigned to the wrong dataset in Coda to be redirected to the
                                               correct one. To configure this redirection, set the `ws_code_match_value`
                                               properties in each `CodaDatasetConfiguration`.
        :type ws_correct_dataset_code_scheme: core_data_modules.data_models.CodeScheme
        :param set_dataset_from_ws_string_value: If a message is labelled as "WS", the Coda sync tool will use the
                                                 `ws_code_match_value` properties of the `dataset_configurations` to
                                                 find the Coda dataset the message should be moved to.
                                                 If it can't find any match in the `dataset_configurations`, then
                                                 the default behaviour is to crash because 
        :type set_dataset_from_ws_string_value: bool
        :param default_ws_dataset: Engagement db dataset to move messages to if there is no dataset configuration for
                                   a particular ws_code_match_value.
                                   If None, crashes if a message is found with a WS label with a string value not in
                                   dataset_configurations. In most circumstances, this should be None as matching
                                   cases where there are no datasets usually indicates a missing piece of configuration.
        :type default_ws_dataset: str | None
        :param project_users_file_url: GS URL to a json file containing a list of user ids, which each user id is a
                                       string containing the email address of a user who should have permission to
                                       access all the Coda datasets configured here.
                                       If a dataset_configuration has its `dataset_users_file_url` property set,
                                       the users will be updated from that file instead of the one referenced here.
        :type project_users_file_url: str | None
        """
        self.dataset_configurations = dataset_configurations
        self.ws_correct_dataset_code_scheme = ws_correct_dataset_code_scheme
        self.set_dataset_from_ws_string_value = set_dataset_from_ws_string_value
        self.default_ws_dataset = default_ws_dataset
        self.project_users_file_url = project_users_file_url

    def get_dataset_config_by_engagement_db_dataset(self, dataset):
        for config in self.dataset_configurations:
            if config.engagement_db_dataset == dataset:
                return config
        raise ValueError(f"Coda configuration does not contain a dataset_configuration with dataset '{dataset}'")

    def get_dataset_config_by_ws_code_match_value(self, ws_code_match_values):
        for config in self.dataset_configurations:
            for value in ws_code_match_values:
                if config.ws_code_match_value == value:
                    return config
        raise ValueError(f"Coda configuration does not contain a dateset_configuration with a ws_code_match_value "
                         f"in '{ws_code_match_values}'")
