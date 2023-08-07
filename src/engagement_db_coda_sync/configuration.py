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

        Coda sync stages copy messages and their labels between an engagement database and Coda.

        When a message is added to an engagement database, the engagement db -> coda sync stage:
         - Sets the coda_id on the message, such that messages with the same text are given the same coda_id and
           so only appear once in Coda.
         - If the message doesn't exist in Coda, then adds the message to Coda, possibly with some automatically
           suggested labels.
         - If the message does exist in Coda, then copies the labels from Coda to the engagement db message.

         When a message is changed in Coda, the coda -> engagement db sync stage updates all the matching messages
         in the engagement db to have the same labels.

         When updating the labels on a message in the engagement db, if a WS - Correct Dataset code (ws_code) is found
         in the new labels then the message's dataset property will be changed appropriately. To determine which new
         dataset to use, the following strategies are tried, in this order:
          1. Search the other dataset configurations for a match for this `ws_code`. If there is no match:
          2. If `set_dataset_from_ws_string_value` has been set, move the message to the dataset
            `ws_code.string_value`. Otherwise:
          3. If the `default_ws_dataset` has been specified, move the message to this default dataset.
          4. Crash with a ValueError.

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
                                                 the default behaviour is to crash because it means there was a WS
                                                 code found that wasn't explicitly configured. However, if this
                                                 argument is True, then instead of crashing on an unseen ws code, the
                                                 sync stage will instead change the message's dataset to the ws code's
                                                 `string_value`. This can be useful (e.g. if migrating an old project to
                                                 the engagement database), but should be used with caution as it means
                                                 messages can be moved to a dataset that has no configuration.
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

        self.validate()

    def validate(self):
        # Ensure that all the ws_code_match_values match a code in the ws_correct_dataset_code_scheme.
        for dataset in self.dataset_configurations:
            try:
                self.ws_correct_dataset_code_scheme.get_code_with_match_value(dataset.ws_code_match_value)
            except KeyError as e:
                raise KeyError(f"A dataset_configuration in the CodaSyncConfiguration had a ws_code_match_value "
                               f"'{dataset.ws_code_match_value}', but this does not match any code in the "
                               f"ws_correct_dataset_code_scheme. Add this code to the ws_correct_dataset_code_scheme "
                               f"or remove this dataset_configuration") from e

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

    def get_non_ws_code_schemes(self):
        """
        TODO: Remove?
        """
        code_schemes = []
        for dataset_config in self.dataset_configurations:
            for code_scheme_config in dataset_config.code_scheme_configurations:
                code_schemes.append(code_scheme_config.code_scheme)
        return code_schemes
