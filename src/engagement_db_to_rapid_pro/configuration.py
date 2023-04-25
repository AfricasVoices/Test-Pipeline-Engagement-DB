from dataclasses import dataclass


class WriteModes:
    # Controls how to write data back to Rapid Pro.
    CONCATENATE_TEXTS = "concatenate_texts"  # Concatenate all the raw messages when writing to a contact field
    SHOW_PRESENCE = "show_presence"          # Write a string showing that we have a message for this contact field
                                             # without writing back the messages themselves.


@dataclass
class ContactField:
    def __init__(self, key, label):
        """
        Configuration for creating/accessing a Rapid Pro contact field.

        :param key: Unique key (id) of the contact field in Rapid Pro.
        :type key: str
        :param label: Label to give the contact field in Rapid Pro, if it does not already exist.
                      If the contact field already exists, this field will be ignored.
        :type label: str
        """
        self.key = key
        self.label = label


@dataclass
class DatasetConfiguration:
    def __init__(self, engagement_db_datasets, rapid_pro_contact_field):
        """
        Configuration for syncing engagement database datasets to a single Rapid Pro contact field.

        :param engagement_db_datasets: List of datasets in the engagement database to sync.
        :type engagement_db_datasets: list of str
        :param rapid_pro_contact_field: Rapid Pro contact field to sync the messages to.
        :type rapid_pro_contact_field: ContactField
        """
        self.engagement_db_datasets = engagement_db_datasets
        self.rapid_pro_contact_field = rapid_pro_contact_field


@dataclass
class EngagementDBToRapidProConfiguration:
    def __init__(self, allow_clearing_fields, write_mode=WriteModes.SHOW_PRESENCE,  normal_datasets=None,
                 consent_withdrawn_dataset=None, weekly_advert_contact_field=None, sync_advert_contacts=False):
        """
        Configuration for syncing an engagement database to a Rapid Pro workspace.

        :param allow_clearing_fields: Whether to allow setting contact fields to empty.
                                      For example, this could be used to 'reset' contact fields for questions that
                                      a participant has previously answered but which Rapid Pro should ask again.
                                      However, setting this to True may not be appropriate for a continuous sync to
                                      Rapid Pro because any new messages that have arrived in Rapid Pro but haven't been
                                      synced to the database yet will not be included in this sync from the database.
                                      This might cause consistency issues/spam, depending on the flow design.
                                      Note: This only clears contact fields from participants who had some data to sync
                                      to other fields. Contacts present in Rapid Pro but not in the engagement database
                                      (or with no new data in the database when running in incremental mode)
                                      will not be updated.
        :type allow_clearing_fields: bool
        :param write_mode: One of `WriteModes`.
                           Controls how data is written to the Rapid Pro contact fields e.g. by writing a copy of
                           the data in the engagement database, or just a marker that the data exists.
        :type write_mode: str
        :param normal_datasets: Configuration for syncing 'normal' datasets to Rapid Pro contact fields. If None,
                                no normal_datasets are synced.
                                List of `DatasetConfiguration`s specifying which engagement db datasets should by synced
                                to which contact fields in Rapid Pro.
                                See `allow_clearing_fields` and `write_mode` for further controls on how data is written
                                to Rapid Pro.
        :type normal_datasets: list of DatasetConfiguration | None
        :param consent_withdrawn_dataset: Configuration for syncing consent_withdrawn status to a Rapid Pro contact
                                          field. If None, no consent status is synced.
                                          If set, all the specified engagement database datasets for each participant
                                          will be searched for a 'STOP' label. If a 'STOP' label is found in those
                                          datasets, the Rapid Pro contact field for that participant will be updated
                                          to "yes".
                                          See `allow_clearing_fields` for controlling the behaviour when a participant
                                          was previously marked as "yes" but now has no 'STOP' labels.
        :type consent_withdrawn_dataset: DatasetConfiguration | None
        :param weekly_advert_contact_field: ContactField to sync weekly advert membership to. All contacts who have a
                                            message in a radio_question_answer dataset in the engagement_db -> analysis
                                            configuration, and haven't opted out, will have this field set to 'yes'.
                                            Note:
                                              - For this to run, `sync_advert_contacts` must be configured to True.
                                              - This determines consent status using the datasets configured in
                                                analysis, not the datasets configured in `consent_withdrawn_dataset`.
                                              - This doesn't clear contact fields, so if a contact is given advert
                                                group membership in Rapid Pro by this means and then later opts out,
                                                their membership will not change here. Mitigate this by setting a
                                                `consent_withdrawn_dataset` too.
                                            If None, does not sync weekly advert membership.
        :type weekly_advert_contact_field: ContactField | None
        :param sync_advert_contacts: Whether to sync advert contact groups to Rapid Pro.
                                     If True, this needs to be configured in the rapid pro -> analysis sync.
                                     It also runs from the rapid pro -> analysis sync, not from this sync.
                                     TODO: Fix this layering violation.
        :type sync_advert_contacts: bool
        """
        self.allow_clearing_fields = allow_clearing_fields
        self.normal_datasets = normal_datasets
        self.consent_withdrawn_dataset = consent_withdrawn_dataset
        self.weekly_advert_contact_field = weekly_advert_contact_field
        self.write_mode = write_mode
        self.sync_advert_contacts = sync_advert_contacts
