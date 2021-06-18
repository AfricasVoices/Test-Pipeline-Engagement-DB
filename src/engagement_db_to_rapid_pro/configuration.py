from dataclasses import dataclass
from typing import Optional, List


class WriteModes:
    # Controls how to write data back to Rapid Pro.
    CONCATENATE_TEXTS = "concatenate_texts"  # Concatenate all the raw messages when writing to a contact field
    SHOW_PRESENCE = "show_presence"          # Write a string showing that we have a message for this contact field
                                             # without writing back the messages themselves.


@dataclass
class ContactField:
    key: str
    label: str


@dataclass
class DatasetConfiguration:
    engagement_db_datasets: [str]
    rapid_pro_contact_field: ContactField


@dataclass
class EngagementDBToRapidProConfiguration:
    normal_datasets: Optional[List[DatasetConfiguration]] = None
    consent_withdrawn_dataset: Optional[DatasetConfiguration] = None
    write_mode: str = WriteModes.SHOW_PRESENCE
    allow_clearing_fields: bool = False  # Whether to allow setting contact fields to empty. Setting this to True may
                                         # not be appropriate for continuous sync because a new message may have arrived
                                         # in Rapid Pro but not yet in the engagement database.
