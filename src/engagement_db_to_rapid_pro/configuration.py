from dataclasses import dataclass
from typing import Optional, List


class SyncModes:
    # Controls how to write data back to Rapid Pro.
    CONCATENATE_TEXTS = "concatenate_texts"  # Concatenate all the raw messages when writing to a contact field
    SHOW_PRESENCE = "show_presence"          # Write a string showing that we have a message for this contact field
                                             # without writing back the messages themselves.


@dataclass
class DatasetConfiguration:
    engagement_db_datasets: [str]
    rapid_pro_contact_field: str


@dataclass
class EngagementDBToRapidProConfiguration:
    normal_datasets: Optional[List[DatasetConfiguration]] = None
    # TODO: consent_withdrawn configuration
    sync_mode: str = SyncModes.SHOW_PRESENCE
