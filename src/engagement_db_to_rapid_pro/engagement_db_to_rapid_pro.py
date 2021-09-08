import glob
import json

from core_data_modules.cleaners import Codes
from core_data_modules.data_models import CodeScheme
from core_data_modules.logging import Logger
from engagement_database.data_models import MessageStatuses

from src.engagement_db_to_rapid_pro.configuration import WriteModes

log = Logger(__name__)

# Value to write to a Rapid Pro contact field if we're only indicating the presence of an answer
_PRESENCE_VALUE = "#ENGAGEMENT-DATABASE-HAS-RESPONSE"


def _ensure_rapid_pro_has_contact_fields(rapid_pro, contact_fields):
    """
    Ensures a Rapid Pro workspace has the given contact fields.

    :param rapid_pro: Rapid Pro client to use to ensure a Rapid Pro workspace has the given keys.
    :type rapid_pro: rapid_pro_tools.rapid_pro.RapidProClient
    :param contact_fields: Keys of the contact fields to make sure exist.
    :type contact_fields: list of src.engagement_db_to_rapid_pro.configuration.ContactField
    """
    existing_contact_field_keys = [f.key for f in rapid_pro.get_fields()]
    for contact_field in contact_fields:
        log.info(f"Ensuring Rapid Pro workspace has contact field '{contact_field.key}'")
        if contact_field.key not in existing_contact_field_keys:
            rapid_pro.create_field(field_id=contact_field.key, label=contact_field.label)


def _labels_contain_consent_withdrawn(labels, code_schemes_lut):
    """
    :param labels: Labels to check for consent withdrawn code.
    :type labels: list of core_data_modules.data_models.Label
    :param code_schemes_lut: Look-up table of code scheme id -> code scheme
    :type code_schemes_lut: dict of str -> core_data_modules.data_models.CodeScheme
    :return: Whether any of the given labels contain a code with code id 'STOP'.
    :rtype: bool
    """
    for label in labels:
        code_scheme = code_schemes_lut[label.scheme_id]
        if code_scheme.get_code_with_code_id(label.code_id).control_code == Codes.STOP:
            return True

    return False


def sync_engagement_db_to_rapid_pro(engagement_db, rapid_pro, uuid_table, sync_config):
    """
    Synchronises an engagement database to Rapid Pro.

    :param engagement_db: Engagement database to sync from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param rapid_pro: Rapid Pro client to sync to.
    :type rapid_pro: rapid_pro_tools.rapid_pro_client.RapidProClient
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :param sync_config: Configuration for the sync.
    :type sync_config: src.engagement_db_to_rapid_pro.configuration.EngagementDBToRapidProConfiguration
    """
    # Get all messages to sync.
    # Only sync live messages. Anything else means we should signal Rapid Pro to request the data again if it can.
    # TODO: This could get expensive as the number of datasets increases, in which case we can optimise by adding
    #       an array-contains filter on the dataset field, so we only need to download messages in the datasets we
    #       want to sync.
    messages = engagement_db.get_messages(firestore_query_filter=lambda q: q.where("status", "in", [MessageStatuses.LIVE]))

    # Organise messages by participant then by engagement db dataset.
    participants = dict()  # of participant_uuid -> (dict of dataset -> list of Message)
    for msg in messages:
        if msg.participant_uuid not in participants:
            participants[msg.participant_uuid] = dict()
        participant = participants[msg.participant_uuid]

        if msg.dataset not in participant:
            participant[msg.dataset] = []
        dataset = participant[msg.dataset]

        dataset.append(msg)

    # Make sure all the contact fields exist in the Rapid Pro workspace.
    contact_fields_to_sync = [dataset_config.rapid_pro_contact_field\
        for dataset_config in sync_config.normal_datasets] if sync_config.normal_datasets is not None else []
    if sync_config.consent_withdrawn_dataset is not None:
        contact_fields_to_sync.append(sync_config.consent_withdrawn_dataset.rapid_pro_contact_field)
    _ensure_rapid_pro_has_contact_fields(rapid_pro, contact_fields_to_sync)

    # Load all the project code schemes so we can easily scan for STOP messages later.
    code_schemes = {}
    for path in glob.glob("code_schemes/*.json"):
        with open(path) as f:
            code_scheme = CodeScheme.from_firebase_map(json.load(f))
            code_schemes[code_scheme.scheme_id] = code_scheme

    # Sync each participant to Rapid Pro.
    for i, (participant_uuid, datasets) in enumerate(participants.items()):
        log.info(f"Syncing participant {i + 1}/{len(participants)}: {participant_uuid}...")
        # Re-identify the participant
        urn = uuid_table.uuid_to_data(participant_uuid)

        # Build a dictionary of contact_field -> value to write for each normal dataset.
        contact_fields = dict()
        for dataset_config in sync_config.normal_datasets:
            # Find all the messages from this participant that are relevant to this dataset.
            message_strings = []
            for dataset in dataset_config.engagement_db_datasets:
                messages = datasets.get(dataset, [])
                message_strings.extend([f"\"{m.text}\" - engagement_db.{dataset}" for m in messages])

            # Only overwrite this contact field if there is data to write or it's ok to clear a field.
            if len(message_strings) > 0:
                if sync_config.write_mode == WriteModes.SHOW_PRESENCE:
                    contact_fields[dataset_config.rapid_pro_contact_field.key] = _PRESENCE_VALUE
                else:
                    assert sync_config.write_mode == WriteModes.CONCATENATE_TEXTS
                    contact_fields[dataset_config.rapid_pro_contact_field.key] = "; ".join(message_strings)
            elif sync_config.allow_clearing_fields:
                contact_fields[dataset_config.rapid_pro_contact_field.key] = ""

        if sync_config.consent_withdrawn_dataset is not None:
            # Detect and update consent withdrawn status, by searching all the latest labels on all the messages
            # for a consent withdrawn
            consent_withdrawn_contact_field = sync_config.consent_withdrawn_dataset.rapid_pro_contact_field
            for dataset in sync_config.consent_withdrawn_dataset.engagement_db_datasets:
                labels = []
                for message in datasets.get(dataset, []):
                    labels.extend(message.get_latest_labels())

                if _labels_contain_consent_withdrawn(labels, code_schemes):
                    contact_fields[consent_withdrawn_contact_field.key] = "yes"
                elif sync_config.allow_clearing_fields:
                    contact_fields[consent_withdrawn_contact_field.key] = ""

        # TODO: Update special group membership status e.g listening groups

        # Write the contact fields to rapid pro
        rapid_pro.update_contact(urn, contact_fields=contact_fields)
