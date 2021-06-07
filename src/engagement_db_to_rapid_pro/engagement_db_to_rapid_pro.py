from core_data_modules.logging import Logger
from engagement_database.data_models import MessageStatuses

from src.engagement_db_to_rapid_pro.configuration import WriteModes

log = Logger(__name__)

# Value to write to a Rapid Pro contact field if we're only indicating the presence of an answer
_PRESENCE_VALUE = "#ENGAGEMENT-DATABASE-HAS-RESPONSE"


def sync_engagement_db_to_rapid_pro(engagement_db, rapid_pro, uuid_table, sync_config):
    """
    Synchronises an engagement database to Rapid Pro.

    :param engagement_db: Engagement database to sync to.
    :type engagement_db: engagement_database.EngagementDatabase
    :param rapid_pro: Rapid Pro client to sync from.
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
    messages = engagement_db.get_messages(filter=lambda q: q.where("status", "in", [MessageStatuses.LIVE]))

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
    existing_contact_fields = [f.key for f in rapid_pro.get_fields()]
    contact_fields_to_sync = [dataset_config.rapid_pro_contact_field for dataset_config in sync_config.normal_datasets]
    for contact_field in contact_fields_to_sync:
        if contact_field not in existing_contact_fields:
            rapid_pro.create_field(contact_field)

    # Sync each participant to Rapid Pro.
    for i, (participant_uuid, datasets) in enumerate(participants.items()):
        log.info(f"Syncing participant {i + 1}/{len(participants)}: {participant_uuid}...")
        # Re-identify the participant
        urn = uuid_table.uuid_to_data(participant_uuid)

        # Build a dictionary of contact_field -> value to write for each normal dataset.
        contact_fields = dict()
        for dataset_config in sync_config.normal_datasets:
            # Find all the messages from this participant that are relevant to this dataset.
            messages = []
            for dataset in dataset_config.engagement_db_datasets:
                messages.extend(datasets.get(dataset, []))

            # TODO: Add a flag for controlling whether to overwrite fields where data doesn't exist
            if len(messages) > 0:
                if sync_config.write_mode == WriteModes.SHOW_PRESENCE:
                    contact_fields[dataset_config.rapid_pro_contact_field] = _PRESENCE_VALUE
                else:
                    assert sync_config.write_mode == WriteModes.CONCATENATE_TEXTS
                    contact_fields[dataset_config.rapid_pro_contact_field] = ";".join([m.text for m in messages])

        # TODO: Detect and update consent status

        # Write the contact fields to rapid pro
        rapid_pro.update_contact(urn, contact_fields=contact_fields)
