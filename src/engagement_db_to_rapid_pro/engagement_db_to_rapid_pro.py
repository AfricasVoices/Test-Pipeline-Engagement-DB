import glob
import json
from collections import defaultdict

from core_data_modules.cleaners import Codes
from core_data_modules.data_models import CodeScheme
from core_data_modules.logging import Logger

from src.common.cache import Cache
from src.common.get_messages_in_datasets import get_messages_in_datasets
from src.engagement_db_to_rapid_pro.configuration import WriteModes

log = Logger(__name__)

# Value to write to a Rapid Pro contact field if we're only indicating the presence of an answer
_PRESENCE_VALUE = "#ENGAGEMENT-DATABASE-HAS-RESPONSE"


def _engagement_db_datasets_in_sync_config(sync_config):
    """
    :param sync_config: Sync config to get the engagement_db_datasets from.
    :type sync_config: src.engagement_db_to_rapid_pro.configuration.EngagementDBToRapidProConfiguration
    :return: All the engagement_db_datasets in the sync_config.
    :rtype: set of str
    """
    engagement_db_datasets = set()

    if sync_config.normal_datasets is not None:
        for dataset_config in sync_config.normal_datasets:
            engagement_db_datasets.update(dataset_config.engagement_db_datasets)

    if sync_config.consent_withdrawn_dataset is not None:
        engagement_db_datasets.update(sync_config.consent_withdrawn_dataset.engagement_db_datasets)

    return engagement_db_datasets


def _get_all_messages(engagement_db, sync_config, cache=None):
    """
    Gets all the messages in the database relevant to this sync_config i.e. all messages in the engagement_db_datasets
    in the sync_config.

    :param engagement_db: Engagement database to get messages from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param sync_config: Sync config defining which messages to get.
    :type sync_config: src.engagement_db_to_rapid_pro.configuration.EngagementDBToRapidProConfiguration
    :param cache: Cache to use for messages, or None.
    :type cache: src.common.cache.Cache | None
    :return: All messages in the datasets in the sync_config.
    :rtype: list of engagement_database.data_models.Message
    """
    engagement_db_datasets = _engagement_db_datasets_in_sync_config(sync_config)
    messages_by_dataset = get_messages_in_datasets(engagement_db, engagement_db_datasets, cache)

    messages = []
    for msgs in messages_by_dataset.values():
        messages.extend(msgs)

    return messages


def _get_normal_contact_fields_for_participant(participant_messages, sync_config):
    """
    Gets the normal contact fields for a given participant and sync configuration.

    :param participant_messages: All messages from the participant to process.
    :type participant_messages: list of engagement_database.data_models.Message
    :param sync_config: Sync config defining which messages to get.
    :type sync_config: src.engagement_db_to_rapid_pro.configuration.EngagementDBToRapidProConfiguration
    :return: Dictionary of Rapid Pro contact field id -> value.
    :rtype: dict of str -> str
    """
    if sync_config.normal_datasets is None:
        return dict()

    contact_fields = dict()
    for dataset_config in sync_config.normal_datasets:
        # Find all the messages from this participant in this dataset
        dataset_messages = []
        for msg in participant_messages:
            if msg.dataset in dataset_config.engagement_db_datasets and msg.text is not None:
                dataset_messages.append(msg)

        # If there are no messages in this dataset, either clear the contact field if we're allowed to, or simply skip
        # this dataset if not.
        # (We might not be able to be allowed to clear the field because doing so could cause synchronisation problems
        #  with Rapid Pro, due to the delay between a flow updating a contact field and that change being processed
        #  by our infrastructure and synced back - in other words, we can't guarantee read-after-write consistency
        #  between Rapid Pro and the engagement database)
        if len(dataset_messages) == 0:
            if sync_config.allow_clearing_fields:
                contact_fields[dataset_config.rapid_pro_contact_field.key] = ""
            continue

        # Write the found data back to Rapid Pro as either, depending on the configuration, the raw data
        # (better for debugging) or as presence indicators (stronger privacy preservation).
        if sync_config.write_mode == WriteModes.SHOW_PRESENCE:
            contact_fields[dataset_config.rapid_pro_contact_field.key] = _PRESENCE_VALUE
        else:
            assert sync_config.write_mode == WriteModes.CONCATENATE_TEXTS
            message_strings = [f"\"{msg.text}\" - engagement_db.{msg.dataset}" for msg in dataset_messages]
            contact_fields[dataset_config.rapid_pro_contact_field.key] = "; ".join(message_strings)

    return contact_fields


def _get_consent_withdrawn_field_for_participant(participant_messages, sync_config, code_schemes):
    """
    Gets the consent_withdrawn contact field for a given participant and sync configuration.

    :param participant_messages: All messages from the participant to process.
    :type participant_messages: list of engagement_database.data_models.Message
    :param sync_config: Sync config defining which messages to get.
    :type sync_config: src.engagement_db_to_rapid_pro.configuration.EngagementDBToRapidProConfiguration
    :param code_schemes: Project code schemes (used to decode the labels to identify consent withdrawn messages).
    :type code_schemes: list of core_data_modules.data_models.CodeScheme
    :return: Dictionary of Rapid Pro contact field id -> value.
    :rtype: dict of str -> str
    """
    if sync_config.consent_withdrawn_dataset is None:
        return dict()

    all_labels = []
    for msg in participant_messages:
        if msg.dataset in sync_config.consent_withdrawn_dataset.engagement_db_datasets:
            all_labels.extend(msg.get_latest_labels())

    contact_fields = dict()
    consent_withdrawn_contact_field = sync_config.consent_withdrawn_dataset.rapid_pro_contact_field
    if _labels_contain_consent_withdrawn(all_labels, code_schemes):
        contact_fields[consent_withdrawn_contact_field.key] = "yes"
    elif sync_config.allow_clearing_fields:
        contact_fields[consent_withdrawn_contact_field.key] = ""

    return contact_fields


def _ensure_rapid_pro_has_contact_fields(rapid_pro, contact_fields, dry_run=False):
    """
    Ensures a Rapid Pro workspace has the given contact fields.

    :param rapid_pro: Rapid Pro client to use to ensure a Rapid Pro workspace has the given keys.
    :type rapid_pro: rapid_pro_tools.rapid_pro.RapidProClient
    :param contact_fields: Keys of the contact fields to make sure exist.
    :type contact_fields: list of src.engagement_db_to_rapid_pro.configuration.ContactField
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    """
    existing_contact_field_keys = [f.key for f in rapid_pro.get_fields()]
    for contact_field in contact_fields:
        log.info(f"Ensuring Rapid Pro workspace has contact field '{contact_field.key}'")
        if contact_field.key not in existing_contact_field_keys and not dry_run:
            rapid_pro.create_field(field_id=contact_field.key, label=contact_field.label)


def _code_scheme_for_label(label, code_schemes):
    for code_scheme in code_schemes:
        if label.scheme_id.startswith(code_scheme.scheme_id):
            return code_scheme


def _merge_code_schemes(code_schemes):
    """
    Merges the given `code_schemes` into a single code scheme containing all the codes from the input `code_schemes`.

    Fails if `len(code_schemes) == 0` or if any of the `code_schemes` have differing `scheme_id`s.

    :param code_schemes: Code schemes to merge into one.
    :type code_schemes: list of core_data_modules.data_models.CodeScheme
    :return: `code_schemes` merged into a single code scheme containing all the codes.
    :rtype: core_data_modules.data_models.CodeScheme
    """
    assert len(code_schemes) > 0, len(code_schemes)

    merged_code_scheme = code_schemes[0]
    for code_scheme in code_schemes[1:]:
        assert code_scheme.scheme_id == merged_code_scheme.scheme_id
        # For each code in this code scheme, add it to the merged code scheme if it doesn't exist on that scheme yet.
        # If it does exist, ensure the code is the same as the one that exists already.
        for code in code_scheme.codes:
            merged_code_ids = {c.code_id for c in merged_code_scheme.codes}
            if code.code_id in merged_code_ids:
                assert code == merged_code_scheme.get_code_with_code_id(code.code_id)
                continue

            merged_code_scheme.codes.append(code)

    return merged_code_scheme


def _merge_code_schemes_by_scheme_id(code_schemes):
    """
    Merges the given `code_schemes` such that those that share the same scheme_id are combined into one code scheme
    that contains all the codes from the merged code schemes. Code schemes which have unique scheme_ids will be
    included in the results unmodified.

    :param code_schemes: Code schemes to merge by scheme_id.
    :type code_schemes: list of core_data_modules.data_models.CodeScheme
    :return: `code_schemes` merged by scheme_ids.
    :rtype code_schemes: list of core_data_modules.data_models.CodeScheme
    """
    code_schemes_by_id = defaultdict(list)
    for code_scheme in code_schemes:
        code_schemes_by_id[code_scheme.scheme_id].append(code_scheme)

    merged_code_schemes = []
    for code_schemes in code_schemes_by_id.values():
        merged_code_schemes.append(_merge_code_schemes(code_schemes))
    return merged_code_schemes


def _labels_contain_consent_withdrawn(labels, code_schemes):
    """
    :param labels: Labels to check for consent withdrawn code.
    :type labels: list of core_data_modules.data_models.Label
    :param code_schemes: List of project code schemes.
    :type code_schemes: iterable of core_data_modules.data_models.CodeScheme
    :return: Whether any of the given labels contain a code with code id 'STOP'.
    :rtype: bool
    """
    for label in labels:
        code_scheme = _code_scheme_for_label(label, code_schemes)
        assert code_scheme is not None, f"Label has scheme_id {label.scheme_id}, but this is not present in any of " \
                                        f"the given code schemes."
        if code_scheme.get_code_with_code_id(label.code_id).control_code == Codes.STOP:
            return True

    return False


def sync_engagement_db_to_rapid_pro(engagement_db, rapid_pro, uuid_table, sync_config, cache_path=None, dry_run=False):
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
    :param cache_path: Path to a directory to use to cache results needed for incremental operation.
                       If None, runs in non-incremental mode.
    :type cache_path: str | None
    :param dry_run: Whether to perform a dry run.
    :type dry_run: bool
    """
    # Initialise the cache
    if cache_path is None:
        cache = None
        log.warning(f"No `cache_path` provided. This tool will sync all relevant engagement db messages from all of time")
    else:
        log.info(f"Initialising engagement db -> rapid pro sync cache at '{cache_path}/engagement_db_to_rapid_pro'")
        cache = Cache(f"{cache_path}/engagement_db_to_rapid_pro")

    # Load all the project code schemes, so we can easily scan for STOP messages later.
    code_schemes = []
    for path in glob.glob("code_schemes/**/*.json", recursive=True):
        with open(path) as f:
            code_schemes.append(CodeScheme.from_firebase_map(json.load(f)))

    # Some RQAs from projects that were run before this Engagement-Data-Pipeline infrastructure was created accidentally
    # used the same scheme_id for different code_schemes between projects. Since this stage only uses code schemes
    # to check for consent withdrawn status, handle this by merging impacted code_schemes into one for now.
    # TODO: Edit the problematic code schemes in affected projects to give them all unique scheme ids, then
    #       remove this workaround.
    code_schemes = _merge_code_schemes_by_scheme_id(code_schemes)

    # Get all the messages from the datasets we're interested in syncing, and group them by participant
    messages = _get_all_messages(engagement_db, sync_config, cache)
    messages_by_participant = defaultdict(list)  # of participant_uuid -> list of Message
    for msg in messages:
        messages_by_participant[msg.participant_uuid].append(msg)

    # Get the messages updated since the message we last synced. We'll update contacts for each of these messages.
    last_synced_message = None
    if cache is not None:
        last_synced_message = cache.get_message("last_synced")

    if last_synced_message is None:
        messages_triggering_sync = messages
    else:
        messages_triggering_sync = []
        for msg in messages:
            if msg.last_updated < last_synced_message.last_updated:
                continue
            if msg.last_updated == last_synced_message.last_updated and msg.message_id <= last_synced_message.message_id:
                continue
            messages_triggering_sync.append(msg)
    messages_triggering_sync.sort(key=lambda msg: (msg.last_updated, msg.message_id))

    # Make sure all the contact fields exist in the Rapid Pro workspace.
    contact_fields_to_sync = [dataset_config.rapid_pro_contact_field \
        for dataset_config in sync_config.normal_datasets] if sync_config.normal_datasets is not None else []
    if sync_config.consent_withdrawn_dataset is not None:
        contact_fields_to_sync.append(sync_config.consent_withdrawn_dataset.rapid_pro_contact_field)
    _ensure_rapid_pro_has_contact_fields(rapid_pro, contact_fields_to_sync, dry_run)

    # Sync each message to Rapid Pro, by recomputing the state of every participant.
    participants_synced_this_cycle = set()
    non_deindentified_uuids = 0
    for i, message in enumerate(messages_triggering_sync):
        log.info(f"Syncing message {i + 1}/{len(messages_triggering_sync)}: {message.message_id}...")
        participant_uuid = message.participant_uuid

        if not participant_uuid.startswith(uuid_table._uuid_prefix):
            non_deindentified_uuids += 1
            continue

        if participant_uuid in participants_synced_this_cycle:
            log.info(f"Skipping this message because we've already synced participant_uuid {participant_uuid} in this "
                     f"pipeline run")
            if cache is not None and not dry_run:
                cache.set_message("last_synced", message)
            continue

        # Build a dictionary of contact_field -> value for all the latest values for this participant.
        contact_fields = dict()
        contact_fields.update(
            _get_normal_contact_fields_for_participant(messages_by_participant[participant_uuid], sync_config)
        )
        contact_fields.update(
            _get_consent_withdrawn_field_for_participant(messages_by_participant[participant_uuid], sync_config, code_schemes)
        )

        # TODO: Update special group membership status e.g listening groups

        # Re-identify the participant.
        urn = uuid_table.uuid_to_data(participant_uuid)

        # Write the contact fields to rapid pro
        if not dry_run:
            rapid_pro.update_contact(urn, contact_fields=contact_fields)

        participants_synced_this_cycle.add(participant_uuid)
        if cache is not None and not dry_run:
            cache.set_message("last_synced", message)

    log.warning(f"skipped syncing {non_deindentified_uuids} non deindentified uuids")

    log.info(f"Done")
    # TODO: Print summary of actions
