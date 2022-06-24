
from core_data_modules.analysis import analysis_utils, AnalysisConfiguration as core_data_analysis_config
from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger

from src.engagement_db_to_analysis.cache import AnalysisCache
from src.engagement_db_to_analysis.membership_group import (_get_membership_groups_data)
from src.pipeline_configuration_spec import *


log = Logger(__name__)

CONSENT_WITHDRAWN_KEY = "consent_withdrawn"

#TODO move this to engagement db to rapid_pro sync once we support syncing imputed labels to db
def _generate_weekly_advert_uuids(participants_by_column, analysis_config,
                                              google_cloud_credentials_file_path, membership_group_dir_path):
    '''
    Generates sets of weekly advert UUIDs to advertise to. A participant is considered as
    being needed to advertise to if they are in the participants_by_column or
    a listening group and haven't opted out.

    :param participants_by_column: list of participants column view TracedData object to generate the uuids from.
    :type participants_by_column: list of core_data_modules.traced_data.TracedData
    :param analysis_config: Configuration for the export.
    :type analysis_config: src.engagement_db_to_analysis.configuration.AnalysisConfiguration
    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               access the credentials bucket.
    :type google_cloud_credentials_file_path: str
    :param membership_group_dir_path: Path to directory containing de-identified membership groups CSVs containing membership groups data
                        stored as `avf-participant-uuid` column.
    :type: membership_group_dir_path: str
    :return opt_out_uuids and weekly_advert_uuids : Set of opted out and weekly advert uuids.
    :rtype opt_out_uuids & weekly_advert_uuids: (set of str, set of str)
    '''

    opt_out_uuids = set()
    weekly_advert_uuids = set()
    for participant_td in participants_by_column:
        if participant_td["consent_withdrawn"] == Codes.TRUE:
            opt_out_uuids.add(participant_td["participant_uuid"])
            continue

        weekly_advert_uuids.add(participant_td["participant_uuid"])

    # If available, add consented membership group uids to advert uuids
    if analysis_config.membership_group_configuration is not None:
        log.info(f"Adding consented membership group uids to advert uuids ")
        membership_group_csv_urls = \
            analysis_config.membership_group_configuration.membership_group_csv_urls.items()

        membership_groups_data = _get_membership_groups_data(google_cloud_credentials_file_path,
                                                            membership_group_csv_urls, membership_group_dir_path)

        consented_membership_groups_uuids = 0
        opt_out_membership_groups_uuids = 0
        for membership_group in membership_groups_data.values():
            for uuid in membership_group:
                if uuid in opt_out_uuids:
                    opt_out_membership_groups_uuids += 1
                    continue

                consented_membership_groups_uuids += 1
                weekly_advert_uuids.add(uuid)

        log.info(f"Found {opt_out_membership_groups_uuids} membership_groups_uuids who have opted out")
        log.info(f"Added {consented_membership_groups_uuids} membership_groups_uuids to advert uuids")

    return weekly_advert_uuids


def _generate_non_relevant_advert_uuids_by_dataset(participants_by_column, analysis_dataset_config):
    '''
    Generates non relevant advert UUIDS for each episode.

    :param participants_by_column: list of participants column view Traced Data object to generate the uuids from.
    :type participants_by_column: list of core_data_modules.traced_data.TracedData
    :param analysis_dataset_config: Analysis configuration to generate non relevant uuids for.
    :type analysis_dataset_config: src.engagement_db_to_analysis.configuration.AnalysisConfiguration.dataset_configurations
    :return non_relevant_uuids : A map of dataset_name -> uuids who sent messages labelled with non relevant themes.
    :rtype non_relevant_uuids: dict of str -> list of str
    '''

    non_relevant_uuids= set()
    for participant_td in participants_by_column:
        if participant_td["consent_withdrawn"] == Codes.TRUE:
            continue

        for coding_config in analysis_dataset_config.coding_configs:
            label_key = f'{coding_config.analysis_dataset}_labels'

            # TODO: Move this to coding_config_to_column_config()
            analysis_configurations = core_data_analysis_config(
                analysis_dataset_config.raw_dataset,
                analysis_dataset_config.raw_dataset,
                label_key,
                coding_config.code_scheme
            )

            codes = analysis_utils.get_codes_from_td(participant_td, analysis_configurations)
            if not analysis_utils.relevant(participant_td, "consent_withdrawn", analysis_configurations):
                for code in codes:
                    if code.string_value in ["opt_in"]: #TODO Move this to config?
                        non_relevant_uuids.add(participant_td["participant_uuid"])

    return non_relevant_uuids


def _convert_uuids_to_urns(uuids_group, uuid_table):
    """
    Converts a list of UUIDs to their respective rapid_pro urns.

    :param uuids_group: set of participant UUIDs to convert.
    :type uuids_group: set of participant UUIDs.
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable
    :return urns: a set of de-identified urn.
    :rtype: set of str
    """

    log.info(f"Converting {len(uuids_group)} uuids to urns...")
    urn_lut = uuid_table.uuid_to_data_batch(uuids_group)
    urns = {urn_lut[uuid] for uuid in uuids_group}
    log.info(f"Converted {len(uuids_group)} uuids to {len(urns)} urns")

    return urns


#Todo: standardize and move to rapidpro tools
def _ensure_contact_field_exists(workspace_contact_fields, contact_field, rapid_pro):
    """
    Checks if a workspace contains the target contact field, creates one otherwise.

    :param workspace_contact_fields: All contact fields in the rapid_pro workspace.
    :type workspace_contact_fields: list of temba_client.v2.types.Field
    :param contact_field: Keys of the contact fields to make sure exist.
    :type contact_field: src.engagement_db_to_rapid_pro.configuration.ContactField
    :param rapid_pro: Rapid Pro client to check the contact fields from.
    :type rapid_pro: rapid_pro_tools.rapid_pro.RapidProClient
    """
    workspace_contact_field_keys = [cf.key for cf in workspace_contact_fields]
    if contact_field.key not in workspace_contact_field_keys:
        rapid_pro.create_field(field_id=contact_field.key, label=contact_field.label)


def _sync_advert_contacts_fields_to_rapid_pro(cache, target_uuids, advert_contact_field_key, uuid_table, rapid_pro):
    '''
    Updates the advert contact field for the target urns.

    :param cache: An instance of AnalysisCache to get uuids synced in previous pipeline run and set uuids synced in this session.
    :param type: AnalysisCache
    :param target_uuids: Set containing all uuids for the target context e.g opt_out uuids, weekly advert uuids.
    :type target_uuids: set of str
    :param advert_contact_field_key: Key of the contact field to update for the advert urns in rapid_pro.
    :type advert_contact_field_key: str
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable.
    :param rapid_pro: Rapid Pro client to sync the groups to.
    :type rapid_pro: rapid_pro_tools.rapid_pro.RapidProClient
    '''

    synced_uuids = []
    if cache is not None:
        previously_synced_non_relevant_uuids = cache.get_synced_uuids(advert_contact_field_key)
        log.info(f'Found {len(previously_synced_non_relevant_uuids)} uuids whose {advert_contact_field_key} contact '
                 f'field was synced in previous pipeline run...')
        synced_uuids.extend(previously_synced_non_relevant_uuids)

    # If cache is available, check for uuids to sync in the current pipeline run.
    uuids_to_sync = target_uuids - set(synced_uuids)

    if len(uuids_to_sync) > 0:
        log.info(f'Syncing {len(uuids_to_sync)} urns in this run ')
        # Re-identify the uuids.
        urns_to_sync = _convert_uuids_to_urns(uuids_to_sync, uuid_table)
        # Update the advert contact field for the target urns.
        for urn in urns_to_sync:
            rapid_pro.update_contact(urn, contact_fields={advert_contact_field_key: "yes"})
            synced_uuids.append(uuid_table.data_to_uuid(urn))

            if cache is not None:
                cache.set_synced_uuids(advert_contact_field_key, synced_uuids)

    else:
        assert len(uuids_to_sync) == 0
        log.info("Found 0 uuids to sync in this run skipping...")


def sync_advert_contacts_to_rapid_pro(participants_by_column, uuid_table, pipeline_config, rapid_pro,
                                     google_cloud_credentials_file_path, membership_group_dir_path, cache_path):
    '''
    Syncs advert contacts to rapid_pro by:
      1. Updating the contact field for weekly advert urns who are in the participants_by_column or a listening group and
         have not opted out.
      2. Updating the by dataset non relevant contact fields for the urns who sent a message(s) that was labelled under the
         non relevant themes and have not opted out.

    :param participants_by_column: Participants column view Traced Data object to generate the uuids from.
    :type participants_by_column: list of core_data_modules.traced_data.TracedData
    :param uuid_table: UUID table to use to de-identify contact urns.
    :type uuid_table: id_infrastructure.firestore_uuid_table.FirestoreUuidTable.
    :param pipeline_config: Pipeline configuration to derive configurations needed for the sync functions.
    :type pipeline_config: PipelineConfiguration.
    :param rapid_pro: Rapid Pro client to sync the contact fields to.
    :type rapid_pro: rapid_pro_tools.rapid_pro.RapidProClient
    :param google_cloud_credentials_file_path: Path to the Google Cloud service account credentials file to use to
                                               access the credentials bucket.
    :type google_cloud_credentials_file_path: str.
    :param membership_group_dir_path: Path to directory containing de-identified membership groups CSVs containing membership groups data
                        stored as `avf-participant-uuid` column.
    :type: membership_group_dir_path: str.
    :param cache_path: Path to a directory to get uuids synced in previous pipeline run and set uuids synced in this session.
    :type cache_path: str
    '''

    if cache_path is None:
        cache = None
        log.warning(f"No `cache_path` provided. This tool will perform a full sync of advert contacts to rapidpro")
    else:
        log.info(f"Initialising EngagementAnalysisCache at '{cache_path}'")
        cache = AnalysisCache(f"{cache_path}")

    weekly_advert_uuids = _generate_weekly_advert_uuids(
        participants_by_column, pipeline_config.analysis,
        google_cloud_credentials_file_path, membership_group_dir_path
    )

    # Get workspace contact fields to check whether our target contact field exists
    workspace_contact_fields = rapid_pro.get_fields()

    log.info(f'Syncing weekly advert contacts to rapid pro...')
    weekly_advert_contact_field = pipeline_config.rapid_pro_target.sync_config.weekly_advert_contact_field
    _ensure_contact_field_exists(workspace_contact_fields, weekly_advert_contact_field, rapid_pro)

    _sync_advert_contacts_fields_to_rapid_pro(cache, weekly_advert_uuids, weekly_advert_contact_field.key, uuid_table,
                                              rapid_pro)

    #Update  dataset non relevant groups to rapid_pro
    log.info(f'Syncing contacts who sent non relevant messages for each episode...')
    for analysis_dataset_config in pipeline_config.analysis.dataset_configurations:
        if analysis_dataset_config.rapid_pro_non_relevant_field is not None:
            non_relevant_uuids = _generate_non_relevant_advert_uuids_by_dataset(participants_by_column,
                                                                                analysis_dataset_config)

            _ensure_contact_field_exists(workspace_contact_fields, analysis_dataset_config.rapid_pro_non_relevant_field,
                                         rapid_pro)

            _sync_advert_contacts_fields_to_rapid_pro(cache, non_relevant_uuids,
                                                      analysis_dataset_config.rapid_pro_non_relevant_field.key,
                                                      uuid_table, rapid_pro)
