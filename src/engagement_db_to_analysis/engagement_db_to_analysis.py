from dateutil.parser import isoparse

from core_data_modules.logging import Logger
from core_data_modules.util import TimeUtils

from src.engagement_db_to_analysis.cache import EngagementAnalysisCache

log = Logger(__name__)


def serialise_message(msg):

    msg = msg.to_dict()
    msg["timestamp"] = TimeUtils.datetime_to_utc_iso_string(msg["timestamp"])
    msg["last_updated"] = TimeUtils.datetime_to_utc_iso_string(msg["last_updated"])

    return msg

def get_project_messages_from_engagement_db(dataset_configurations, engagement_db, cache_path=None):
    """

    Downloads project messages from engagement database. It performs a full download if there is no previous export and
    incrementally otherwise.

    :param pipeline_config: Dataset configuration in pipeline configuration module.
    :type pipeline_config: pipeline_config.coda_sync.sync_config.dataset_configurations
    :param engagement_db: Engagement database to download the messages from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param cache_path: Directory containing engagement_db dataset files and a timestamp generated from a previous export.
    :type cache_path: str
    :return: engagement_db_dataset_messages_map of engagement_db_dataset to list of messages.
    :rtype: dict of str -> list of Message
    """

    log.info(f"Initialising EngagementAnalysisCache at'{cache_path}'")
    cache = EngagementAnalysisCache(cache_path)

    engagement_db_dataset_messages_map = {} # of engagement_db_dataset to list of messages
    for dataset_config in dataset_configurations:

        engagement_db_dataset = dataset_config.engagement_db_dataset
        latest_message_timestamp = cache.get_latest_message_timestamp(engagement_db_dataset)
        cache_messages = cache.get_previous_export_messages(engagement_db_dataset)

        messages = []
        if latest_message_timestamp is not None:

            log.info(f"Downloading {engagement_db_dataset} messages created after the previous run...")
            cache_message_ids = set()
            for msg in cache_messages:
                cache_message_ids.add(msg["message_id"])

            new_messages_filter = lambda q: q \
                .where("dataset", "==", engagement_db_dataset) \
                .where("message_id", "not-in", cache_message_ids)

            new_messages = engagement_db.get_messages(filter=new_messages_filter)
            messages.extend(serialise_message(msg) for msg in new_messages)
            log.debug(f"Downloaded {len(new_messages)} new messages")

            log.info(f"Downloading {engagement_db_dataset} messages updated after the previous run...")
            # 1. Download messages that have been updated after the previous run.
            # 2. Check for messages that have been moved to a different dataset after the previous run through ws correction.
            # 3. Remove those have been moved.
            # 4. Update those with updated msg properties e.g labels or status.
            updated_messages_filter = lambda q: q \
                .where("message_id", "in", cache_message_ids) \
                .where("last_updated", ">", latest_message_timestamp)

            updated_messages = engagement_db.get_messages(filter=updated_messages_filter)
            updated_message_count = 0
            moved_messages_count = 0
            for msg in updated_messages:
                if msg.dataset != engagement_db_dataset:
                    moved_messages_count += 1
                    continue
                messages.append(serialise_message(msg))
                updated_message_count += 1

            log.debug(f"{moved_messages_count} {engagement_db_dataset} cache message(s) moved to a different dataset")
            log.debug(f"Updated {updated_message_count} cache message(s)")

            # Retain cache messages that have not been updated in engagement db
            updated_message_ids = {msg.message_id for msg in updated_messages}
            for msg in cache_messages:
                if msg["message_id"] not in updated_message_ids:
                    messages.append(msg)
        else:
            log.warning(f"{engagement_db_dataset} previous export file does not exist, "
                        f"performing a full download ...")

            full_download_filter = lambda q: q \
                .where("dataset", "==", engagement_db_dataset)

            messages.extend(msg.to_dict() for msg in engagement_db.get_messages(filter=full_download_filter))

        engagement_db_dataset_messages_map[engagement_db_dataset] = messages

        for msg in messages:
            # Update latest_message_timestamp
            if latest_message_timestamp is None or isoparse(msg["last_updated"]) > latest_message_timestamp:
                latest_message_timestamp = isoparse(msg["last_updated"])

        # Export latest message timestamp to cache
        cache.set_latest_message_timestamp(engagement_db_dataset, latest_message_timestamp)

        # Export project engagement_dataset files
        cache.export_engagement_db_dataset(engagement_db_dataset, messages)

    return engagement_db_dataset_messages_map

#TODO: Filter Messages
def filter_messages():
    return None

#TODO: Fold messages by uid
def fold_messages_by_uid():
    return None
