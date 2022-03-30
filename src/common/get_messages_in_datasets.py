from core_data_modules.logging import Logger

log = Logger(__name__)


def get_messages_in_datasets(engagement_db, engagement_db_datasets, cache=None):
    """
    Gets messages in the specified datasets.

    :param engagement_db: Engagement database to fetch messages from.
    :type engagement_db: engagement_database.EngagementDatabase
    :param engagement_db_datasets: Datasets to download.
    :type engagement_db_datasets: iterable of str
    :param cache: Cache to use, or None. If None, downloads all messages from the engagement database. If a cache is
                  specified, writes all the fetched messages to the cache and only queries for messages changed since
                  the most recently updated message in the cache.
    :type cache: src.common.cache.Cache
    :return: Dictionary of engagement db dataset -> list of Messages in dataset.
    :rtype: dict of str -> list of engagement_database.data_models.Message
    """
    engagement_db_messages_map = dict()  # of engagement db dataset -> list of Message

    for engagement_db_dataset in engagement_db_datasets:
        messages = []
        latest_message_timestamp = None if cache is None else cache.get_date_time(engagement_db_dataset)
        full_download_required = latest_message_timestamp is None
        if not full_download_required:
            log.info(f"Performing incremental download for {engagement_db_dataset} messages...")

            # Download messages that have been updated/created after the previous run
            incremental_messages_filter = lambda q: q \
                .where("dataset", "==", engagement_db_dataset) \
                .where("last_updated", ">", latest_message_timestamp)

            updated_messages = engagement_db.get_messages(firestore_query_filter=incremental_messages_filter)
            messages.extend(updated_messages)

            # Check and remove cached messages that have been ws corrected away from this dataset after the previous
            # run. We do this by searching for all messages that used to be in this dataset, that we haven't
            # already seen.
            latest_ws_message_timestamp = cache.get_date_time(f"{engagement_db_dataset}_ws")
            ws_corrected_messages_filter = lambda q: q \
                .where("previous_datasets", "array_contains", engagement_db_dataset) \
                .where("last_updated", ">", latest_ws_message_timestamp)

            downloaded_ws_corrected_messages = engagement_db.get_messages(firestore_query_filter=ws_corrected_messages_filter)

            # Filter ws_corrected_messages whose dataset == the engagement_db_dataset.
            # This prevents messages that have the current dataset in their previous_datasets from being erroneously
            # removed.
            ws_corrected_messages = [msg for msg in downloaded_ws_corrected_messages if msg.dataset != engagement_db_dataset]

            log.info(f"Downloaded {len(updated_messages)} updated messages in this dataset, "
                     f"{len(ws_corrected_messages)} messages that were previously in this dataset but have moved.")
            log.debug(f"Also downloaded {len(downloaded_ws_corrected_messages) - len(ws_corrected_messages)} messages "
                      f"that have this dataset in .dataset and .previous_datasets simultaneously. "
                      f"Not moving these messages")

            # Update the latest seen ws message from this dataset
            if len(downloaded_ws_corrected_messages) > 0:
                for msg in downloaded_ws_corrected_messages:
                    if latest_ws_message_timestamp is None or msg.last_updated > latest_ws_message_timestamp:
                        latest_ws_message_timestamp = msg.last_updated
                cache.set_date_time(f"{engagement_db_dataset}_ws", latest_ws_message_timestamp)

            cache_messages = cache.get_messages(engagement_db_dataset)
            for msg in cache_messages:
                if msg.message_id in {msg.message_id for msg in ws_corrected_messages}:
                    continue
                messages.append(msg)
        else:
            log.warning(f"Performing a full download for {engagement_db_dataset} messages...")

            full_download_filter = lambda q: q \
                .where("dataset", "==", engagement_db_dataset)

            messages = engagement_db.get_messages(firestore_query_filter=full_download_filter)
            log.info(f"Downloaded {len(messages)} messages")

        # Filter messages for their latest versions
        # (Filter by sorting in reverse order of last_updated and keeping the first snapshot we see of each id)
        latest_messages = []
        seen_message_ids = set()
        messages.sort(key=lambda msg: msg.last_updated, reverse=True)
        for msg in messages:
            if msg.message_id not in seen_message_ids:
                seen_message_ids.add(msg.message_id)
                latest_messages.append(msg)

        log.info(f"Filtered for latest message snapshots: {len(latest_messages)}/{len(messages)} snapshots remain")
        messages = latest_messages
        engagement_db_messages_map[engagement_db_dataset] = messages

        # Update latest_message_timestamp
        for msg in messages:
            msg_last_updated = msg.last_updated
            if latest_message_timestamp is None or msg_last_updated > latest_message_timestamp:
                latest_message_timestamp = msg_last_updated

        if cache is not None and latest_message_timestamp is not None:
            # Export latest message timestamp to cache.
            if latest_message_timestamp is not None:
                cache.set_date_time(engagement_db_dataset, latest_message_timestamp)

            if full_download_required:
                # Export this as the ws case too, as there will be no need to check for ws messages that moved from
                # this dataset before this initial fetch.
                cache.set_date_time(f"{engagement_db_dataset}_ws", latest_message_timestamp)

            # Export project engagement_dataset files
            if len(messages) > 0:
                cache.set_messages(engagement_db_dataset, messages)

    # Ensure that origin_ids in the exported messages are all unique. If we have multiple messages with the same
    # origin_id, that means there is a problem with the database or with the cache.
    # (Most likely we added the same message twice or we deleted a message and forgot to delete the analysis cache).
    all_message_origins = set()
    for messages in engagement_db_messages_map.values():
        for msg in messages:
            assert msg.origin.origin_id not in all_message_origins, f"Multiple messages had the same origin id: " \
                                                                    f"'{msg.origin.origin_id}'"
            all_message_origins.add(msg.origin.origin_id)

    return engagement_db_messages_map
