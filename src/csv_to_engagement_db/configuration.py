from datetime import datetime
import pytz


_MIN_DATE_UTC = pytz.timezone("utc").localize(datetime.min)
_MAX_DATE_UTC = pytz.timezone("utc").localize(datetime.max)


class CSVDatasetConfiguration:
    def __init__(self, engagement_db_dataset, start_date=_MIN_DATE_UTC, end_date=_MAX_DATE_UTC):
        """
        Configuration for an engagement db dataset to sync csv messages to.

        To restrict messages to this dataset by message timestamps, optionally set the `start_date`/`end_date`
        properties.

        :param engagement_db_dataset: Name of the dataset to use in the engagement database.
        :type engagement_db_dataset: str
        :param start_date: Start date for this dataset configuration.
        :type start_date: datetime.datetime
        :param end_date: End date for this dataset configuration.
        :type end_date: datetime.datetime
        """
        self.engagement_db_dataset = engagement_db_dataset
        self.start_date = start_date
        self.end_date = end_date

    def to_dict(self, serialize_datetimes_to_str=False):
        return {
            "engagement_db_dataset": self.engagement_db_dataset,
            "start_date": self.start_date.isoformat() if serialize_datetimes_to_str else self.start_date,
            "end_date": self.end_date.isoformat() if serialize_datetimes_to_str else self.end_date
        }


class CSVSource:
    def __init__(self, gs_url, engagement_db_datasets, timezone):
        """
        Configuration for a CSV data source. The CSV should have the headings 'Sender', 'Message', and 'ReceivedOn'.

        :param gs_url: Google Cloud Storage URL to the csv file.
        :type gs_url: str
        :param engagement_db_datasets: Configuration for the engagement db datasets for this csv.
        :type engagement_db_datasets: list of CSVDatasetConfiguration
        :param timezone: Timezone to interpret the csv's timestamps in e.g. 'Africa/Nairobi'.
        :type timezone: str
        """
        self.gs_url = gs_url
        self.engagement_db_datasets = engagement_db_datasets
        self.timezone = timezone

    def get_dataset_for_timestamp(self, timestamp):
        """
        Gets the engagement db dataset for a message with the given timestamp, by searching the `engagement_db_datasets`
        configurations for one which covers this time range.

        Raises a LookupError if no matching dataset was found for this timestamp.
        Raises an AssertionError if the timestamp matches multiple time ranges.

        :param timestamp: Timestamp to get the engagement db dataset for.
        :type timestamp: datetime.datetime
        :return: Engagement db dataset for this timestamp.
        :rtype: str
        """
        matching_dataset = None
        for dataset in self.engagement_db_datasets:
            if dataset.start_date <= timestamp < dataset.end_date:
                assert matching_dataset is None, f"Timestamp {timestamp} matches multiple dataset time ranges"
                matching_dataset = dataset.engagement_db_dataset

        if matching_dataset is not None:
            return matching_dataset

        # No matching time range was found.
        raise LookupError(timestamp)

    def to_dict(self, serialize_datetimes_to_str=False):
        if self.engagement_db_datasets is None:
            serialized_engagement_db_datasets = None
        else:
            serialized_engagement_db_datasets = [
                dataset.to_dict(serialize_datetimes_to_str) for dataset in self.engagement_db_datasets
            ]

        return {
            "gs_url": self.gs_url,
            "engagement_db_datasets": serialized_engagement_db_datasets,
            "timezone": self.timezone
        }
