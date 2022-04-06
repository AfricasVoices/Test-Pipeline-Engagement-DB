from dateutil.parser import isoparse


class TelegramGroupSource:
    def __init__(self, token_file_url, datasets):
        self.token_file_url = token_file_url
        self.datasets = datasets

    def to_dict(self):
        return {
            "token_file_url": self.token_file_url,
            "datasets": [dataset for dataset in self.datasets]
        }


class TelegramGroupDataset:
    def __init__(self, engagement_db_dataset, search):
        self.engagement_db_dataset = engagement_db_dataset
        self.search = search


    def to_dict(self):
        return {
            "engagement_db_dataset":self.engagement_db_dataset,
            "search":self.search
        }


class TelegramGroupSearch:
    def __init__(self, group_ids, start_date, end_date, min_id=None):
        self.group_ids = group_ids
        self.start_date = start_date
        self.end_date = end_date
        self.min_id = min_id

    def to_dict(self):
        return {
            "group_ids": self.group_ids,
            "start_date": isoparse(self.start_date),
            "end_date": isoparse(self.end_date),
            "min_id": self.min_id
        }
