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
    def __init__(self, offset_date, max_id=None):
        self.offset_date = offset_date
        self.max_id = max_id

    def to_dict(self):
        return {
            "offset_date": isoparse(self.offset_date),
            "max_id": isoparse(self.max_id)
        }
