from dateutil.parser import isoparse


class FacebookSource:
    def __init__(self, page_id, token_file_url, datasets):
        self.page_id = page_id
        self.token_file_url = token_file_url
        self.datasets = datasets

    def to_dict(self):
        return {
            "page_id": self.page_id,
            "token_file_url": self.token_file_url,
            "datasets": [dataset for dataset in self.datasets]
        }


class FacebookDataset:
    def __init__(self, engagement_db_dataset, post_ids=None, search=None):
        self.engagement_db_dataset = engagement_db_dataset
        self.post_ids = post_ids
        self.search = search

        if search is not None:
            self.search = FacebookSearch.to_dict(search)

        assert self.post_ids is not None or self.search is not None, \
            "Must provide at least a post_id or search"

    def to_dict(self):
        return {
            "engagement_db_dataset":self.engagement_db_dataset,
            "post_ids":self.post_ids,
            "search":self.search
        }


class FacebookSearch:
    def __init__(self, match, start_date, end_date):
        self.match = match
        self.start_date = start_date
        self.end_date = end_date

    def to_dict(self):
        return {
            "match": self.match,
            "start_date": isoparse(self.start_date),
            "end_date": isoparse(self.end_date)
        }
