class CSVSource:
    def __init__(self, gs_url, engagement_db_dataset, timezone):
        self.gs_url = gs_url
        self.engagement_db_dataset = engagement_db_dataset
        self.timezone = timezone

    def to_dict(self):
        return {
            "gs_url": self.gs_url,
            "engagement_db_dataset": self.engagement_db_dataset,
            "timezone": self.timezone
        }
