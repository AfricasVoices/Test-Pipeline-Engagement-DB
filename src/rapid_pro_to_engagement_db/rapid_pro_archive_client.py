import json

from core_data_modules.logging import Logger
from temba_client.v2 import Org, Flow, Run, Contact


log = Logger(__name__)


class RapidProArchiveClient:
    def __init__(self, archive_dir):
        """
        A reimplementation of RapidProClient which operates on a Rapid Pro archive rather than connecting to a
        production Rapid Pro workspace. Contains only the functions needed to run the Rapid Pro -> engagement db sync.

        :param archive_dir: Path to a Rapid Pro archive directory created by RapidProClient.export_all_data.
        :type archive_dir: str
        """
        self.archive_dir = archive_dir

    def _get_org(self):
        with open(f"{self.archive_dir}/org.json") as f:
            return Org.deserialize(json.load(f))

    def get_workspace_name(self):
        return self._get_org().name

    def get_workspace_uuid(self):
        return self._get_org().uuid

    def get_flow_id(self, flow_name):
        with open(f"{self.archive_dir}/flows.jsonl") as f:
            flows = [Flow.deserialize(json.loads(d)) for d in f]

        matching_flows = [f for f in flows if f.name == flow_name]

        if len(matching_flows) == 0:
            available_flow_names = [f.name for f in flows]
            raise KeyError(f"Requested flow not found on RapidPro (Available flows: {', '.join(available_flow_names)})")
        if len(matching_flows) > 1:
            raise KeyError("Non-unique flow name")

        return matching_flows[0].uuid

    def get_raw_runs(self, flow_id, last_modified_after_inclusive=None):
        log.info(f"Loading raw runs for flow {flow_id}, modified after {last_modified_after_inclusive}, from archives...")
        with open(f"{self.archive_dir}/runs.jsonl") as f:
            all_runs = [Run.deserialize(json.loads(d)) for d in f]

        # Filter for runs that are for the requested flow and, optionally, last modified since the requested date.
        filtered_runs = []
        for run in all_runs:
            if run.flow.uuid != flow_id:
                continue
            if last_modified_after_inclusive is not None and run.modified_on < last_modified_after_inclusive:
                continue
            filtered_runs.append(run)

        filtered_runs.sort(key=lambda r: r.modified_on)

        log.info(f"Returning {len(filtered_runs)} runs")
        return filtered_runs

    def update_raw_contacts_with_latest_modified(self, prev_raw_contacts=None, raw_export_log_file=None):
        # Note: This function contains unused arguments because it's re-implementing the same interface as,
        # RapidProClient, where they are used. These arguments don't make sense when reading from archives though,
        # so silently ignoring them.
        log.info(f"Loading contacts from archives...")
        with open(f"{self.archive_dir}/contacts.jsonl") as f:
            contacts = [Contact.deserialize(json.loads(d)) for d in f]
        log.info(f"Loaded {len(contacts)} contacts")
        return contacts
