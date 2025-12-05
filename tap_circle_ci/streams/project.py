from typing import List, Dict
from singer import metrics, write_record, get_logger
from .abstracts import FullTableStream

LOGGER = get_logger()


class Project(FullTableStream):
    """Full-table Project stream (depends on Collaborations)."""

    stream = "project"
    tap_stream_id = "project"
    key_properties = ["id"]
    url_endpoint = "https://circleci.com/api/private/project?organization-id={org_id}"
    parent_stream = "collaborations"

    def get_org_ids(self) -> List[str]:
        """Fetch org IDs from the Collaborations stream."""
        if not hasattr(self.client, "shared_collaborations_ids"):
            raise Exception(
                "Collaborations data not available yet. Make sure Collaborations sync runs first."
            )
        return self.client.shared_collaborations_ids.get(self.parent_stream, [])

    def get_records(self) -> list:
        org_ids = self.get_org_ids()  # get from parent Collaborations
        all_records = []
        params = {}

        for org_id in org_ids:
            url = self.url_endpoint.format(org_id=org_id)
            while True:
                response = self.client.get(url, params, {})
                items = response.get("items", [])
                for item in items:
                    item["org_id"] = org_id  # Add org_id here
                all_records.extend(items)
                next_token = response.get("next_page_token")
                if not next_token:
                    break
                params["page-token"] = next_token

        return all_records

    def sync(self, state, schema, stream_metadata, transformer):
        LOGGER.info("Starting Project full-table sync")
        records = self.get_records()

        with metrics.Timer(self.tap_stream_id, None):
            with metrics.Counter(self.tap_stream_id) as counter:
                for record in records:
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

        project_ids = [r["id"] for r in records if "id" in r]
        if not hasattr(self.client, "shared_project_ids"):
            self.client.shared_project_ids = {}
        self.client.shared_project_ids[self.tap_stream_id] = project_ids

        return state
