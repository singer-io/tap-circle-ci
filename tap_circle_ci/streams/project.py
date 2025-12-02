"""tap-circle-ci project stream module."""
from singer import (
    metrics,
    write_record,
    write_state,
    get_logger
)

from .abstracts import FullTableStream

LOGGER = get_logger()


class Project(FullTableStream):
    """Full-table Project stream."""

    stream = "project"
    tap_stream_id = "project"
    key_properties = ["id"]
    url_endpoint = "https://circleci.com/api/private/project?organization-id={org_id}"

    def get_records(self) -> list:
        org_id = self.client.config.get("org_id")
        url = self.url_endpoint.format(org_id=org_id)
        all_records = []
        params = {}
        while True:
            response = self.client.get(url, params, {})
            items = response.get("items", [])
            all_records.extend(items)
            next_token = response.get("next_page_token")
            if not next_token:
                break
            params["page-token"] = next_token
        return all_records

    def sync(self, state, schema, stream_metadata, transformer):
        LOGGER.info("Starting Project full-table sync")

        records = self.get_records()

        LOGGER.info("Found {} records".format(records))

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
