from typing import Dict, Iterator, List
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

    def get_url(self, org_id: str) -> str:
        """Build the URL for each org."""
        return self.url_endpoint.format(org_id=org_id)

    def get_records(self) -> Iterator[Dict]:
        """Fetch all project records for each org (supports pagination)."""
        org_ids = self.get_org_ids()
        with metrics.Counter("page_count") as counter:
            for org_id in org_ids:
                url = self.get_url(org_id)
                # ---- First page ----
                params = {}
                response = self.client.get(url, params, {})
                counter.increment()
                items = response.get("items", [])
                next_page_token = response.get("next_page_token")

                for item in items:
                    item["organization_id"] = org_id
                    yield item
                # ---- Subsequent pages ----
                while next_page_token:
                    params = {"page-token": next_page_token}
                    response = self.client.get(url, params, {})
                    counter.increment()
                    items = response.get("items", [])
                    next_page_token = response.get("next_page_token")
                    for item in items:
                        item["organization_id"] = org_id
                        yield item

    def sync(self, state, schema, stream_metadata, transformer):
        all_records = []
        with metrics.Timer(self.tap_stream_id, None):
            with metrics.Counter(self.tap_stream_id) as counter:
                for record in self.get_records():
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    all_records.append({"id": record["id"], "slug": record["slug"]})
                    counter.increment()

        # Store both id and slug for other streams to use
        if not hasattr(self.client, "shared_project_ids"):
            self.client.shared_project_ids = {}
        self.client.shared_project_ids[self.tap_stream_id] = all_records
        return state
