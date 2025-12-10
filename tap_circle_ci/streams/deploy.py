from typing import Dict, Iterator
from datetime import datetime
from singer import Transformer, get_logger, metrics, write_record
from .abstracts import IncrementalStream, _iter_pages

LOGGER = get_logger()


class Deploy(IncrementalStream):
    """Incremental stream for CircleCI Deploy Environments."""

    stream = "deploy"
    tap_stream_id = "deploy"
    key_properties = ["id"]
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    url_endpoint = "https://circleci.com/api/v2/deploy/environments?org-id={organization_id}&page-size={page_size}"
    requires_project = False
    parent_stream = "collaborations"

    def get_org_ids(self):
        """Fetch org IDs from the Collaborations stream."""
        if not hasattr(self.client, "shared_collaborations_ids"):
            raise Exception(
                "Collaborations data not available yet. Make sure Collaborations sync runs first."
            )
        return self.client.shared_collaborations_ids.get("collaborations", [])

    def get_url(self, organization_id: str) -> str:
        page_size = self.client.config.get("page_size", 200)
        return self.url_endpoint.format(organization_id=organization_id, page_size=page_size)

    def get_records(self) -> Iterator[Dict]:
        """Fetch all records for each organization with pagination."""
        org_ids = self.get_org_ids()
        with metrics.Counter("page_count") as counter:
            for org_id in org_ids:
                url = self.get_url(org_id)
                for page in _iter_pages(self.client.get, url):
                    counter.increment()
                    for record in page.get("items", []):
                        record["organization_id"] = org_id
                        yield record

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        LOGGER.info("Starting Deploy incremental sync")

        current_bookmark = self.get_bookmark(state)
        max_bookmark = current_bookmark

        def parse_datetime(value: str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record_bookmark_val = record.get(self.replication_key)
                if not record_bookmark_val:
                    raise Exception(
                        f"Record missing replication key '{self.replication_key}': {record}"
                    )
                if not current_bookmark:
                    is_new_record = True
                else:
                    try:
                        is_new_record = parse_datetime(record_bookmark_val) > parse_datetime(current_bookmark)
                    except Exception as e:
                        LOGGER.warning(
                            f"Failed to compare bookmark: {record_bookmark_val} vs {current_bookmark}. Error: {e}"
                        )
                        is_new_record = True
                if is_new_record:
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()
                    if not max_bookmark or parse_datetime(record_bookmark_val) > parse_datetime(max_bookmark):
                        max_bookmark = record_bookmark_val

        if max_bookmark:
            state = self.write_bookmark(state, None, max_bookmark)
            LOGGER.info("Updated deploy bookmark to: %s", max_bookmark)

        return state
