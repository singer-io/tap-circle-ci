"""tap-circle-ci groups stream module."""
from typing import Dict, Iterator

from singer import (
    Transformer,
    get_logger,
    metrics,
    write_record,
)

from .abstracts import FullTableStream

LOGGER = get_logger()


class Groups(FullTableStream):
    """Full-table Groups stream (child of Collaborations)."""

    stream = "groups"
    tap_stream_id = "groups"
    key_properties = ["id"]
    url_endpoint = "https://circleci.com/api/v2/organizations/{organization_id}/groups"
    project = False
    parent_stream = "collaborations"

    def get_org_ids(self):
        """Fetch org IDs from the parent Collaborations stream."""
        if not hasattr(self.client, "shared_collaborations_ids"):
            raise Exception(
                "Collaborations data not available yet. Make sure Collaborations sync runs first."
            )
        return self.client.shared_collaborations_ids.get(self.parent_stream, [])

    def get_url(self, organization_id: str) -> str:
        """Build API endpoint URL for a given org_id."""
        return self.url_endpoint.format(organization_id=organization_id)

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Perform sync for the Groups stream."""
        LOGGER.info("Starting Groups full-table sync")
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed)
                counter.increment()
        LOGGER.info("Completed Groups full-table sync")
        return state
