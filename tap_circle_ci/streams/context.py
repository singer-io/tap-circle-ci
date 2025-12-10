"""tap-circle-ci context stream module."""
from typing import Dict, Iterator
from singer import Transformer, get_logger, metrics, write_record
from .abstracts import FullTableStream

LOGGER = get_logger()


class Context(FullTableStream):
    """Full-table stream for CircleCI contexts."""

    stream = "context"
    tap_stream_id = "context"
    key_properties = ["id"]
    url_endpoint = "https://circleci.com/api/v2/context?owner-id={organization_id}&owner-type=organization"
    project = False
    parent_stream = "collaborations"

    def get_org_ids(self):
        """Fetch org IDs from the Collaborations stream."""
        if not hasattr(self.client, "shared_collaborations_ids"):
            raise Exception(
                "Collaborations data not available yet. Make sure Collaborations sync runs first."
            )
        return self.client.shared_collaborations_ids.get("collaborations", [])

    def get_url(self, organization_id: str) -> str:
        """Return the URL for the given org."""
        return self.url_endpoint.format(organization_id=organization_id)

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Full-table sync, no bookmarks."""
        pipeline_ids = []

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                pipeline_ids.append(record["id"])
                transformed_record = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed_record)
                counter.increment()

        if self.client.shared_pipeline_ids is None:
            self.client.shared_pipeline_ids = {}
        self.client.shared_pipeline_ids.update({self.project: pipeline_ids})

        # Full-table stream, state doesn't change
        return state
