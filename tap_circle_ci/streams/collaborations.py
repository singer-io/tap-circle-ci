from typing import List, Dict
from singer import metrics, write_record, get_logger
from .abstracts import FullTableStream

LOGGER = get_logger()


class Collaborations(FullTableStream):
    """Full-table Collaborations stream (acts as parent for Deploy)."""

    stream = "collaborations"
    tap_stream_id = "collaborations"
    key_properties = ["id"]
    url_endpoint = "https://circleci.com/api/v2/me/collaborations"
    requires_project = False

    def get_records(self) -> List[Dict]:
        """Fetch all organizations/collaborations from CircleCI API."""
        response = self.client.get(self.url_endpoint, {}, {})
        if not isinstance(response, list):
            raise Exception(f"Unexpected collaborations response: {response}")
        return response

    def sync(self, state, schema, stream_metadata, transformer):
        LOGGER.info("Starting Collaborations full-table sync")
        records = self.get_records()

        with metrics.Timer(self.tap_stream_id, None):
            with metrics.Counter(self.tap_stream_id) as counter:
                for record in records:
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

        # Store org IDs for downstream streams (like Deploy)
        collab_ids = [r["id"] for r in records if "id" in r]
        if not hasattr(self.client, "shared_collaborations_ids"):
            self.client.shared_collaborations_ids = {}
        self.client.shared_collaborations_ids[self.tap_stream_id] = collab_ids

        return state
