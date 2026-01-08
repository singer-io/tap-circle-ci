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

    def prefetch_collaborations_ids(self) -> List:
        """Helper method to load all collaboration IDs if not already cached.
        Returns list of collaboration IDs for downstream streams.
        """
        if not hasattr(self.client, "shared_collaborations_ids"):
            collaboration_ids = []
            self.client.shared_collaborations_ids = {}
            LOGGER.info("Fetching all collaboration records")
            for record in self.get_records():
                try:
                    collaboration_ids.append(record["id"])
                except KeyError:
                    LOGGER.warning("Unable to find Collaboration ID")
            collaboration_ids.sort()
            self.client.shared_collaborations_ids[self.tap_stream_id] = collaboration_ids

        return self.client.shared_collaborations_ids.get(self.tap_stream_id, [])
