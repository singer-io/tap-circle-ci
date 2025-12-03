"""tap-circle-ci schedule stream module."""
from typing import Dict, List

from singer import (
    get_logger,
    metrics,
    write_record,
)

from .abstracts import IncrementalStream

LOGGER = get_logger()


class Schedule(IncrementalStream):
    """Schedule incremental stream."""

    stream = "schedule"
    tap_stream_id = "schedule"
    key_properties = ["id"]
    replication_key = "updated-at"
    valid_replication_keys = ["updated-at"]

    # Hardcoded URL — you can replace org/repo using config.json later
    url_endpoint = "https://circleci.com/api/v2/project/gh/singer-io/singer-python/schedule"

    def get_records(self, state=None) -> List[Dict]:
        """
        Fetch schedule records from CircleCI with pagination.
        """

        all_records = []
        params = {}

        while True:
            response = self.client.get(self.url_endpoint, params, {})

            items = response.get("items", [])
            all_records.extend(items)

            next_token = response.get("next_page_token")
            if not next_token:
                break

            params["page-token"] = next_token

        return all_records

    def sync(self, state, schema, stream_metadata, transformer):
        LOGGER.info("Starting Schedule incremental sync")

        records = self.get_records(state)

        with metrics.Timer(self.tap_stream_id, None):
            with metrics.Counter(self.tap_stream_id) as counter:

                for record in records:
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

        # Update bookmark with max(updated_at)
        if state is None:
            state = {}

        if records:
            latest = max(
                r.get(self.replication_key)
                for r in records
                if r.get(self.replication_key)
            )

            state[self.tap_stream_id] = {self.replication_key: latest}

        return state
