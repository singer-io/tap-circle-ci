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
    """Stream class for CircleCI Groups."""

    stream = "groups"
    tap_stream_id = "groups"
    key_properties = ["id"]
    url_endpoint = "https://circleci.com/api/v2/organizations/{org_id}/groups"
    project = None

    def get_url_endpoint(self) -> str:
        org_id = self.client.config.get("org_id")
        if not org_id:
            raise Exception("Missing 'org_id' in config.json")
        return self.url_endpoint.format(org_id=org_id)

    def get_records(self) -> Iterator[Dict]:
        url = self.get_url_endpoint()
        params = {}

        with metrics.Counter("page_count") as page_counter:
            while True:
                response = self.client.get(url, params, {})
                page_counter.increment()
                items = response.get("items", [])
                next_page_token = response.get("next_page_token")
                for record in items:
                    yield record
                if not next_page_token:
                    break
                params["page-token"] = next_page_token

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed = transformer.transform(record, schema, stream_metadata)
                write_record(self.tap_stream_id, transformed)
                counter.increment()
        return state
