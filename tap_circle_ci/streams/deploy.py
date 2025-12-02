"""tap-circle-ci deploy environments stream module."""

from typing import Dict, Iterator
from datetime import datetime

from singer import (
    Transformer,
    get_logger,
    metrics,
    write_record,
)

from .abstracts import IncrementalStream

LOGGER = get_logger()


class Deploy(IncrementalStream):
    """Incremental stream for CircleCI Deploy Environments."""

    stream = "deploy"
    tap_stream_id = "deploy"
    key_properties = ["id"]
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    url_endpoint = "https://circleci.com/api/v2/deploy/environments?org-id={org_id}&page-size={page_size}"
    project = None

    def get_url(self) -> str:
        org_id = self.client.config.get("org_id")
        page_size = self.client.config.get("page_size", 200)
        if not org_id:
            raise Exception("Missing 'org_id' in config.json")
        return self.url_endpoint.format(org_id=org_id, page_size=page_size)

    def get_records(self) -> Iterator[Dict]:
        url = self.get_url()
        params = {}
        with metrics.Counter("page_count") as counter:
            while True:
                response = self.client.get(url, params, {})
                counter.increment()
                items = response.get("items", [])
                next_page_token = response.get("next_page_token")
                if not items:
                    break
                for record in items:
                    yield record
                if not next_page_token:
                    break
                params["page-token"] = next_page_token

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        LOGGER.info("Starting Deploy incremental sync")
        current_bookmark = self.get_bookmark(state)  # could be False if first run
        max_bookmark = current_bookmark

        def parse_dt(v):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record_bookmark_val = record.get(self.replication_key)
                if not record_bookmark_val:
                    continue
                if not current_bookmark:
                    is_new_record = True
                else:
                    try:
                        is_new_record = parse_dt(record_bookmark_val) > parse_dt(current_bookmark)
                    except Exception as e:
                        LOGGER.warning(
                            f"Failed to compare bookmark: {record_bookmark_val} vs {current_bookmark}. Error: {e}")
                        is_new_record = True
                if is_new_record:
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()
                    if not max_bookmark or parse_dt(record_bookmark_val) > parse_dt(max_bookmark):
                        max_bookmark = record_bookmark_val
        if max_bookmark:
            state = self.write_bookmark(state, None, max_bookmark)
            LOGGER.info("Updated deploy bookmark to: %s", max_bookmark)

        LOGGER.info("Completed Deploy incremental sync")
        return state
