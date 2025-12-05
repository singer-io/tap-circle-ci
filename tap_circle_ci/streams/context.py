"""tap-circle-ci context stream module."""
from typing import Dict, Iterator, List
from singer.utils import strftime, strptime_to_utc

from singer import (
    Transformer,
    clear_bookmark,
    get_bookmark,
    get_logger,
    metrics,
    write_record,
    write_state,
)

from .abstracts import FullTableStream
from .workflows import Workflows

LOGGER = get_logger()


class Context(FullTableStream):
    """Full-table stream for CircleCI contexts."""

    stream = "context"
    tap_stream_id = "context"
    key_properties = ["id"]
    url_endpoint = "https://circleci.com/api/v2/project/PROJECT_PATH/context"
    project = None

    def get_url_endpoint(self) -> str:
        """Returns a formatted endpoint using the stream attributes."""
        # return self.url_endpoint.replace("PROJECT_PATH", self.project)
        return "https://circleci.com/api/v2/context?owner-id=358ac4ed-dd17-4991-a8e5-f99a5b3811e6&owner-type=organization"

    def get_records(self) -> Iterator[Dict]:
        extraction_url = self.get_url_endpoint()
        params = {}
        with metrics.Counter("page_count") as page_counter:
            while True:
                response = self.client.get(extraction_url, params, {})
                page_counter.increment()
                next_page_token = response.get("next_page_token")
                raw_records = response.get("items", [])
                if not raw_records:
                    break
                params["page-token"] = next_page_token
                yield from raw_records
                if next_page_token is None:
                    break

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
