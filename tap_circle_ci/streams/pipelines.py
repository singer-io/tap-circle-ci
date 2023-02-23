"""tap-circle-ci unsubsrcibers stream module."""
from typing import Dict, Iterator, List

from singer import Transformer, get_logger, metrics, write_record
from singer.utils import strftime, strptime_to_utc

from .abstracts import IncrementalStream, UrlEndpointMixin

LOGGER = get_logger()


class Pipelines(IncrementalStream, UrlEndpointMixin):
    """class for `pipeline` stream."""

    stream = "pipelines"
    tap_stream_id = "pipelines"
    key_properties = ["id"]
    url_endpoint = "https://circleci.com/api/v2/project/PROJECT_PATH/pipeline"
    config_start_key = "start_date"
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    project = None

    def get_url_endpoint(self) -> str:
        """Returns a formatted endpoint using the stream attributes."""
        return self.url_endpoint.replace("PROJECT_PATH", self.project)

    def get_records(self) -> Iterator[Dict]:
        extraction_url = self.get_url_endpoint()
        params = {}
        with metrics.Counter("page_count") as page_counter:
            while True:
                response = self.client.get(extraction_url, params, {})
                page_counter.increment()
                next_page_token = response["next_page_token"]
                raw_records = response.get("items", [])
                if not raw_records:
                    break
                params["page-token"] = next_page_token
                yield from raw_records
                if next_page_token is None:
                    break

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Implementation for `type: Incremental` stream."""
        current_bookmark_date = self.get_bookmark(state, f"{self.replication_key}_{self.project}")
        pipeline_ids = []
        max_bookmark = current_bookmark_date_utc = strptime_to_utc(current_bookmark_date)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                pipeline_ids.append(record["id"])
                try:
                    record_timestamp = strptime_to_utc(record[self.replication_key])
                except IndexError as err:
                    LOGGER.error("Unable to process Record, Exception occurred: %s for stream %s", err, self.__class__)
                    continue
                if record_timestamp >= current_bookmark_date_utc:
                    transformed_record = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()
                    max_bookmark = max(max_bookmark, record_timestamp)
            if self.client.shared_pipeline_ids is None:
                self.client.shared_pipeline_ids = {}
            self.client.shared_pipeline_ids.update({self.project: pipeline_ids})
            state = self.write_bookmark(
                state, key=f"{self.replication_key}_{self.project}", value=strftime(max_bookmark)
            )
        return state

    def prefetch_pipeline_ids(self, project) -> List:
        """Helper method implemented for other streams to load all parent
        pipeline_ids.

        eg: pipelines are required to fetch `workflows`
        """
        pipeline_ids = self.client.shared_pipeline_ids or {}
        project_pipeline_ids = []
        self.project = project
        if pipeline_ids and project in pipeline_ids:
            return pipeline_ids[project]
        else:
            LOGGER.info("Fetching all pipeline records")
            for record in self.get_records():
                try:
                    project_pipeline_ids.append(record["id"])
                except KeyError:
                    LOGGER.warning("Unable to find Pipeline ID")
            project_pipeline_ids.sort()
            pipeline_ids.update({project: project_pipeline_ids})
            self.client.shared_pipeline_ids = pipeline_ids
        return project_pipeline_ids
