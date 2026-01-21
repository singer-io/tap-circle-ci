"""tap-circle-ci product-reviews stream module."""
from datetime import datetime
from typing import Dict, List, Tuple

from singer import (
    Transformer,
    clear_bookmark,
    get_bookmark,
    get_logger,
    metrics,
    write_record,
    write_state,
)
from singer.utils import strftime, strptime_to_utc

from .abstracts import IncrementalStream
from .pipelines import Pipelines

LOGGER = get_logger()


class Workflows(IncrementalStream):
    """class for workflow stream."""

    stream = "workflows"
    tap_stream_id = "workflows"
    key_properties = ["id"]
    replication_key = "created_at"
    valid_replication_keys = ["created_at"]
    config_start_key = "start_date"
    url_endpoint = "https://circleci.com/api/v2/pipeline/PIPELINE_ID/workflow"
    project = None

    def get_pipelines(self, state: Dict) -> Tuple[List, int]:
        """Returns index for sync resuming on interuption."""
        shared_pipeline_ids = Pipelines(self.client).prefetch_pipeline_ids(self.project)
        last_synced = get_bookmark(state, self.tap_stream_id, "currently_syncing", False)
        last_sync_index = 0
        if last_synced:
            for pos, prod_id in enumerate(shared_pipeline_ids):
                if prod_id == last_synced:
                    LOGGER.warning("Last Sync was interrupted after product *****%s", str(prod_id)[-4:])
                    last_sync_index = pos
                    break
        return shared_pipeline_ids, last_sync_index

    def get_records(self, pipeline_id: str, bookmark_date: str) -> Tuple[List, datetime]:
        # pylint: disable=W0221
        """performs api querying and pagination of response."""
        params = {}
        extraction_url = self.url_endpoint.replace("PIPELINE_ID", pipeline_id)
        config_start = self.client.config.get(self.config_start_key, False)
        bookmark_date = bookmark_date or config_start
        bookmark_date = current_max = max(strptime_to_utc(bookmark_date), strptime_to_utc(config_start))
        filtered_records, parent_record_ids = [], []
        while True:
            response = self.client.get(extraction_url, params, {})
            raw_records = response.get("items", [])
            next_page_token = response.get("next_page_token", None)
            if not raw_records:
                break
            for record in raw_records:
                parent_record_ids.append((record["id"], pipeline_id))
                record_timestamp = strptime_to_utc(record[self.replication_key])
                if record_timestamp >= bookmark_date:
                    current_max = max(current_max, record_timestamp)
                    filtered_records.append(record)
            if next_page_token is None:
                break
            params["page-token"] = next_page_token
        return (parent_record_ids, filtered_records, current_max)

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Sync implementation for `product_reviews` stream."""
        # pylint: disable=R0914
        with metrics.Timer(self.tap_stream_id, None):
            pipelines, start_index = self.get_pipelines(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            prod_len = len(pipelines)
            pipeline_wflo_ids = []
            with metrics.Counter(self.tap_stream_id) as counter:
                for index, pipeline_id in enumerate(pipelines[start_index:], max(start_index, 1)):
                    LOGGER.info("Syncing workflows for pipeline *****%s (%s/%s)", pipeline_id[-4:], index, prod_len)
                    bookmark_date = self.get_bookmark(state, pipeline_id)
                    parent_record_ids, records, max_bookmark = self.get_records(pipeline_id, bookmark_date)
                    pipeline_wflo_ids += parent_record_ids
                    for rec in records:
                        write_record(self.tap_stream_id, transformer.transform(rec, schema, stream_metadata))
                        counter.increment()

                    LOGGER.info("Total records synced : %s", len(records))
                    state = self.write_bookmark(state, pipeline_id, strftime(max_bookmark))
                    state = self.write_bookmark(state, "currently_syncing", pipeline_id)
                    write_state(state)
                if self.client.shared_workflow_ids is None:
                    self.client.shared_workflow_ids = {}
            pipeline_wflo_ids.sort(key=lambda x: x[1])
            self.client.shared_workflow_ids.update({self.project: pipeline_wflo_ids})
            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")
        return state

    def prefetch_workflow_ids(self, project) -> List:
        """Helper method implemented for other streams to load all parent
        workflow_ids.

        eg: workflow id's are required to fetch `jobs`
        """
        # This stream is a child stream of the `pipeline` stream hence it requires prefetching of pipelines to fetch all workflows
        workflow_ids = self.client.shared_workflow_ids or {}
        pipeline_wflo_ids = []
        self.project = project
        if workflow_ids and self.project in workflow_ids:
            return workflow_ids[self.project]

        project_pipeline_ids = Pipelines(self.client).prefetch_pipeline_ids(self.project)
        LOGGER.info("Fetching all workflow records for Pipelines")
        pipeline_len = len(project_pipeline_ids)
        LOGGER.info("Total Pipelines %s for project %s", pipeline_len, self.project)
        pipeline_wflo_ids = []
        for index, pipeline_id in enumerate(project_pipeline_ids):
            LOGGER.info("Fetching workflows for pipeline *****%s (%s/%s)", pipeline_id[-4:], index, pipeline_len)
            parent_ids, *_ = self.get_records(pipeline_id, None)
            pipeline_wflo_ids += parent_ids
        pipeline_wflo_ids.sort(key=lambda x: x[1])
        self.client.shared_workflow_ids = {self.project: pipeline_wflo_ids}
        return pipeline_wflo_ids
