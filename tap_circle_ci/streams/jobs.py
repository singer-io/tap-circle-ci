"""tap-circle-ci product-reviews stream module."""
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

from .abstracts import FullTableStream
from .workflows import Workflows

LOGGER = get_logger()


class Jobs(FullTableStream):
    """class for jobs stream."""

    stream = "jobs"
    tap_stream_id = "jobs"
    key_properties = ["id","_workflow_id"]
    url_endpoint = "https://circleci.com/api/v2/workflow/WORKFLOW_ID/job"
    project = None

    def get_workflows(self, state: Dict) -> Tuple[List, int]:
        """Returns index for sync resuming on interuption."""
        shared_workflow_ids = Workflows(self.client).prefetch_workflow_ids(self.project)
        last_synced = get_bookmark(state, self.tap_stream_id, "currently_syncing", False)
        last_sync_index = 0
        if last_synced:
            for pos, (workflow_id, _) in enumerate(shared_workflow_ids):
                if workflow_id == last_synced:
                    LOGGER.warning("Last Sync was interrupted after product *****%s", str(workflow_id)[-4:])
                    last_sync_index = pos
                    break
        LOGGER.info("last index for workflow-jobs %s", last_sync_index)
        return shared_workflow_ids, last_sync_index

    def get_records(self, workflow_id: str) -> List:
        # pylint: disable=W0221
        """performs api querying and pagination of response."""
        params = {}
        extraction_url = self.url_endpoint.replace("WORKFLOW_ID", workflow_id)
        while True:
            response = self.client.get(extraction_url, params, {})
            raw_records = response.get("items", [])
            next_page_token = response.get("next_page_token", None)
            if not raw_records:
                break
            if next_page_token is None:
                break
            params["page-token"] = next_page_token

        return raw_records

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Sync implementation for `jobs` stream."""
        # pylint: disable=R0914
        with metrics.Timer(self.tap_stream_id, None):
            pipelines, start_index = self.get_workflows(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            prod_len = len(pipelines)

            with metrics.Counter(self.tap_stream_id) as counter:
                for index, (workflow_id, pipeline_id) in enumerate(pipelines[start_index:], max(start_index, 1)):
                    LOGGER.info("Syncing jobs for workflow *****%s (%s/%s)", workflow_id[-4:], index, prod_len)
                    for rec in self.get_records(workflow_id):
                        rec["_workflow_id"], rec["_pipeline_id"] = workflow_id, pipeline_id
                        write_record(self.tap_stream_id, transformer.transform(rec, schema, stream_metadata))
                        counter.increment()
                    state = self.write_bookmark(state, "currently_syncing", workflow_id)
                    write_state(state)
            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")
        return state
