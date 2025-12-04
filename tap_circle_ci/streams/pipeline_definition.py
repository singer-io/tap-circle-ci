"""tap-circle-ci pipeline-definition stream module."""
from singer import (
    metrics,
    write_record,
    get_logger
)

from .abstracts import FullTableStream

LOGGER = get_logger()


class PipelineDefinition(FullTableStream):
    """Full-table PipelineDefinition stream, depends on Project stream."""

    stream = "pipeline_definition"
    tap_stream_id = "pipeline_definition"
    key_properties = ["id", "project_id"]
    url_endpoint = "https://circleci.com/api/v2/projects/{project_id}/pipeline-definitions"
    parent_stream = "project"

    def get_parent_ids(self, state) -> list:
        project_ids = state.get("project_ids", [])
        if not project_ids and hasattr(self.client, "shared_project_ids"):
            project_ids = self.client.shared_project_ids.get("project", [])

        if not project_ids:
            LOGGER.warning("No parent project IDs found in state for pipeline_definition stream")
        return project_ids

    def get_records_for_project(self, project_id: str) -> list:
        url = self.url_endpoint.format(project_id=project_id)
        response = self.client.get(url, {}, {})
        items = response.get("items", [])
        return items

    def get_records(self, state=None) -> list:
        all_records = []
        parent_ids = self.get_parent_ids(state)
        for project_id in parent_ids:
            records = self.get_records_for_project(project_id)
            for record in records:
                record["project_id"] = project_id
            all_records.extend(records)
        return all_records

    def sync(self, state, schema, stream_metadata, transformer):
        LOGGER.info("Starting PipelineDefinition full-table sync")
        records = self.get_records(state)
        with metrics.Timer(self.tap_stream_id, None):
            with metrics.Counter(self.tap_stream_id) as counter:
                for record in records:
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()
        if state is None:
            state = {}
        pipeline_ids = [r["id"] for r in records if "id" in r]
        state["pipeline_ids"] = pipeline_ids

        return state
