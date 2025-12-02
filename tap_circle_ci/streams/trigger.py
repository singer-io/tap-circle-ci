"""tap-circle-ci pipeline-triggers stream module."""
from singer import (
    metrics,
    write_record,
    get_logger
)

from .abstracts import FullTableStream

LOGGER = get_logger()


class Trigger(FullTableStream):
    """Triggers stream under pipeline_definition."""

    stream = "trigger"
    tap_stream_id = "trigger"

    key_properties = ["id"]
    parent_stream = "pipeline_definition"

    url_endpoint = (
        "https://circleci.com/api/v2/projects/{project_id}/pipeline-definitions/{pipeline_definition_id}/triggers"
    )

    def get_records(self, state=None) -> list:
        """Fetch triggers for each pipeline definition."""
        if state is None:
            state = {}

        project_ids = state.get("project_ids", [])
        pipeline_definition_ids = state.get("pipeline_ids", [])

        all_records = []

        for project_id in project_ids:
            for pipeline_definition_id in pipeline_definition_ids:

                url = self.url_endpoint.format(
                    project_id=project_id,
                    pipeline_definition_id=pipeline_definition_id
                )

                response = self.client.get(url, {}, {})

                items = response.get("items", [])

                for item in items:
                    item["_project_id"] = project_id
                    item["_pipeline_definition_id"] = pipeline_definition_id

                all_records.extend(items)

        return all_records

    def sync(self, state, schema, stream_metadata, transformer):
        LOGGER.info("Starting triggers sync...")

        records = self.get_records(state)

        with metrics.Timer(self.tap_stream_id, None):
            with metrics.Counter(self.tap_stream_id) as counter:
                for record in records:
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

        # Update state
        trigger_ids = [r["id"] for r in records if "id" in r]
        state["trigger_ids"] = trigger_ids

        return state
