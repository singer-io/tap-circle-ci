"""tap-circle-ci pipeline-triggers stream module."""
from typing import Dict, Iterator, List
from singer import metrics, write_record, get_logger
from .abstracts import FullTableStream

LOGGER = get_logger()


class Trigger(FullTableStream):
    """Full-table Trigger stream under PipelineDefinition."""

    stream = "trigger"
    tap_stream_id = "trigger"
    key_properties = ["id"]
    parent_stream = "pipeline_definition"
    requires_project = False

    url_endpoint = (
        "https://circleci.com/api/v2/projects/{project_id}/pipeline-definitions/{pipeline_definition_id}/triggers"
    )

    def get_parent_projects(self) -> List[Dict]:
        """Fetch projects from Project stream."""
        project_map = getattr(self.client, "shared_project_ids", {}) or {}
        projects = project_map.get("project", [])
        if not projects:
            LOGGER.warning("No projects found in shared_project_ids")
        return projects

    def get_pipeline_definitions_for_project(self, project_id: str) -> List[str]:
        """Fetch pipeline definition IDs for a project."""
        pipeline_map = getattr(self.client, "shared_pipeline_definition_ids", {}) or {}
        pipeline_ids = pipeline_map.get(project_id, [])
        if not pipeline_ids:
            LOGGER.info(f"No pipeline definitions found for project {project_id}")
        return pipeline_ids

    def get_records(self) -> Iterator[Dict]:
        """Fetch all trigger records for each project and pipeline definition."""
        with metrics.Counter("page_count") as counter:
            for project in self.get_parent_projects():
                project_id = project.get("id")
                if not project_id:
                    LOGGER.warning(f"Skipping project with missing id: {project}")
                    continue

                pipeline_ids = self.get_pipeline_definitions_for_project(project_id)
                if not pipeline_ids:
                    continue  # skip projects without pipelines

                for pipeline_definition_id in pipeline_ids:
                    url = self.url_endpoint.format(
                        project_id=project_id,
                        pipeline_definition_id=pipeline_definition_id,
                    )

                    response = self.client.get(url, {}, {})
                    counter.increment()

                    items = response.get("items", [])
                    if not items:
                        LOGGER.info(
                            f"No triggers returned for pipeline_definition {pipeline_definition_id} "
                            f"of project {project_id}"
                        )
                        continue

                    for item in items:
                        item["project_id"] = project_id
                        item["pipeline_definition_id"] = pipeline_definition_id
                        item["id"] = item.get("id")  # ensure key_properties is always present
                        yield item

    def sync(
        self,
        state: Dict,
        schema: Dict,
        stream_metadata: Dict,
        transformer,
    ) -> Dict:
        """Full-table sync for triggers."""
        LOGGER.info("Starting Trigger full-table sync")

        with metrics.Timer(self.tap_stream_id, None):
            with metrics.Counter(self.tap_stream_id) as counter:
                for record in self.get_records():
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

        LOGGER.info("Trigger sync completed")
        # Full-table stream: state does not change
        return state
