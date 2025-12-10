"""tap-circle-ci pipeline-definition stream module."""
from typing import Dict, Iterator, List
from singer import metrics, write_record, get_logger
from .abstracts import FullTableStream

LOGGER = get_logger()


class PipelineDefinition(FullTableStream):
    """Full-table PipelineDefinition stream, depends on Project stream."""

    stream = "pipeline_definition"
    tap_stream_id = "pipeline_definition"
    key_properties = ["id", "project_id"]
    url_endpoint = "https://circleci.com/api/v2/projects/{project_id}/pipeline-definitions"
    parent_stream = "project"
    project = False

    def get_parent_projects(self) -> List[Dict]:
        """Fetch projects from the Project stream."""
        if not hasattr(self.client, "shared_project_ids"):
            raise Exception(
                "Project data not available yet. The Project stream must sync first."
            )
        return self.client.shared_project_ids.get("project", [])

    def get_url(self, project_id: str) -> str:
        """Construct the URL for pipeline definitions for a given project."""
        return self.url_endpoint.format(project_id=project_id)

    def get_records(self) -> Iterator[Dict]:
        """Loop through all projects and yield pipeline definition records."""
        projects = self.get_parent_projects()

        with metrics.Counter("page_count") as counter:
            for project in projects:
                project_id = project["id"]
                url = self.get_url(project_id)
                response = self.client.get(url, {}, {})
                counter.increment()
                for record in response.get("items", []):
                    record["project_id"] = project_id
                    yield record

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer) -> Dict:
        """Full-table sync for pipeline definitions."""
        LOGGER.info("Starting PipelineDefinition full-table sync")
        records = list(self.get_records())
        with metrics.Timer(self.tap_stream_id, None):
            with metrics.Counter(self.tap_stream_id) as counter:
                for record in records:
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()

        # Store pipeline definition IDs for the Trigger stream
        if not hasattr(self.client, "shared_pipeline_ids"):
            self.client.shared_pipeline_ids = {}
        for record in records:
            project_id = record["project_id"]
            self.client.shared_pipeline_ids.setdefault(project_id, []).append(record["id"])

        return state
