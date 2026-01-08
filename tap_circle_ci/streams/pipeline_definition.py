"""tap-circle-ci pipeline-definition stream module."""
from typing import Dict, Iterator, List, Tuple
from singer import (
    metrics,
    write_record,
    get_logger,
    clear_bookmark,
    get_bookmark,
    write_state,
)
from .abstracts import FullTableStream

LOGGER = get_logger()


class PipelineDefinition(FullTableStream):
    """Full-table PipelineDefinition stream, depends on Project stream."""

    stream = "pipeline_definition"
    tap_stream_id = "pipeline_definition"
    key_properties = ["id", "project_id", "organization_id"]
    url_endpoint = "https://circleci.com/api/v2/projects/{project_id}/pipeline-definitions"
    parent_stream = "project"
    requires_project = False

    def get_parent_projects(self):
        """Fetch project IDs from the Project stream."""
        from .project import Project
        project_stream = Project(self.client)
        projects = project_stream.prefetch_project_ids()
        return projects

    def get_projects(self, state: Dict) -> Tuple[List, int]:
        """Returns projects and index for sync resuming on interruption."""
        projects = self.get_parent_projects()
        last_synced = get_bookmark(state, self.tap_stream_id, "currently_syncing", False)
        last_sync_index = 0
        if last_synced:
            for pos, project in enumerate(projects):
                if project["id"] == last_synced:
                    LOGGER.warning("Last Sync was interrupted after project *****%s", str(project["id"])[-4:])
                    last_sync_index = pos
                    break
        LOGGER.info("last index for pipeline_definition-projects %s", last_sync_index)
        return projects, last_sync_index

    def get_url(self, project_id: str) -> str:
        """Construct the URL for pipeline definitions for a given project."""
        return self.url_endpoint.format(project_id=project_id)

    def get_records(self, project: Dict) -> Iterator[Dict]:
        """Fetch pipeline definition records for a specific project"""
        # Fetch for a specific project
        project_id = project["id"]
        organization_id = project.get("organization_id")
        url = self.get_url(project_id)
        response = self.client.get(url, {}, {})
        for record in response.get("items", []):
            record["project_id"] = project_id
            record["organization_id"] = organization_id
            yield record

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer) -> Dict:
        """Full-table sync for pipeline definitions with resumable state tracking."""
        LOGGER.info("Starting PipelineDefinition full-table sync")

        # Initialize storage for pipeline definition IDs
        if not hasattr(self.client, "shared_pipeline_definition_ids"):
            self.client.shared_pipeline_definition_ids = {}

        with metrics.Timer(self.tap_stream_id, None):
            projects, start_index = self.get_projects(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            project_len = len(projects)

            with metrics.Counter(self.tap_stream_id) as counter:
                for index, project in enumerate(projects[start_index:], max(start_index, 1)):
                    project_id = project["id"]
                    LOGGER.info("Syncing pipeline definitions for project *****%s (%s/%s)", str(project_id)[-4:], index, project_len)

                    for record in self.get_records(project):
                        transformed = transformer.transform(record, schema, stream_metadata)
                        write_record(self.tap_stream_id, transformed)
                        counter.increment()

                        # Store pipeline definition IDs for the Trigger stream
                        self.client.shared_pipeline_definition_ids.setdefault(project_id, []).append(record["id"])

                    state = self.write_bookmark(state, "currently_syncing", project_id)
                    write_state(state)

            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")

        LOGGER.info("Completed PipelineDefinition full-table sync")
        return state

    def prefetch_pipeline_definition_ids(self) -> List:
        """Helper method to load all pipeline definition IDs if not already cached.
        Returns list of pipeline definition IDs for downstream streams.
        """
        if not hasattr(self.client, "shared_pipeline_definition_ids"):
            self.client.shared_pipeline_definition_ids = {}
            LOGGER.info("Fetching all pipeline definition records")
            projects = self.get_parent_projects()
            for project in projects:
                try:
                    for record in self.get_records(project):
                        project_id = record["project_id"]
                        self.client.shared_pipeline_definition_ids.setdefault(project_id, []).append(record["id"])
                except KeyError:
                    LOGGER.warning("Unable to find Project ID")
        return self.client.shared_pipeline_definition_ids or {}
