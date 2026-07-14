"""tap-circle-ci pipeline-triggers stream module."""
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

    def get_project_pipeline_combinations(self, state: Dict) -> Tuple[List[Tuple], int]:
        """Returns project-pipeline combinations and index for sync resuming on interruption."""
        combinations = []
        projects = self.get_parent_projects()

        for project in projects:
            project_id = project.get("id")
            if not project_id:
                continue
            pipeline_ids = self.get_pipeline_definitions_for_project(project_id)
            for pipeline_id in pipeline_ids:
                combinations.append((project, pipeline_id))

        last_synced = get_bookmark(state, self.tap_stream_id, "currently_syncing", False)
        last_sync_index = 0
        if last_synced:
            for pos, (project, pipeline_id) in enumerate(combinations):
                combo_key = f"{project['id']}_{pipeline_id}"
                if combo_key == last_synced:
                    LOGGER.warning("Last Sync was interrupted after project-pipeline *****%s", combo_key[-20:])
                    last_sync_index = pos
                    break
        LOGGER.info("last index for trigger-combinations %s", last_sync_index)
        return combinations, last_sync_index

    def get_parent_projects(self) -> List[Dict]:
        """Fetch projects from Project stream."""
        from .project import Project
        project_stream = Project(self.client)
        projects = project_stream.prefetch_project_ids()
        if not projects:
            LOGGER.warning("No projects found in shared_project_ids")
        unique_projects = {p["id"]: p for p in projects if p.get("id")}
        return list(unique_projects.values())

    def get_pipeline_definitions_for_project(self, project_id: str) -> List[str]:
        """Fetch pipeline definition IDs for a project."""
        from .pipeline_definition import PipelineDefinition
        pipeline_definition_stream = PipelineDefinition(self.client)
        pipeline_map = pipeline_definition_stream.prefetch_pipeline_definition_ids()
        pipeline_ids = pipeline_map.get(project_id, [])
        if not pipeline_ids:
            LOGGER.info("No pipeline definitions found for project %s", project_id)
        return list(dict.fromkeys(pipeline_ids))

    def get_records(self, project: Dict, pipeline_definition_id: str) -> Iterator[Dict]:
        """Fetch trigger records for a specific project-pipeline combo."""
        project_id = project.get("id")
        url = self.url_endpoint.format(
            project_id=project_id,
            pipeline_definition_id=pipeline_definition_id,
        )
        response = self.client.get(url, {}, {})
        items = response.get("items", [])
        if not items:
            LOGGER.info(
                "No triggers returned for pipeline_definition %s of project %s",
                pipeline_definition_id,
                project_id
            )
        for item in items:
            item["project_id"] = project_id
            item["pipeline_definition_id"] = pipeline_definition_id
            item["organization_id"] = project.get("organization_id")
            yield item

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer) -> Dict:
        """Full-table sync for triggers with resumable state tracking."""
        LOGGER.info("Starting Trigger full-table sync")

        with metrics.Timer(self.tap_stream_id, None):
            combinations, start_index = self.get_project_pipeline_combinations(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            combo_len = len(combinations)

            with metrics.Counter(self.tap_stream_id) as counter:
                for index, (project, pipeline_id) in enumerate(combinations[start_index:], max(start_index, 1)):
                    project_id = project["id"]
                    combo_key = f"{project_id}_{pipeline_id}"
                    LOGGER.info(
                        "Syncing triggers for project-pipeline *****%s (%s/%s)",
                        combo_key[-20:], index, combo_len
                    )

                    for record in self.get_records(project, pipeline_id):
                        transformed = transformer.transform(record, schema, stream_metadata)
                        write_record(self.tap_stream_id, transformed)
                        counter.increment()

                    state = self.write_bookmark(state, "currently_syncing", combo_key)
                    write_state(state)

            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")

        LOGGER.info("Trigger sync completed")
        return state
