from typing import Dict, Iterator, List, Tuple
from datetime import datetime
from singer import (
    Transformer,
    get_logger,
    metrics,
    write_record,
    clear_bookmark,
    get_bookmark,
    write_state,
)
from singer.utils import strptime_to_utc
from .abstracts import IncrementalStream

LOGGER = get_logger()


class Schedule(IncrementalStream):
    """Incremental stream for CircleCI Schedules (per project)."""
    stream = "schedule"
    tap_stream_id = "schedule"
    key_properties = ["id", "project-slug"]
    replication_key = "updated-at"
    valid_replication_keys = ["updated-at"]
    config_start_key = "start_date"
    parent_stream = "project"
    requires_project = False
    url_endpoint = "https://circleci.com/api/v2/project/{project_slug}/schedule"

    def get_project_slugs(self) -> List[Dict]:
        """Fetch project slugs from Project stream, filtered by config."""
        from .project import Project
        project_stream = Project(self.client)
        all_projects = project_stream.prefetch_project_ids()

        # Filter to only configured project slugs
        configured_slugs = set(self.client.config.get("project_slugs", "").split())
        filtered_projects = [p for p in all_projects if p.get("slug") in configured_slugs]

        if not filtered_projects:
            LOGGER.warning("No configured projects found in synced projects")

        return filtered_projects

    def get_projects(self, state: Dict) -> Tuple[List, int]:
        """Returns projects and index for sync resuming on interruption."""
        projects = self.get_project_slugs()
        last_synced = get_bookmark(state, self.tap_stream_id, "currently_syncing", False)
        last_sync_index = 0
        if last_synced:
            for pos, project in enumerate(projects):
                if project["id"] == last_synced:
                    LOGGER.warning("Last Sync was interrupted after project *****%s", str(project["id"])[-4:])
                    last_sync_index = pos
                    break
        LOGGER.info("last index for schedule-projects %s", last_sync_index)
        return projects, last_sync_index

    def get_url(self, project_slug: str) -> str:
        """Construct the schedule URL for a project."""
        return self.url_endpoint.format(project_slug=project_slug)

    def get_records(self, project: Dict) -> Iterator[Dict]:
        """Fetch schedule records for a specific project or all projects with pagination support."""
        slug = project["slug"]
        url = self.get_url(slug)
        for page in self.iter_pages(url):
            items = page.get("items", [])
            for record in items:
                record["project_id"] = project["id"]
                record["project_slug"] = slug
                yield record

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        LOGGER.info("Starting Schedule incremental sync")
        current_bookmark = self.get_bookmark(state)
        max_bookmark = bookmark_date_utc = strptime_to_utc(current_bookmark)

        with metrics.Timer(self.tap_stream_id, None):
            projects, start_index = self.get_projects(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            project_len = len(projects)

            with metrics.Counter(self.tap_stream_id) as counter:
                for index, project in enumerate(projects[start_index:], max(start_index, 1)):
                    project_id = project["id"]
                    LOGGER.info("Syncing schedules for project *****%s (%s/%s)", str(project_id)[-4:], index, project_len)

                    for record in self.get_records(project):
                        record_bookmark_val = record.get(self.replication_key)
                        if not record_bookmark_val:
                            raise Exception(
                                f"Record missing replication key '{self.replication_key}': {record}"
                            )

                        try:
                            record_timestamp = strptime_to_utc(record_bookmark_val)
                        except Exception as e:
                            LOGGER.warning(
                                f"Failed to parse timestamp: {record_bookmark_val}. Error: {e}"
                            )
                            continue

                        if record_timestamp >= bookmark_date_utc:
                            transformed = transformer.transform(record, schema, stream_metadata)
                            write_record(self.tap_stream_id, transformed)
                            counter.increment()
                            max_bookmark = max(max_bookmark, record_timestamp)

                    state = self.write_bookmark(state, "currently_syncing", project_id)
                    write_state(state)

            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")

        if max_bookmark:
            from singer.utils import strftime
            max_bookmark_str = strftime(max_bookmark)
            state = self.write_bookmark(state, None, max_bookmark_str)
            LOGGER.info("Updated schedule bookmark to: %s", max_bookmark_str)

        return state
