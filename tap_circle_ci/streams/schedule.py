from typing import Dict, Iterator, List
from datetime import datetime
from singer import Transformer, get_logger, metrics, write_record
from .abstracts import IncrementalStream

LOGGER = get_logger()


class Schedule(IncrementalStream):
    """Incremental stream for CircleCI Schedules (per project)."""
    stream = "schedule"
    tap_stream_id = "schedule"
    key_properties = ["id"]
    replication_key = "updated-at"
    valid_replication_keys = ["updated-at"]
    parent_stream = "project"
    url_endpoint = "https://circleci.com/api/v2/project/{project_slug}/schedule"

    def get_project_slugs(self) -> List[Dict]:
        """Fetch project slugs from Project stream."""
        if not hasattr(self.client, "shared_project_ids"):
            raise Exception(
                "Project data not available yet. Make sure Project sync runs first."
            )
        return self.client.shared_project_ids.get("project", [])

    def get_url(self, project_slug: str) -> str:
        """Construct the schedule URL for a project."""
        return self.url_endpoint.format(project_slug=project_slug)

    def get_records(self) -> Iterator[Dict]:
        """Fetch schedule records for all projects with pagination support."""
        project_slugs = self.get_project_slugs()
        with metrics.Counter("page_count") as counter:
            for project in project_slugs:
                slug = project["slug"]
                url = self.get_url(slug)
                # ---- First page ----
                params = {}
                response = self.client.get(url, params, {})
                counter.increment()
                items = response.get("items", [])
                next_page_token = response.get("next_page_token")
                for record in items:
                    record["project_id"] = project["id"]
                    record["project_slug"] = slug
                    yield record
                # ---- Subsequent pages ----
                while next_page_token:
                    params = {"page-token": next_page_token}
                    response = self.client.get(url, params, {})
                    counter.increment()
                    items = response.get("items", [])
                    next_page_token = response.get("next_page_token")
                    for record in items:
                        record["project_id"] = project["id"]
                        record["project_slug"] = slug
                        yield record

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        LOGGER.info("Starting Schedule incremental sync")
        current_bookmark = self.get_bookmark(state)
        max_bookmark = current_bookmark
        def parse_datetime(value: str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record_bookmark_val = record.get(self.replication_key)
                if not record_bookmark_val:
                    continue
                if not current_bookmark:
                    is_new_record = True
                else:
                    try:
                        is_new_record = parse_datetime(record_bookmark_val) > parse_datetime(current_bookmark)
                    except Exception as e:
                        LOGGER.warning(
                            f"Failed to compare bookmark: {record_bookmark_val} vs {current_bookmark}. Error: {e}"
                        )
                        is_new_record = True
                if is_new_record:
                    transformed = transformer.transform(record, schema, stream_metadata)
                    write_record(self.tap_stream_id, transformed)
                    counter.increment()
                    if not max_bookmark or parse_datetime(record_bookmark_val) > parse_datetime(max_bookmark):
                        max_bookmark = record_bookmark_val

        if max_bookmark:
            state = self.write_bookmark(state, None, max_bookmark)
            LOGGER.info("Updated schedule bookmark to: %s", max_bookmark)

        return state
