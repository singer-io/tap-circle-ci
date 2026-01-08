from typing import Dict, Iterator, List, Tuple
from singer import (
    metrics,
    write_record,
    get_logger,
    clear_bookmark,
    get_bookmark,
    write_state,
)
from .abstracts import FullTableStream, CollaborationMixin

LOGGER = get_logger()


class Project(CollaborationMixin, FullTableStream):
    """Full-table Project stream (depends on Collaborations)."""

    stream = "project"
    tap_stream_id = "project"
    key_properties = ["id", "organization_id"]
    url_endpoint = "https://circleci.com/api/private/project?organization-id={organization_id}"
    parent_stream = "collaborations"
    requires_project = False

    def sync(self, state, schema, stream_metadata, transformer):
        """Full-table sync with resumable state tracking."""
        LOGGER.info("Starting Project full-table sync")
        if not hasattr(self.client, "shared_project_ids"):
            self.client.shared_project_ids = {}
        all_records = []

        with metrics.Timer(self.tap_stream_id, None):
            collaborations, start_index = self.get_collaborations(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            collab_len = len(collaborations)

            with metrics.Counter(self.tap_stream_id) as counter:
                for index, org_id in enumerate(collaborations[start_index:], max(start_index, 1)):
                    LOGGER.info("Syncing projects for collaboration *****%s (%s/%s)", str(org_id)[-4:], index, collab_len)

                    for record in self.get_records(org_id):
                        transformed = transformer.transform(record, schema, stream_metadata)
                        write_record(self.tap_stream_id, transformed)
                        all_records.append({
                            "id": record.get("id"),
                            "slug": record.get("slug"),
                            "organization_id": record.get("organization_id")
                        })
                        counter.increment()

                    state = self.write_bookmark(state, "currently_syncing", org_id)
                    write_state(state)

            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")

        self.client.shared_project_ids[self.tap_stream_id] = all_records
        LOGGER.info("Completed Project full-table sync")
        return state

    def prefetch_project_ids(self) -> List:
        """Helper method to load all project IDs if not already cached.
        Returns list of project IDs for downstream streams.
        """
        if not hasattr(self.client, "shared_project_ids"):
            project_ids = []
            if not hasattr(self.client, "shared_project_ids"):
                self.client.shared_project_ids = {}
            LOGGER.info("Fetching all project records")
            org_ids = self.get_org_ids()
            for org_id in org_ids:
                for record in self.get_records(org_id):
                    try:
                        project_ids.append({
                            "id": record.get("id"),
                            "slug": record.get("slug"),
                            "organization_id": record.get("organization_id")
                        })
                    except KeyError:
                        LOGGER.warning("Unable to find Project ID")
            self.client.shared_project_ids[self.tap_stream_id] = project_ids
        return self.client.shared_project_ids.get(self.tap_stream_id, [])
