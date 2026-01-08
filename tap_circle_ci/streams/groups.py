"""tap-circle-ci groups stream module."""
from typing import Dict

from singer import (
    Transformer,
    clear_bookmark,
    get_logger,
    metrics,
    write_record,
    write_state,
)

from .abstracts import FullTableStream, CollaborationMixin

LOGGER = get_logger()


class Groups(CollaborationMixin, FullTableStream):
    """Full-table Groups stream (child of Collaborations)."""

    stream = "groups"
    tap_stream_id = "groups"
    key_properties = ["id", "organization_id"]
    url_endpoint = "https://circleci.com/api/v2/organizations/{organization_id}/groups"
    project = None
    requires_project = False
    parent_stream = "collaborations"

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Perform sync for the Groups stream with resumable state tracking."""
        LOGGER.info("Starting Groups full-table sync")
        with metrics.Timer(self.tap_stream_id, None):
            collaborations, start_index = self.get_collaborations(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            collab_len = len(collaborations)

            with metrics.Counter(self.tap_stream_id) as counter:
                for index, org_id in enumerate(collaborations[start_index:], max(start_index, 1)):
                    LOGGER.info("Syncing groups for collaboration *****%s (%s/%s)", str(org_id)[-4:], index, collab_len)
                    for record in self.get_records(org_id):
                        transformed = transformer.transform(record, schema, stream_metadata)
                        write_record(self.tap_stream_id, transformed)
                        counter.increment()
                    state = self.write_bookmark(state, "currently_syncing", org_id)
                    write_state(state)
            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")
        LOGGER.info("Completed Groups full-table sync")
        return state
