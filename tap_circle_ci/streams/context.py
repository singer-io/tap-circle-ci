"""tap-circle-ci context stream module."""
from typing import Dict
from singer import (
    Transformer,
    clear_bookmark,
    get_logger,
    metrics,
    write_record,
    write_state
)
from .abstracts import FullTableStream, CollaborationMixin

LOGGER = get_logger()


class Context(CollaborationMixin, FullTableStream):
    """Full-table stream for CircleCI contexts."""

    stream = "context"
    tap_stream_id = "context"
    key_properties = ["id", "organization_id"]
    url_endpoint = "https://circleci.com/api/v2/context?owner-id={organization_id}&owner-type=organization"
    project = None
    parent_stream = "collaborations"
    requires_project = False

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        """Full-table sync with resumable state tracking."""
        with metrics.Timer(self.tap_stream_id, None):
            collaborations, start_index = self.get_collaborations(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            collab_len = len(collaborations)

            with metrics.Counter(self.tap_stream_id) as counter:
                for index, collab_id in enumerate(collaborations[start_index:], max(start_index, 1)):
                    LOGGER.info("Syncing contexts for collaboration *****%s (%s/%s)", str(collab_id)[-4:], index, collab_len)
                    for record in self.get_records(collab_id):
                        transformed_record = transformer.transform(record, schema, stream_metadata)
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()
                    state = self.write_bookmark(state, "currently_syncing", collab_id)
                    write_state(state)
            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")
        return state
