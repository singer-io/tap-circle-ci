from typing import Dict
from datetime import datetime
from singer import (
    Transformer,
    clear_bookmark,
    get_logger,
    metrics,
    write_record,
    write_state,
)
from .abstracts import IncrementalStream, CollaborationMixin

LOGGER = get_logger()


class Deploy(CollaborationMixin, IncrementalStream):
    """Incremental stream for CircleCI Deploy Environments."""

    stream = "deploy"
    tap_stream_id = "deploy"
    key_properties = ["id"]
    replication_key = "updated_at"
    valid_replication_keys = ["updated_at"]
    url_endpoint = "https://circleci.com/api/v2/deploy/environments?org-id={organization_id}&page-size={page_size}"
    requires_project = False
    parent_stream = "collaborations"

    def get_url(self, organization_id: str) -> str:
        page_size = self.client.config.get("page_size", 200)
        return self.url_endpoint.format(organization_id=organization_id, page_size=page_size)

    def sync(self, state: Dict, schema: Dict, stream_metadata: Dict, transformer: Transformer) -> Dict:
        LOGGER.info("Starting Deploy incremental sync")

        current_bookmark = self.get_bookmark(state)
        max_bookmark = current_bookmark

        def parse_datetime(value: str):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))

        with metrics.Timer(self.tap_stream_id, None):
            collaborations, start_index = self.get_collaborations(state)
            LOGGER.info("STARTING SYNC FROM INDEX %s", start_index)
            collab_len = len(collaborations)

            with metrics.Counter(self.tap_stream_id) as counter:
                for index, org_id in enumerate(collaborations[start_index:], max(start_index, 1)):
                    LOGGER.info("Syncing deploy for collaboration *****%s (%s/%s)", str(org_id)[-4:], index, collab_len)
                    for record in self.get_records(org_id):
                        record_bookmark_val = record.get(self.replication_key)
                        if not record_bookmark_val:
                            raise Exception(
                                f"Record missing replication key '{self.replication_key}': {record}"
                            )
                        if not current_bookmark:
                            is_new_record = True
                        else:
                            try:
                                is_new_record = parse_datetime(record_bookmark_val) > parse_datetime(current_bookmark)
                            except Exception as err:
                                LOGGER.warning(
                                    "Failed to compare bookmark: %s vs %s. Error: %s",
                                    record_bookmark_val,
                                    current_bookmark,
                                    err
                                )
                                is_new_record = True
                        if is_new_record:
                            transformed = transformer.transform(record, schema, stream_metadata)
                            write_record(self.tap_stream_id, transformed)
                            counter.increment()
                            if not max_bookmark or parse_datetime(record_bookmark_val) > parse_datetime(max_bookmark):
                                max_bookmark = record_bookmark_val

                    state = self.write_bookmark(state, "currently_syncing", org_id)
                    write_state(state)

            state = clear_bookmark(state, self.tap_stream_id, "currently_syncing")

        if max_bookmark:
            state = self.write_bookmark(state, None, max_bookmark)
            LOGGER.info("Updated deploy bookmark to: %s", max_bookmark)

        return state
