"""tap-circle-ci sync."""
from typing import Dict

import singer

from tap_circle_ci.client import Client
from tap_circle_ci.streams import STREAMS

LOGGER = singer.get_logger()


def sync(config :dict, state: Dict, catalog: singer.Catalog):
    """performs sync for selected streams."""
    client = Client(config)
    projects = list(filter(None, client.config["project_slugs"].split(" ")))
    with singer.Transformer() as transformer:
        for stream in catalog.get_selected_streams(state):
            tap_stream_id = stream.tap_stream_id
            stream_schema = stream.schema.to_dict()
            stream_metadata = singer.metadata.to_map(stream.metadata)
            stream_obj = STREAMS[tap_stream_id](client)
            LOGGER.info("Starting sync for stream: %s", tap_stream_id)
            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)
            singer.write_schema(tap_stream_id, stream_schema, stream_obj.key_properties, stream.replication_key)
            for project in projects:
                stream_obj.project = project
                LOGGER.info("Starting sync for project: %s", project)
                state = stream_obj.sync(
                    state=state, schema=stream_schema, stream_metadata=stream_metadata, transformer=transformer
                )
            singer.write_state(state)

    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)