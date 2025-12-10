"""tap-circle-ci sync."""
from typing import Dict
import singer

from tap_circle_ci.client import Client
from tap_circle_ci.streams import STREAMS

LOGGER = singer.get_logger()


def sync(config: dict, state: Dict, catalog: singer.Catalog):
    """Performs sync for selected streams with proper dependency ordering."""
    client = Client(config)
    projects = list(filter(None, client.config["project_slugs"].split(" ")))

    # Stream execution priority
    PRIORITY = {
        "collaborations": 0,         # must run first
        "project": 1,
        "pipeline_definition": 2
    }

    with singer.Transformer() as transformer:
        selected_streams = catalog.get_selected_streams(state)
        ordered_streams = sorted(selected_streams,key=lambda s: PRIORITY.get(s.tap_stream_id, float("inf")))

        for stream in ordered_streams:
            tap_stream_id = stream.tap_stream_id
            stream_schema = stream.schema.to_dict()
            stream_metadata = singer.metadata.to_map(stream.metadata)
            stream_obj = STREAMS[tap_stream_id](client)

            LOGGER.info("Starting sync for stream: %s", tap_stream_id)
            state = singer.set_currently_syncing(state, tap_stream_id)
            singer.write_state(state)

            singer.write_schema(
                tap_stream_id,
                stream_schema,
                stream_obj.key_properties,
                stream.replication_key,
            )
            requires_project = getattr(stream_obj, "project", True)
            if requires_project:
                for project in projects:
                    stream_obj.project = project
                    LOGGER.info("Starting sync for project: %s", project)
                    state = stream_obj.sync(
                        state=state,
                        schema=stream_schema,
                        stream_metadata=stream_metadata,
                        transformer=transformer,
                    )
            else:
                LOGGER.info("Running stream '%s' ", tap_stream_id)
                state = stream_obj.sync(
                    state=state,
                    schema=stream_schema,
                    stream_metadata=stream_metadata,
                    transformer=transformer,
                )
            singer.write_state(state)
    state = singer.set_currently_syncing(state, None)
    singer.write_state(state)
