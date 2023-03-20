"""tap-circle-ci discover module."""
import json
from typing import Dict
from pathlib import Path

from singer.catalog import Catalog

from tap_circle_ci.client import Client
from tap_circle_ci.streams import STREAMS


def discover(config: Dict = None):
    """Performs Discovery for tap-circle-ci."""
    if config:
        # permission/auth check
        client = Client(config)
        client.get("https://circleci.com/api/v2/me", {}, {})
    streams = []
    for stream_name, stream in STREAMS.items():
        schema_path = Path(__file__).parent.resolve() / f"schemas/{stream_name}.json"
        with open(schema_path, encoding="utf-8") as schema_file:
            schema = json.load(schema_file)
        streams.append(
            {
                "stream": stream_name,
                "tap_stream_id": stream.tap_stream_id,
                "schema": schema,
                "metadata": stream.get_metadata(schema),
            }
        )
    return Catalog.from_dict({"streams": streams})
