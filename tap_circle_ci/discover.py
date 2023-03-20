import singer
from tap_circle_ci.helpers import load_schemas
from singer.metadata import get_standard_metadata


def discover() -> singer.catalog.Catalog:
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : get_standard_metadata(schema=schema, key_properties=["id"])
        }
        streams.append(catalog_entry)

    return singer.catalog.Catalog.from_dict({'streams': streams})
