import singer
from tap_circle_ci.helpers import load_schemas


def discover() -> singer.catalog.Catalog:
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        stream_key_properties = []

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : [],
            'key_properties': []
        }
        streams.append(catalog_entry)

    return singer.catalog.Catalog.from_dict({'streams': streams})
