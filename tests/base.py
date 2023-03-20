import copy
import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import dateutil.parser
import pytz
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER


class CircleCiBaseTest(unittest.TestCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """

    PRIMARY_KEYS = "table-key-properties"
    START_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    BOOKMARK_COMPARISON_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
    REPLICATION_KEYS = "valid-replication-keys"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    OBEYS_START_DATE = "obey-start-date"

    def expected_replication_method(self):
        """Return a dictionary with key of table name and value of replication
        method."""
        return {
            table: properties.get(self.REPLICATION_METHOD, None)
            for table, properties in self.expected_metadata().items()
        }

    def setUp(self):
        """Checking required environment variables."""
        if os.getenv("TAP_CIRCLE_CI_ACCESS_TOKEN", None) is None:
            raise Exception("Missing test-required environment variables")

    def get_type(self):
        """The expected url route ending."""
        return "platform.circle-ci"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-circle-ci"

    def expected_metadata(self):
        """The expected streams and metadata about the streams."""
        return {
            "pipelines": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"updated_at"},
                self.OBEYS_START_DATE: True,
            },
            "workflows": {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"created_at"},
                self.OBEYS_START_DATE: True,
            },
            "jobs": {
                self.PRIMARY_KEYS: {"id", "_workflow_id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.OBEYS_START_DATE: False,
            },
        }

    def expected_streams(self):
        """A set of expected stream names."""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        """Return a dictionary with key of table name and value as a set of
        primary key fields."""
        return {
            table: properties.get(self.PRIMARY_KEYS, set()) for table, properties in self.expected_metadata().items()
        }

    def parse_date(self, date_value):
        """Pass in string-formatted-datetime, parse the value, and return it as
        an unformatted datetime object."""
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d",
        }
        for date_format in date_formats:
            try:
                date_stripped = dt.strptime(date_value, date_format)
                LOGGER.info(f"AAAAAA  used format is {date_format}")
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError(f"Tests do not account for dates of this format: {date_value}")

    def expected_replication_keys(self):
        """Return a dictionary with key of table name and value as a set of
        replication key fields."""
        return {
            table: properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }

    def get_credentials(self):
        """Authentication information for the test account."""
        return {"token": os.getenv("TAP_CIRCLE_CI_ACCESS_TOKEN")}

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": "2022-07-01T00:00:00Z",
            "project_slugs": os.getenv("TAP_CIRCLE_CI_PROJECTS"),
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def expected_automatic_fields(self):
        """Retrieving primary keys and replication keys as an automatic
        fields."""
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set())
        return auto_fields

    def run_and_verify_check_mode(self, conn_id):
        """Run the tap in check mode and verify it succeeds.

        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # Run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg=f"unable to locate schemas for connection {conn_id}")

        found_catalog_names = set(map(lambda c: c["stream_name"], found_catalogs))

        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        LOGGER.info("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """Run a sync job and make sure it exited properly.

        Return a dictionary with keys of streams synced and values of
        records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys()
        )
        self.assertGreater(sum(sync_record_count.values()), 0, msg=f"failed to replicate any data: {sync_record_count}")
        LOGGER.info(f"total replicated row count: {sum(sync_record_count.values())}")
        return sync_record_count

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs, select_all_fields=True):
        """Perform table and field selection based off of the streams to select
        set and field selection parameters.

        Verify this results in the expected streams selected and all or
        no fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields)

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get("stream_name") for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat["stream_id"])

            # Verify all testable streams are selected
            selected = catalog_entry.get("annotated-schema").get("selected")
            LOGGER.info("Validating selection on {}: {}".format(cat["stream_name"], selected))
            if cat["stream_name"] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue  # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get("annotated-schema").get("properties").items():
                    field_selected = field_props.get("selected")
                    LOGGER.info("\tValidating selection on {}.{}: {}".format(cat["stream_name"], field, field_selected))
                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat["stream_name"])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry["metadata"])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        """Returning the fields which are marked as automatic and marked as
        selected:true."""
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field["breadcrumb"]) > 1
            inclusion_automatic_or_selected = (
                field["metadata"]["selected"] is True or field["metadata"]["inclusion"] == "automatic"
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field["breadcrumb"][1])
        return selected_fields

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields=True):
        """Select all streams and all fields within streams."""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            non_selected_properties = []
            if not select_all_fields:
                # Get a list of all properties so that none are selected
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {}).keys()

            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema, [], non_selected_properties)

    def timedelta_formatted(self, dtime, dt_format, days=0):
        """Checking the datetime format is as per the expectation Adding the
        lookback window days in the date given as an argument."""
        try:
            date_stripped = dt.strptime(dtime, dt_format)
            return_date = date_stripped + timedelta(days=days)
            return dt.strftime(return_date, dt_format)

        except ValueError:
            return Exception(f"Datetime object is not of the format: {dt_format}")

    def is_incremental(self, stream):
        """Checking if the given stream is incremental or not."""
        return self.expected_metadata().get(stream).get(self.REPLICATION_METHOD) == self.INCREMENTAL

    def create_interrupt_sync_state(self, state, interrupt_stream, pending_streams, sync_records):
        """This function will create a new interrupt sync bookmark state where
        "currently_syncing" will have the non-null value and this state will be
        used to resume the data extraction."""
        expected_replication_keys = self.expected_replication_keys()
        interrupted_sync_states = copy.deepcopy(state)
        bookmark_state = interrupted_sync_states["bookmarks"]
        # Set the interrupt stream as currently syncing
        interrupted_sync_states["currently_syncing"] = interrupt_stream

        # For pending streams, removing the bookmark_value
        for stream in pending_streams:
            bookmark_state.pop(stream, None)

        if self.is_incremental(interrupt_stream):
            # update state for interrupt stream and set the bookmark to a date earlier
            interrupt_stream_bookmark = bookmark_state.get(interrupt_stream, {})
            interrupt_stream_bookmark.pop("offset", None)

            bookmark_keys = {
                "pipelines": "project_slug",
                "workflows": "pipeline_id",
            }

            replication_key = next(iter(expected_replication_keys[interrupt_stream]))
            if interrupt_stream in {"pipelines", "worksflows"}:
                interrupt_stream_rec = {}
                for record in sync_records.get(interrupt_stream).get("messages"):
                    if record.get("action") == "upsert":
                        rec = record.get("data")
                        rec_id = str(rec[bookmark_keys[interrupt_stream]])
                        id_wise_records = interrupt_stream_rec.get(rec_id, [])
                        id_wise_records.append(rec)
                        interrupt_stream_rec[rec_id] = id_wise_records

                last_key = str(list(interrupt_stream_rec)[-1])
                interrupt_stream_rec.popitem()
                interrupt_stream_bookmark = bookmark_state[interrupt_stream]

                interrupt_stream_bookmark.pop(last_key)
                interrupt_stream_bookmark["currently_syncing"] = last_key

                for key, value in interrupt_stream_rec.items():
                    key_index = len(value) // 2 if len(value) > 1 else 0
                    interrupt_stream_bookmark[key] = value[key_index][replication_key]
            else:
                interrupt_stream_rec = []
                for record in sync_records.get(interrupt_stream).get("messages"):
                    if record.get("action") == "upsert":
                        rec = record.get("data")
                        interrupt_stream_rec.append(rec)
                interrupt_stream_index = len(interrupt_stream_rec) // 2 if len(interrupt_stream_rec) > 1 else 0
                interrupt_stream_bookmark[replication_key] = interrupt_stream_rec[interrupt_stream_index][
                    replication_key
                ]
            bookmark_state[interrupt_stream] = interrupt_stream_bookmark
            interrupted_sync_states["bookmarks"] = bookmark_state
        return interrupted_sync_states

    def strptime_to_utc(self, dtimestr):
        """Parse DTIME according to DATETIME_PARSE without TZ safety."""
        d_object = dateutil.parser.parse(dtimestr)
        if d_object.tzinfo is None:
            return d_object.replace(tzinfo=pytz.UTC)
        else:
            return d_object.astimezone(tz=pytz.UTC)

    def assertIsDateFormat(self, value, str_format):
        """Assertion Method that verifies a string value is a formatted
        datetime with the specified format."""
        try:
            dt.strptime(value, str_format)
        except ValueError as err:
            raise AssertionError(f"Value: {value} does not conform to expected format: {str_format}") from err
