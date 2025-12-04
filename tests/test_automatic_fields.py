"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import CircleCiBaseTest
from tap_tester import connections, runner


class CircleCiAutomaticFields(CircleCiBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_circleci_automatic_fields"

    def test_run(self):
        """
        - Verify we can deselect all fields except when inclusion=automatic,
        which is handled by base.py methods.
        - Verify that only the automatic fields are sent to the target.
        - Verify that all replicated records have unique primary key
        values.
        """

        expected_streams = self.expected_streams() - {"context"}
        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_automatic_fields = [
            catalog for catalog in found_catalogs if catalog.get("stream_name") in expected_streams
        ]

        self.perform_and_verify_table_and_field_selection(
            conn_id,
            test_catalogs_automatic_fields,
            select_all_fields=False,
        )

        # Run initial sync
        record_count = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # Expected values
                expected_keys = self.expected_automatic_fields().get(stream)
                expected_primary_keys = self.expected_primary_keys()[stream]

                # Skip streams with no data
                if record_count.get(stream, 0) == 0:
                    continue
                # check if at least 1 record is synced
                self.assertGreater(record_count.get(stream, 0), 0)

                # Collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row.get("data").keys()) for row in data.get("messages", {})]
                primary_keys_list = [
                    tuple(message.get("data", {}).get(expected_pk) for expected_pk in expected_primary_keys)
                    for message in data.get("messages", [])
                    if message.get("action") == "upsert"
                ]
                unique_primary_keys_list = set(primary_keys_list)

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

                # Verify that all replicated records have unique primary key values.
                self.assertEqual(
                    len(primary_keys_list),
                    len(unique_primary_keys_list),
                    msg="Replicated record does not have unique primary key values.",
                )
