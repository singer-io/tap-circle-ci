from datetime import datetime as dt
from datetime import timedelta

import dateutil.parser
from base import CircleCiBaseTest
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER


class CircleCiBookMarkTest(CircleCiBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    def name(self):
        return "tap_tester_circleci_bookmark_test"

    def test_run(self):
        """Verify that for each stream you can do a sync which records
        bookmarks. That the bookmark is the maximum value sent to the target
        for the replication key. That a second sync respects the bookmark All
        data of the second sync is >= the bookmark from the first sync The
        number of records in the 2nd sync is less then the first (This assumes
        that new data added to the stream is done at a rate slow enough that
        you haven't doubled the amount of data from the start date to the first
        sync between the first sync and second sync run in this test)

        Verify that for full table stream, all data replicated in sync 1 is replicated again in sync 2.

        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """

        expected_streams = self.expected_streams() - {"context"}
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        # First Sync
        ##########################################################################
        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        catalog_entries = [catalog for catalog in found_catalogs if catalog.get("tap_stream_id") in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)
        currently_syncing_first = first_sync_bookmarks["currently_syncing"]

        # check that the sync was not interrupted
        self.assertIsNone(
            currently_syncing_first, msg=f"first sync was interrupted at {currently_syncing_first} stream"
        )

        for stream in expected_streams:
            with self.subTest(stream=stream):
                first_count = first_sync_record_count.get(stream, 0)
                if first_count == 0:
                    LOGGER.warning(f"No records for stream {stream} in first sync, skipping bookmark tests")
                    continue

                self.assertGreater(
                    first_count, 0,
                    msg=f"no records replicated for {stream} in first sync"
                )

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)
        new_state = {"bookmarks": simulated_states}
        menagerie.set_state(conn_id, new_state)

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)
        for stream in expected_streams:
            with self.subTest(stream=stream):
                second_count = second_sync_record_count.get(stream, 0)
                if second_count == 0:
                    LOGGER.warning(f"No records for stream {stream} in second sync, skipping bookmark tests")
                    continue

                self.assertGreater(
                    second_count, 0,
                    msg=f"No records replicated for {stream} in second sync"
                )

        ##########################################################################
        # Test By Stream
        ##########################################################################

        bookmark_keys = {"pipelines": "project_slug", "workflows": "pipeline_id"}

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_replication_method = expected_replication_methods[stream]

                # Collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                first_sync_messages = [
                    record.get("data")
                    for record in first_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                second_sync_messages = [
                    record.get("data")
                    for record in second_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                first_bookmark = first_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)
                second_bookmark = second_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)
                simulated_bookmark = new_state.get("bookmarks", {stream: None}).get(stream)
                if expected_replication_method == self.INCREMENTAL:
                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark)

                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark)

                    # Collect information specific to incremental streams from syncs 1 & 2
                    replication_key = next(iter(expected_replication_keys[stream]))

                    self.assertIsInstance(first_bookmark, dict)
                    for bmk_key, bmk_value in first_bookmark.items():
                        self.assertIsInstance(bmk_value, str)
                        self.assertIsDateFormat(bmk_value, self.BOOKMARK_COMPARISON_FORMAT)

                    first_bmk_value = {k: self.strptime_to_utc(v) for k, v in first_bookmark.items()}
                    second_bmk_value = {k: self.strptime_to_utc(v) for k, v in second_bookmark.items()}
                    simulated_bmk_value = {k: self.strptime_to_utc(v) for k, v in simulated_bookmark.items()}

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    # assumes no changes to data during test
                    for key, bookmark in first_bmk_value.items():
                        self.assertGreaterEqual(second_bmk_value.get(key), bookmark)

                    # Verify that you get less than or equal to data getting at 2nd time around
                    self.assertLessEqual(
                        second_sync_count,
                        first_sync_count,
                        msg="second sync didn't have less records, bookmark usage not verified",
                    )

                    bookmark_record_key = bookmark_keys[stream]
                    records_count_gt_manipulated_bmk = 0
                    for record in first_sync_messages:
                        try:
                            replication_key_value = self.strptime_to_utc(record.get(replication_key))
                            first_bookmark_value_utc_value = first_bmk_value[record[bookmark_record_key]]
                            simulated_bookmark = simulated_bmk_value[record[bookmark_record_key]]

                            if replication_key_value >= simulated_bookmark:
                                self.assertIn(record, second_sync_messages)
                                records_count_gt_manipulated_bmk += 1

                            # check that the bookmark value of the first sync is max value of all the records
                            self.assertLessEqual(
                                replication_key_value,
                                first_bookmark_value_utc_value,
                                msg="First sync bookmark was set incorrectly, \
                                                    a record with a greater replication-key value was synced.",
                            )

                        except KeyError:
                            LOGGER.info(
                                "Key not found in the first bookmark value %s %s",
                                record[bookmark_keys[stream]],
                                replication_key_value,
                            )

                    # check if records that have a greater bookmark value than manipulated bookmark are present in second sync records
                    self.assertEqual(
                        records_count_gt_manipulated_bmk,
                        second_sync_count,
                        msg=f"record count mismatch for stream {stream}",
                    )

                    for record in second_sync_messages:
                        try:
                            replication_key_value = self.strptime_to_utc(record.get(replication_key))
                            second_bookmark_value_utc_value = second_bmk_value[record[bookmark_record_key]]
                            simulated_bookmark = simulated_bmk_value[record[bookmark_record_key]]

                            # Verify the second sync bookmark value is the max replication key value for a given stream
                            self.assertLessEqual(
                                replication_key_value,
                                second_bookmark_value_utc_value,
                                msg="Second sync bookmark was set incorrectly, \
                                                    a record with a greater replication-key value was synced.",
                            )

                            # check that all records synced hold a replication key value higher than the simulated bookmark value
                            self.assertGreaterEqual(
                                replication_key_value,
                                simulated_bookmark,
                                msg="Second sync records do not repeat the previous bookmark.",
                            )

                        except KeyError:
                            LOGGER.info(
                                "Key not found in the second bookmark value %s %s %s",
                                replication_key_value,
                                simulated_bookmark,
                                second_bookmark_value_utc_value,
                            )

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not create bookmark for full table streams
                    self.assertEqual(first_bookmark, {})
                    self.assertEqual(second_bookmark, {})

                    # check if all records from first sync exist in second sync
                    for record in first_sync_messages:
                        self.assertIn(record, second_sync_messages)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)
                else:
                    raise NotImplementedError(
                        f"INVALID EXPECTATIONS STREAM: {stream} REPLICATION_METHOD: {expected_replication_method}"
                    )

    def calculated_states_by_stream(self, current_state):
        """Look at the bookmarks from a previous sync and set a new bookmark
        value based off timedelta expectations.

        This ensures the subsequent sync will replicate at least 1
        record but, fewer records than the previous sync. If the test
        data is changed in the future this will break expectations for
        this test.
        """
        stream_timedelta = {stream: {"seconds": 5} for stream in self.expected_streams()}
        diff_state = {stream: "" for stream in current_state["bookmarks"].keys()}
        for stream, state in current_state["bookmarks"].items():
            new_state = {}
            for parent_key, bookmark_value in state.items():
                bmk_converted = dateutil.parser.parse(bookmark_value)
                diff_bmk_value = bmk_converted - timedelta(**stream_timedelta[stream])
                calculated_state_formatted = dt.strftime(diff_bmk_value, self.BOOKMARK_COMPARISON_FORMAT)
                new_state[parent_key] = calculated_state_formatted
            diff_state[stream] = new_state
        return diff_state
