from base import CircleCiBaseTest
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER


class CircleCiInterruptedSyncTest(CircleCiBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    def name(self):
        return "tap_tester_circleci_interrupt_test"

    def test_run(self):
        streams_to_exclude = {
            "context",  # Skipping context stream as we do not have permission
            "project",
            "jobs",
            "groups",
            "trigger",  # there is No Data.
            "schedule",  # there is No Data.
            "pipeline_definition",  # there is No Data.
            "deploy"
        }
        expected_streams = self.expected_streams() - streams_to_exclude
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()
        LOGGER.info(
            f"expected_replication_keys = {expected_replication_keys} \n expected_replication_methods = {expected_replication_methods}"
        )

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

        ##########################################################################
        # Update State Between Syncs
        ##########################################################################

        pending_streams = {"workflows", "deploy", "schedule"}
        interrupt_stream = "pipelines"
        interrupted_sync_states = self.create_interrupt_sync_state(
            first_sync_bookmarks, interrupt_stream, pending_streams, first_sync_records
        )
        bookmark_state = interrupted_sync_states["bookmarks"]
        menagerie.set_state(conn_id, interrupted_sync_states)

        LOGGER.info(f"Interrupted Bookmark - {interrupted_sync_states}")

        ##########################################################################
        # Second Sync
        ##########################################################################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()

        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##########################################################################
        # Test By Stream
        ##########################################################################

        for stream in expected_streams:
            LOGGER.info(f"Executing for stream = {stream}")
            with self.subTest(stream=stream):
                # Expected values
                expected_replication_method = expected_replication_methods[stream]

                # Collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # Gather results
                full_records = [message["data"] for message in first_sync_records[stream]["messages"]]
                interrupted_records = [message["data"] for message in second_sync_records[stream]["messages"]]

                first_bookmark_value = first_sync_bookmarks["bookmarks"].setdefault(stream, {})
                second_bookmark_value = second_sync_bookmarks["bookmarks"].setdefault(stream, {})

                LOGGER.info(
                    f"first_bookmark_value = {first_bookmark_value} \n \
                            second_bookmark_value = {second_bookmark_value}"
                )

                if expected_replication_method == self.INCREMENTAL:

                    replication_key = next(iter(expected_replication_keys[stream]))

                    if stream == interrupted_sync_states.get("currently_syncing", None):
                        # For interrupted stream records sync count should be less equals
                        self.assertLessEqual(
                            second_sync_count,
                            first_sync_count,
                            msg=f"For interrupted stream - {stream},\
                                seconds sync record count should be less than or equal to first sync",
                        )

                        # Verify the interrupted sync replicates the expected record set
                        # All interrupted recs are in full recs
                        for record in interrupted_records:
                            self.assertIn(
                                record,
                                full_records,
                                msg="incremental table record in interrupted sync not found in full sync",
                            )

                        if stream in {"pipelines", "workflows"}:
                            repl_key = {
                                "pipelines": "project_slug",
                                "workflows": "pipeline_id",
                            }

                            for record in interrupted_records:
                                rec_time = self.strptime_to_utc(record.get(replication_key))
                                rec_repl_key = str(record[repl_key[stream]])
                                interrupted_stream_state = bookmark_state[stream]
                                if interrupted_stream_state["currently_syncing"] == rec_repl_key:
                                    continue
                                interrupted_bmk = interrupted_stream_state[rec_repl_key]
                                self.assertGreaterEqual(rec_time, self.strptime_to_utc(interrupted_bmk))
                        else:
                            interrupted_bmk = self.strptime_to_utc(bookmark_state[stream][replication_key])
                            for record in interrupted_records:
                                rec_time = self.strptime_to_utc(record.get(replication_key))
                                self.assertGreaterEqual(rec_time, self.strptime_to_utc(interrupted_bmk))

                            # Record count for all streams of interrupted sync match expectations
                            full_records_after_interrupted_bookmark = 0

                            for record in full_records:
                                rec_time = self.strptime_to_utc(record.get(replication_key))
                                if rec_time >= self.strptime_to_utc(interrupted_bmk):
                                    full_records_after_interrupted_bookmark += 1

                            self.assertEqual(
                                full_records_after_interrupted_bookmark,
                                len(interrupted_records),
                                msg=f"Expected {full_records_after_interrupted_bookmark} records in each sync",
                            )

                    elif stream in pending_streams:
                        # First sync and second sync record count match
                        self.assertGreaterEqual(
                            second_sync_count,
                            first_sync_count,
                            msg=f"For pending sync streams - {stream},\
                            second sync record count should be more than or equal to first sync",
                        )
                    else:
                        raise Exception(f"Invalid state of stream {stream} in interrupted state")

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertEqual({}, first_bookmark_value)
                    self.assertEqual({}, second_bookmark_value)

                    # Verify the interrupted sync replicates the expected record set
                    # All interrupted recs are in full recs
                    for record in interrupted_records:
                        self.assertIn(
                            record,
                            full_records,
                            msg="full-table interrupted sync record not found in full sync",
                        )
                    # Verify at least 1 record was replicated for each stream
                    self.assertGreater(second_sync_count, 0)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} \
                                              REPLICATION_METHOD: {}".format(
                            stream, expected_replication_method
                        )
                    )
