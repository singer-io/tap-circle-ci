"""Unit tests for tap-circle-ci sync module."""
import unittest
from unittest.mock import patch, MagicMock, call
from tap_circle_ci.sync import sync
from tap_circle_ci.exceptions import Server5xxError, Http500RequestError, Http503RequestError


class TestSync(unittest.TestCase):
    """Test sync module functions."""

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_sync_collaborations_stream_first(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """Test that collaborations stream is synced first due to priority."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1"}
        mock_client_class.return_value = mock_client

        mock_catalog = MagicMock()
        collab_stream = MagicMock()
        collab_stream.tap_stream_id = "collaborations"
        collab_stream.schema.to_dict.return_value = {"type": "object"}
        collab_stream.metadata = []
        collab_stream.replication_key = None

        project_stream = MagicMock()
        project_stream.tap_stream_id = "project"
        project_stream.schema.to_dict.return_value = {"type": "object"}
        project_stream.metadata = []
        project_stream.replication_key = None

        mock_catalog.get_selected_streams.return_value = [project_stream, collab_stream]

        mock_collab_obj = MagicMock()
        mock_collab_obj.key_properties = ["id"]
        mock_collab_obj.sync.return_value = {}

        mock_project_obj = MagicMock()
        mock_project_obj.key_properties = ["id"]
        mock_project_obj.sync.return_value = {}
        mock_project_obj.requires_project = False

        mock_streams.__getitem__.side_effect = lambda x: {
            "collaborations": MagicMock(return_value=mock_collab_obj),
            "project": MagicMock(return_value=mock_project_obj)
        }[x]

        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        sync({"token": "test", "project_slugs": "gh/org/repo1"}, {}, mock_catalog)

        self.assertEqual(mock_collab_obj.sync.call_count, 1)
        self.assertEqual(mock_project_obj.sync.call_count, 1)

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_sync_requires_project_true(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """Test that streams with requires_project=True are synced per project."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1 gh/org/repo2"}
        mock_client_class.return_value = mock_client

        mock_catalog = MagicMock()
        stream = MagicMock()
        stream.tap_stream_id = "pipelines"
        stream.schema.to_dict.return_value = {"type": "object"}
        stream.metadata = []
        stream.replication_key = "created_at"

        mock_catalog.get_selected_streams.return_value = [stream]

        mock_stream_obj = MagicMock()
        mock_stream_obj.key_properties = ["id"]
        mock_stream_obj.requires_project = True
        mock_stream_obj.sync.return_value = {}

        mock_streams.__getitem__.return_value = MagicMock(return_value=mock_stream_obj)
        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        # Execute
        sync({"token": "test", "project_slugs": "gh/org/repo1 gh/org/repo2"}, {}, mock_catalog)

        # Verify sync called twice (once per project)
        self.assertEqual(mock_stream_obj.sync.call_count, 2)

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_sync_requires_project_false(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """Test that streams with requires_project=False are synced once."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1 gh/org/repo2"}
        mock_client_class.return_value = mock_client

        mock_catalog = MagicMock()
        stream = MagicMock()
        stream.tap_stream_id = "collaborations"
        stream.schema.to_dict.return_value = {"type": "object"}
        stream.metadata = []
        stream.replication_key = None

        mock_catalog.get_selected_streams.return_value = [stream]
        mock_stream_obj = MagicMock()
        mock_stream_obj.key_properties = ["id"]
        mock_stream_obj.requires_project = False
        mock_stream_obj.sync.return_value = {}

        mock_streams.__getitem__.return_value = MagicMock(return_value=mock_stream_obj)
        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        sync({"token": "test", "project_slugs": "gh/org/repo1 gh/org/repo2"}, {}, mock_catalog)

        self.assertEqual(mock_stream_obj.sync.call_count, 1)

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_sync_writes_schema_for_each_stream(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """Test that write_schema is called for each selected stream."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1"}
        mock_client_class.return_value = mock_client

        mock_catalog = MagicMock()
        stream1 = MagicMock()
        stream1.tap_stream_id = "collaborations"
        stream1.schema.to_dict.return_value = {"type": "object"}
        stream1.metadata = []
        stream1.replication_key = None

        stream2 = MagicMock()
        stream2.tap_stream_id = "context"
        stream2.schema.to_dict.return_value = {"type": "object"}
        stream2.metadata = []
        stream2.replication_key = None

        mock_catalog.get_selected_streams.return_value = [stream1, stream2]

        mock_obj1 = MagicMock()
        mock_obj1.key_properties = ["id"]
        mock_obj1.requires_project = False
        mock_obj1.sync.return_value = {}

        mock_obj2 = MagicMock()
        mock_obj2.key_properties = ["id"]
        mock_obj2.requires_project = False
        mock_obj2.sync.return_value = {}

        mock_streams.__getitem__.side_effect = lambda x: {
            "collaborations": MagicMock(return_value=mock_obj1),
            "context": MagicMock(return_value=mock_obj2)
        }[x]

        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        sync({"token": "test", "project_slugs": "gh/org/repo1"}, {}, mock_catalog)

        self.assertEqual(mock_write_schema.call_count, 2)


class TestSync5xxHandling(unittest.TestCase):
    """Test sync module graceful handling of 5xx server errors."""

    def _make_stream_mock(self, tap_stream_id, replication_key=None):
        """Helper to create a mock catalog stream entry."""
        stream = MagicMock()
        stream.tap_stream_id = tap_stream_id
        stream.schema.to_dict.return_value = {"type": "object"}
        stream.metadata = []
        stream.replication_key = replication_key
        return stream

    def _make_stream_obj(self, requires_project=False, sync_return=None, sync_side_effect=None):
        """Helper to create a mock stream object."""
        obj = MagicMock()
        obj.key_properties = ["id"]
        obj.requires_project = requires_project
        if sync_side_effect:
            obj.sync.side_effect = sync_side_effect
        else:
            obj.sync.return_value = sync_return or {}
        return obj

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_5xx_on_stream_continues_to_next(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """When a stream raises Server5xxError, sync should continue to next stream."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1"}
        mock_client_class.return_value = mock_client

        # First stream fails with 5xx, second should still run
        stream1 = self._make_stream_mock("collaborations")
        stream2 = self._make_stream_mock("project")
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [stream1, stream2]

        failing_obj = self._make_stream_obj(
            requires_project=False,
            sync_side_effect=Http500RequestError(endpoint="https://circleci.com/api/v2/me/collaborations")
        )
        succeeding_obj = self._make_stream_obj(requires_project=False, sync_return={"bookmarks": {}})

        mock_streams.__getitem__.side_effect = lambda x: {
            "collaborations": MagicMock(return_value=failing_obj),
            "project": MagicMock(return_value=succeeding_obj)
        }[x]
        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        # Should NOT raise — graceful handling
        sync({"token": "test", "project_slugs": "gh/org/repo1"}, {}, mock_catalog)

        # failing stream sync was called (and failed)
        self.assertEqual(failing_obj.sync.call_count, 1)
        # succeeding stream still ran
        self.assertEqual(succeeding_obj.sync.call_count, 1)

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_5xx_saves_state_checkpoint(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """When a stream fails with 5xx, the state should be checkpointed."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1"}
        mock_client_class.return_value = mock_client

        stream1 = self._make_stream_mock("pipelines", replication_key="updated_at")
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [stream1]

        failing_obj = self._make_stream_obj(
            requires_project=True,
            sync_side_effect=Http503RequestError(endpoint="https://circleci.com/api/v2/pipeline")
        )
        mock_streams.__getitem__.return_value = MagicMock(return_value=failing_obj)
        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        initial_state = {"bookmarks": {"pipelines": {"updated_at": "2024-01-01T00:00:00Z"}}}
        sync({"token": "test", "project_slugs": "gh/org/repo1"}, initial_state, mock_catalog)

        # write_state should have been called (checkpoint on failure + final)
        self.assertTrue(mock_write_state.called)

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_5xx_on_one_project_skips_remaining_projects(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """When a per-project stream fails with 5xx mid-project, remaining projects are skipped."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1 gh/org/repo2 gh/org/repo3"}
        mock_client_class.return_value = mock_client

        stream1 = self._make_stream_mock("pipelines", replication_key="updated_at")
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [stream1]

        # Succeed on first project, fail on second
        call_count = {"n": 0}
        def sync_side_effect(**kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise Http500RequestError(endpoint="https://circleci.com/api/v2/pipeline")
            return kwargs.get("state", {})

        stream_obj = self._make_stream_obj(requires_project=True)
        stream_obj.sync.side_effect = sync_side_effect

        mock_streams.__getitem__.return_value = MagicMock(return_value=stream_obj)
        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        # Should NOT crash
        sync({"token": "test", "project_slugs": "gh/org/repo1 gh/org/repo2 gh/org/repo3"}, {}, mock_catalog)

        # sync was called twice: project1 succeeded, project2 failed, project3 skipped
        self.assertEqual(stream_obj.sync.call_count, 2)

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_all_streams_5xx_still_completes(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """If all streams fail with 5xx, sync should still complete without crashing."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1"}
        mock_client_class.return_value = mock_client

        stream1 = self._make_stream_mock("collaborations")
        stream2 = self._make_stream_mock("context")
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [stream1, stream2]

        failing_obj1 = self._make_stream_obj(
            requires_project=False,
            sync_side_effect=Http500RequestError()
        )
        failing_obj2 = self._make_stream_obj(
            requires_project=False,
            sync_side_effect=Http503RequestError()
        )

        mock_streams.__getitem__.side_effect = lambda x: {
            "collaborations": MagicMock(return_value=failing_obj1),
            "context": MagicMock(return_value=failing_obj2)
        }[x]
        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        # Should complete without raising
        sync({"token": "test", "project_slugs": "gh/org/repo1"}, {}, mock_catalog)

        self.assertEqual(failing_obj1.sync.call_count, 1)
        self.assertEqual(failing_obj2.sync.call_count, 1)
        # Final write_state should still happen (currently_syncing = None)
        self.assertTrue(mock_write_state.called)

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_generic_5xx_caught_in_sync(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """Generic Server5xxError (e.g., 505) should also be caught gracefully."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1"}
        mock_client_class.return_value = mock_client

        stream1 = self._make_stream_mock("collaborations")
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [stream1]

        failing_obj = self._make_stream_obj(
            requires_project=False,
            sync_side_effect=Server5xxError(status_code=505, endpoint="https://circleci.com/api/v2/test")
        )
        mock_streams.__getitem__.return_value = MagicMock(return_value=failing_obj)
        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        # Should NOT crash
        sync({"token": "test", "project_slugs": "gh/org/repo1"}, {}, mock_catalog)
        self.assertEqual(failing_obj.sync.call_count, 1)

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_non_5xx_errors_still_propagate(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """Non-5xx exceptions (e.g., ValueError) should still propagate and crash."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1"}
        mock_client_class.return_value = mock_client

        stream1 = self._make_stream_mock("collaborations")
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [stream1]

        failing_obj = self._make_stream_obj(
            requires_project=False,
            sync_side_effect=ValueError("unexpected data format")
        )
        mock_streams.__getitem__.return_value = MagicMock(return_value=failing_obj)
        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        with self.assertRaises(ValueError):
            sync({"token": "test", "project_slugs": "gh/org/repo1"}, {}, mock_catalog)

    @patch("tap_circle_ci.sync.Client")
    @patch("tap_circle_ci.sync.STREAMS")
    @patch("singer.Transformer")
    @patch("singer.write_schema")
    @patch("singer.write_state")
    @patch("singer.set_currently_syncing")
    @patch("singer.metadata.to_map")
    def test_currently_syncing_cleared_after_5xx(
        self, mock_to_map, mock_set_syncing, mock_write_state,
        mock_write_schema, mock_transformer, mock_streams, mock_client_class
    ):
        """After all streams (including failed ones), currently_syncing should be cleared."""
        mock_client = MagicMock()
        mock_client.config = {"project_slugs": "gh/org/repo1"}
        mock_client_class.return_value = mock_client

        stream1 = self._make_stream_mock("collaborations")
        mock_catalog = MagicMock()
        mock_catalog.get_selected_streams.return_value = [stream1]

        failing_obj = self._make_stream_obj(
            requires_project=False,
            sync_side_effect=Http500RequestError()
        )
        mock_streams.__getitem__.return_value = MagicMock(return_value=failing_obj)
        mock_to_map.return_value = {}
        mock_set_syncing.return_value = {}

        sync({"token": "test", "project_slugs": "gh/org/repo1"}, {}, mock_catalog)

        # The last set_currently_syncing call should clear it (None)
        last_call = mock_set_syncing.call_args_list[-1]
        self.assertIsNone(last_call[0][1])
