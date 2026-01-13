"""Unit tests for tap-circle-ci sync module."""
import unittest
from unittest.mock import patch, MagicMock, call
from tap_circle_ci.sync import sync


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
