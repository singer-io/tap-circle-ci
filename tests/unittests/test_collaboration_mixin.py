"""Unit tests for CollaborationMixin class."""
import unittest
from unittest.mock import patch, MagicMock
from tap_circle_ci.streams.abstracts import CollaborationMixin, FullTableStream


class ConcreteCollaborationStream(CollaborationMixin, FullTableStream):
    """Concrete implementation for testing CollaborationMixin."""

    stream = "test_stream"
    tap_stream_id = "test_stream"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    valid_replication_keys = None
    replication_key = None
    url_endpoint = "https://circleci.com/api/v2/test"

    def get_url(self, org_id):
        return self.url_endpoint


class TestCollaborationMixin(unittest.TestCase):
    """Test CollaborationMixin methods."""

    def setUp(self):
        """Setup test fixtures."""
        mock_client = MagicMock()
        self.stream = ConcreteCollaborationStream(client=mock_client)

    @patch("tap_circle_ci.streams.collaborations.Collaborations")
    def test_get_org_ids_from_cache(self, mock_collab_class):
        """Test get_org_ids retrieves IDs via Collaborations stream."""
        mock_collab_instance = MagicMock()
        mock_collab_instance.prefetch_collaborations_ids.return_value = ["org-1", "org-2", "org-3"]
        mock_collab_class.return_value = mock_collab_instance

        result = self.stream.get_org_ids()

        self.assertEqual(result, ["org-1", "org-2", "org-3"])

    @patch("tap_circle_ci.streams.collaborations.Collaborations")
    def test_get_org_ids_empty_cache(self, mock_collab_class):
        """Test get_org_ids returns empty list when no collaborations."""
        mock_collab_instance = MagicMock()
        mock_collab_instance.prefetch_collaborations_ids.return_value = []
        mock_collab_class.return_value = mock_collab_instance

        result = self.stream.get_org_ids()

        self.assertEqual(result, [])

    @patch("tap_circle_ci.streams.collaborations.Collaborations")
    def test_get_org_ids_calls_prefetch(self, mock_collab_class):
        """Test get_org_ids calls Collaborations.prefetch_collaborations_ids."""
        mock_collab_instance = MagicMock()
        mock_collab_instance.prefetch_collaborations_ids.return_value = ["org-1", "org-2"]
        mock_collab_class.return_value = mock_collab_instance

        result = self.stream.get_org_ids()

        mock_collab_class.assert_called_once_with(self.stream.client)
        mock_collab_instance.prefetch_collaborations_ids.assert_called_once()
        self.assertEqual(result, ["org-1", "org-2"])

    @patch("tap_circle_ci.streams.collaborations.Collaborations")
    @patch("tap_circle_ci.streams.abstracts.get_bookmark", return_value=None)
    def test_get_collaborations_no_bookmark(self, mock_get_bookmark, mock_collab_class):
        """Test get_collaborations with no previous bookmark returns from index 0."""
        mock_collab_instance = MagicMock()
        mock_collab_instance.prefetch_collaborations_ids.return_value = ["org-1", "org-2", "org-3"]
        mock_collab_class.return_value = mock_collab_instance

        state = {}

        collabs, start_index = self.stream.get_collaborations(state)

        self.assertEqual(collabs, ["org-1", "org-2", "org-3"])
        self.assertEqual(start_index, 0)
        mock_get_bookmark.assert_called_once_with(state, "test_stream", "currently_syncing", False)

    @patch("tap_circle_ci.streams.collaborations.Collaborations")
    @patch("tap_circle_ci.streams.abstracts.get_bookmark", return_value="org-2")
    def test_get_collaborations_with_bookmark(self, mock_get_bookmark, mock_collab_class):
        """Test get_collaborations resumes from bookmarked collaboration."""
        mock_collab_instance = MagicMock()
        mock_collab_instance.prefetch_collaborations_ids.return_value = ["org-1", "org-2", "org-3"]
        mock_collab_class.return_value = mock_collab_instance

        state = {"bookmarks": {"test_stream": {"currently_syncing": "org-2"}}}

        collabs, start_index = self.stream.get_collaborations(state)

        self.assertEqual(collabs, ["org-1", "org-2", "org-3"])
        self.assertEqual(start_index, 1)

    @patch("tap_circle_ci.streams.collaborations.Collaborations")
    @patch("tap_circle_ci.streams.abstracts.get_bookmark", return_value="org-3")
    def test_get_collaborations_last_bookmark(self, mock_get_bookmark, mock_collab_class):
        """Test get_collaborations when bookmark is the last collaboration."""
        mock_collab_instance = MagicMock()
        mock_collab_instance.prefetch_collaborations_ids.return_value = ["org-1", "org-2", "org-3"]
        mock_collab_class.return_value = mock_collab_instance

        state = {"bookmarks": {"test_stream": {"currently_syncing": "org-3"}}}

        collabs, start_index = self.stream.get_collaborations(state)

        self.assertEqual(collabs, ["org-1", "org-2", "org-3"])
        self.assertEqual(start_index, 2)

    @patch("tap_circle_ci.streams.collaborations.Collaborations")
    @patch("tap_circle_ci.streams.abstracts.get_bookmark", return_value="org-99")
    def test_get_collaborations_bookmark_not_found(self, mock_get_bookmark, mock_collab_class):
        """Test get_collaborations when bookmarked collaboration not in list."""
        mock_collab_instance = MagicMock()
        mock_collab_instance.prefetch_collaborations_ids.return_value = ["org-1", "org-2", "org-3"]
        mock_collab_class.return_value = mock_collab_instance

        state = {"bookmarks": {"test_stream": {"currently_syncing": "org-99"}}}

        collabs, start_index = self.stream.get_collaborations(state)

        # Should start from beginning when bookmark not found
        self.assertEqual(collabs, ["org-1", "org-2", "org-3"])
        self.assertEqual(start_index, 0)

    @patch("tap_circle_ci.streams.collaborations.Collaborations")
    @patch("tap_circle_ci.streams.abstracts.get_bookmark", return_value="org-1")
    def test_get_collaborations_first_bookmark(self, mock_get_bookmark, mock_collab_class):
        """Test get_collaborations when bookmark is the first collaboration."""
        mock_collab_instance = MagicMock()
        mock_collab_instance.prefetch_collaborations_ids.return_value = ["org-1", "org-2", "org-3"]
        mock_collab_class.return_value = mock_collab_instance

        state = {"bookmarks": {"test_stream": {"currently_syncing": "org-1"}}}

        collabs, start_index = self.stream.get_collaborations(state)

        self.assertEqual(collabs, ["org-1", "org-2", "org-3"])
        self.assertEqual(start_index, 0)

    @patch("tap_circle_ci.streams.collaborations.Collaborations")
    def test_get_collaborations_empty_list(self, mock_collab_class):
        """Test get_collaborations with empty collaboration list."""
        mock_collab_instance = MagicMock()
        mock_collab_instance.prefetch_collaborations_ids.return_value = []
        mock_collab_class.return_value = mock_collab_instance

        state = {}

        collabs, start_index = self.stream.get_collaborations(state)

        self.assertEqual(collabs, [])
        self.assertEqual(start_index, 0)
