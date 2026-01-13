"""Unit tests for stream pagination."""
import unittest
from unittest.mock import patch, MagicMock
from tap_circle_ci.streams.abstracts import BaseStream, FullTableStream


class ConcretePaginatedStream(FullTableStream):
    """Concrete implementation for testing pagination."""

    stream = "test_pagination"
    tap_stream_id = "test_pagination"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    valid_replication_keys = None
    replication_key = None
    url_endpoint = "https://circleci.com/api/v2/test"

    def get_url(self, org_id=None):
        return self.url_endpoint

    def get_records(self, org_id=None):
        """Mock implementation for testing."""
        return []


class TestPagination(unittest.TestCase):
    """Test pagination functionality in streams."""

    def setUp(self):
        """Setup test fixtures."""
        mock_client = MagicMock()
        self.stream = ConcretePaginatedStream(client=mock_client)

    def test_iter_pages_single_page(self):
        """Test iter_pages with single page response."""
        page1 = {
            "items": [{"id": "1"}, {"id": "2"}],
            "next_page_token": None
        }
        self.stream.client.get.return_value = page1

        pages = list(self.stream.iter_pages("https://circleci.com/api/v2/test"))

        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0], page1)

    def test_iter_pages_multiple_pages(self):
        """Test iter_pages with multiple pages."""
        page1 = {
            "items": [{"id": "1"}],
            "next_page_token": "token1"
        }
        page2 = {
            "items": [{"id": "2"}],
            "next_page_token": "token2"
        }
        page3 = {
            "items": [{"id": "3"}],
            "next_page_token": None
        }

        self.stream.client.get.side_effect = [page1, page2, page3]

        pages = list(self.stream.iter_pages("https://circleci.com/api/v2/test"))

        self.assertEqual(len(pages), 3)
        self.assertEqual(pages[0]["items"][0]["id"], "1")
        self.assertEqual(pages[1]["items"][0]["id"], "2")
        self.assertEqual(pages[2]["items"][0]["id"], "3")

    def test_iter_pages_with_initial_token(self):
        """Test iter_pages starting with a page token."""
        page1 = {
            "items": [{"id": "2"}],
            "next_page_token": "token2"
        }
        page2 = {
            "items": [{"id": "3"}],
            "next_page_token": None
        }

        self.stream.client.get.side_effect = [page1, page2]
        pages = list(self.stream.iter_pages("https://circleci.com/api/v2/test", token="token1"))

        self.assertEqual(len(pages), 2)
        first_call_args = self.stream.client.get.call_args_list[0]
        self.assertEqual(first_call_args[0][1]["page-token"], "token1")

    def test_iter_pages_empty_response(self):
        """Test iter_pages with empty response."""
        empty_page = {
            "items": [],
            "next_page_token": None
        }
        self.stream.client.get.return_value = empty_page
        pages = list(self.stream.iter_pages("https://circleci.com/api/v2/test"))

        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0]["items"], [])

    def test_iter_pages_stops_on_duplicate_token(self):
        """Test iter_pages stops when receiving duplicate page token."""
        page1 = {
            "items": [{"id": "1"}],
            "next_page_token": "token1"
        }
        page2 = {
            "items": [{"id": "2"}],
            "next_page_token": "token1"  # Same token - infinite loop prevention
        }

        self.stream.client.get.side_effect = [page1, page2]
        pages = list(self.stream.iter_pages("https://circleci.com/api/v2/test"))

        # Should stop after detecting duplicate token
        self.assertEqual(len(pages), 2)
