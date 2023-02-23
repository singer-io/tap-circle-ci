import enum
from unittest import TestCase

from tap_circle_ci.client import Client
from tap_circle_ci.helpers import wait_gen


class TestHelperMethods(TestCase):
    """Checking all helper methods for streams."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})

    def test_waitgen(self, *args):
        """Test the WaitGen if it returns `60` seconds as wait time."""
        self.assertEqual(60, wait_gen().__next__())
