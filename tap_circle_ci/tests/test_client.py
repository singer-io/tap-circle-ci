"""module to test HTTPexceptions for tap-circle-ci."""
import enum
import json
from unittest import TestCase, mock

from requests import HTTPError, Response

import tap_circle_ci.exceptions as errors
from tap_circle_ci.client import Client


class Mockresponse(Response):
    def __init__(self, status_code, response=None, raise_error=True):
        super().__init__()
        self.status_code = status_code
        self._content = str.encode(json.dumps(response))
        self.encoding = None
        self.raise_error = raise_error

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code
        raise HTTPError("sample message")


class HTTPErrorCodeHandling(TestCase):
    """Test cases to verify error is raised with proper message for Http
    Errors."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
    ENDPOINT = "https://test.com/test"

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(400))
    def test_400_error_custom_message(self, *args):
        """Unit test to check proper error message for 400 status code."""
        with self.assertRaises(errors.Http400RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http400RequestError as err:
                self.assertEqual(str(err), "Unable to process request")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(401))
    def test_401_error_custom_message(self, *args):
        """Unit test to check proper error message for 401 status code."""
        with self.assertRaises(errors.Http401RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http401RequestError as err:
                self.assertEqual(str(err), "Invalid credentials provided")
                raise err

    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(403))
    def test_403_error_custom_message(self, *args):
        """Unit test to check proper error message for 403 status code."""
        with self.assertRaises(errors.Http403RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http401RequestError as err:
                self.assertEqual(str(err), "Insufficient permission to access resource")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(429))
    def test_429_error_custom_message(self, *args):
        """Unit test to check proper error message for 429 status code."""
        with self.assertRaises(errors.Http429RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http429RequestError as err:
                self.assertEqual(str(err), "The API limit exceeded")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(500))
    def test_500_error_custom_message(self, *args):
        """Unit test to check proper error message for 500 status code."""
        with self.assertRaises(errors.Http500RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http500RequestError as err:
                self.assertEqual(str(err), "Server Fault, Unable to process request")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(502))
    def test_502_error_custom_message(self, *args):
        """Unit test to check proper error message for 502 status code."""
        with self.assertRaises(errors.Http502RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http502RequestError as err:
                self.assertEqual(str(err), "Bad Gateway")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(503))
    def test_503_error_custom_message(self, *args):
        """Unit test to check proper error message for 503 status code."""
        with self.assertRaises(errors.Http503RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http503RequestError as err:
                self.assertEqual(str(err), "Service is currently unavailable")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(504))
    def test_504_error_custom_message(self, *args):
        """Unit test to check proper error message for 504 status code."""
        with self.assertRaises(errors.Http504RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http504RequestError as err:
                self.assertEqual(str(err), "API service time out")
                raise err