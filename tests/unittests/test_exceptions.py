"""module to test HTTPexceptions for tap-circle-ci."""
import enum
import json
from unittest import TestCase, mock

import tap_circle_ci.exceptions as errors
from requests import HTTPError, Response
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
                self.client_obj.get(self.ENDPOINT, None, None)
            except errors.Http400RequestError as _:
                self.assertEqual(str(_), "Unable to process request")
                raise _

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(401))
    def test_401_error_custom_message(self, *args):
        """Unit test to check proper error message for 401 status code."""
        with self.assertRaises(errors.Http401RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, None, None)
            except errors.Http401RequestError as _:
                self.assertEqual(str(_), "Invalid credentials provided")
                raise _

    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(403))
    def test_403_error_custom_message(self, *args):
        """Unit test to check proper error message for 403 status code."""
        with self.assertRaises(errors.Http403RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, None, None)
            except errors.Http401RequestError as _:
                self.assertEqual(str(_), "Insufficient permission to access resource")
                raise _

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(404))
    def test_404_error_custom_message(self, *args):
        """Unit test to check proper error message for 404 status code."""
        with self.assertRaises(errors.Http404RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, None, None)
            except errors.Http404RequestError as _:
                self.assertEqual(str(_), "Resource not found")
                raise _

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(429))
    def test_429_error_custom_message(self, *args):
        """Unit test to check proper error message for 429 status code."""
        with self.assertRaises(errors.Http429RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, None, None)
            except errors.Http429RequestError as _:
                self.assertEqual(str(_), "The API limit exceeded")
                raise _

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(500))
    def test_500_error_custom_message(self, *args):
        """Unit test to check proper error message for 500 status code."""
        with self.assertRaises(errors.Http500RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, None, None)
            except errors.Http500RequestError as _:
                self.assertEqual(str(_), "Server Fault, Unable to process request")
                raise _

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(502))
    def test_502_error_custom_message(self, *args):
        """Unit test to check proper error message for 502 status code."""
        with self.assertRaises(errors.Http502RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, None, None)
            except errors.Http502RequestError as _:
                self.assertEqual(str(_), "Bad Gateway")
                raise _

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(503))
    def test_503_error_custom_message(self, *args):
        """Unit test to check proper error message for 503 status code."""
        with self.assertRaises(errors.Http503RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, None, None)
            except errors.Http503RequestError as _:
                self.assertEqual(str(_), "Service is currently unavailable")
                raise _

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(504))
    def test_504_error_custom_message(self, *args):
        """Unit test to check proper error message for 504 status code."""
        with self.assertRaises(errors.Http504RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, None, None)
            except errors.Http504RequestError as _:
                self.assertEqual(str(_), "API service time out")
                raise _
