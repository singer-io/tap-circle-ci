"""module to test HTTPexceptions for tap-circle-ci."""
import enum
import json
from unittest import TestCase, mock

from requests import HTTPError, Response
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError

import tap_circle_ci.exceptions as errors
from tap_circle_ci.client import Client, raise_for_error


class Mockresponse(Response):
    def __init__(self, status_code, response=None, raise_error=True, text=None):
        super().__init__()
        self.status_code = status_code
        self._content = str.encode(json.dumps(response or {}))
        self.encoding = None
        self.raise_error = raise_error
        self._text = {} if text is None else text
        self.reason = "error"

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code
        raise HTTPError("sample message")

    def json(self):
        return self._text if isinstance(self._text, dict) else {}


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
                self.assertEqual(str(err), "HTTP-error-code: 400, Error: Unable to process request, endpoint: https://test.com/test")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(401))
    def test_401_error_custom_message(self, *args):
        """Unit test to check proper error message for 401 status code."""
        with self.assertRaises(errors.Http401RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http401RequestError as err:
                self.assertEqual(str(err), "HTTP-error-code: 401, Error: Invalid credentials provided, endpoint: https://test.com/test")
                raise err

    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(403))
    def test_403_error_custom_message(self, *args):
        """Unit test to check proper error message for 403 status code."""
        with self.assertRaises(errors.Http403RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http403RequestError as err:
                self.assertEqual(str(err), "HTTP-error-code: 403, Error: Insufficient permission to access resource, endpoint: https://test.com/test")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(429))
    def test_429_error_custom_message(self, *args):
        """Unit test to check proper error message for 429 status code."""
        with self.assertRaises(errors.Http429RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http429RequestError as err:
                self.assertEqual(str(err), "HTTP-error-code: 429, Error: The API limit exceeded, endpoint: https://test.com/test")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(500))
    def test_500_error_custom_message(self, *args):
        """Unit test to check proper error message for 500 status code."""
        with self.assertRaises(errors.Http500RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http500RequestError as err:
                self.assertEqual(str(err), "HTTP-error-code: 500, Error: Server Fault, Unable to process request, endpoint: https://test.com/test")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(502))
    def test_502_error_custom_message(self, *args):
        """Unit test to check proper error message for 502 status code."""
        with self.assertRaises(errors.Http502RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http502RequestError as err:
                self.assertEqual(str(err), "HTTP-error-code: 502, Error: Bad Gateway, endpoint: https://test.com/test")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(503))
    def test_503_error_custom_message(self, *args):
        """Unit test to check proper error message for 503 status code."""
        with self.assertRaises(errors.Http503RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http503RequestError as err:
                self.assertEqual(str(err), "HTTP-error-code: 503, Error: Service is currently unavailable, endpoint: https://test.com/test")
                raise err

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(504))
    def test_504_error_custom_message(self, *args):
        """Unit test to check proper error message for 504 status code."""
        with self.assertRaises(errors.Http504RequestError):
            try:
                self.client_obj.get(self.ENDPOINT, {}, {})
            except errors.Http504RequestError as err:
                self.assertEqual(str(err), "HTTP-error-code: 504, Error: API service time out, endpoint: https://test.com/test")
                raise err


class TestRaiseForErrorWithoutRetry(TestCase):
    """Test raise_for_error for non-retryable status codes."""

    def test_raises_http401_for_401(self):
        """Should raise Http401RequestError for 401 response."""
        resp = Mockresponse(401)
        with self.assertRaises(errors.Http401RequestError) as ctx:
            raise_for_error(resp)
        expected = "HTTP-error-code: 401, Error: Invalid credentials provided"
        self.assertEqual(str(ctx.exception), expected)

    def test_raises_http403_for_403(self):
        """Should raise Http403RequestError for 403 response."""
        resp = Mockresponse(403)
        with self.assertRaises(errors.Http403RequestError) as ctx:
            raise_for_error(resp)
        expected = "HTTP-error-code: 403, Error: Insufficient permission to access resource"
        self.assertEqual(str(ctx.exception), expected)

    def test_raises_http404_for_404(self):
        """Should raise Http404RequestError for 404 response."""
        resp = Mockresponse(404)
        with self.assertRaises(errors.Http404RequestError) as ctx:
            raise_for_error(resp)
        expected = "HTTP-error-code: 404, Error: Resource not found"
        self.assertEqual(str(ctx.exception), expected)

    def test_raises_http400_for_400(self):
        """Should raise Http400RequestError for 400 response."""
        resp = Mockresponse(400)
        with self.assertRaises(errors.Http400RequestError) as ctx:
            raise_for_error(resp)
        expected = "HTTP-error-code: 400, Error: Unable to process request"
        self.assertEqual(str(ctx.exception), expected)


class TestRaiseForErrorWithRetry(TestCase):
    """Test raise_for_error for retryable status codes (Server5xxError subclasses)."""

    def test_raises_http429_for_429(self):
        """Should raise Http429RequestError for 429 response."""
        resp = Mockresponse(429)
        with self.assertRaises(errors.Http429RequestError) as ctx:
            raise_for_error(resp)
        expected = "HTTP-error-code: 429, Error: The API limit exceeded"
        self.assertEqual(str(ctx.exception), expected)

    def test_raises_http500_for_500(self):
        """Should raise Http500RequestError for 500 response."""
        resp = Mockresponse(500)
        with self.assertRaises(errors.Http500RequestError) as ctx:
            raise_for_error(resp)
        expected = "HTTP-error-code: 500, Error: Server Fault, Unable to process request"
        self.assertEqual(str(ctx.exception), expected)

    def test_raises_http502_for_502(self):
        """Should raise Http502RequestError for 502 response."""
        resp = Mockresponse(502)
        with self.assertRaises(errors.Http502RequestError) as ctx:
            raise_for_error(resp)
        expected = "HTTP-error-code: 502, Error: Bad Gateway"
        self.assertEqual(str(ctx.exception), expected)

    def test_raises_http503_for_503(self):
        """Should raise Http503RequestError for 503 response."""
        resp = Mockresponse(503)
        with self.assertRaises(errors.Http503RequestError) as ctx:
            raise_for_error(resp)
        expected = "HTTP-error-code: 503, Error: Service is currently unavailable"
        self.assertEqual(str(ctx.exception), expected)

    def test_raises_http504_for_504(self):
        """Should raise Http504RequestError for 504 response."""
        resp = Mockresponse(504)
        with self.assertRaises(errors.Http504RequestError) as ctx:
            raise_for_error(resp)
        expected = "HTTP-error-code: 504, Error: API service time out"
        self.assertEqual(str(ctx.exception), expected)


class TestMakeRequestHttpFailureWithoutRetry(TestCase):
    """Test that non-retryable errors do not trigger retries."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
    ENDPOINT = "https://circleci.com/api/v2/test"

    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(401))
    def test_401_no_retry(self, mock_request):
        """401 error should not trigger retries."""
        with self.assertRaises(errors.Http401RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 1)

    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(403))
    def test_403_no_retry(self, mock_request):
        """403 error should not trigger retries."""
        with self.assertRaises(errors.Http403RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 1)

    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(404))
    def test_404_returns_default_response(self, mock_request):
        """404 error should return default response without retry."""
        result = self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(result, {"items": []})
        self.assertEqual(mock_request.call_count, 1)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(400))
    def test_400_retries_5_times(self, mock_request, mock_sleep):
        """400 error should trigger 5 retry attempts."""
        with self.assertRaises(errors.Http400RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 5)


class TestMakeRequestHttpFailureWithRetry(TestCase):
    """Test that retryable errors trigger backoff retries."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
    ENDPOINT = "https://circleci.com/api/v2/test"

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(429))
    def test_429_retries_6_times(self, mock_request, mock_sleep):
        """429 error should trigger 6 retry attempts."""
        with self.assertRaises(errors.Http429RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 6)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(500))
    def test_500_retries_5_times(self, mock_request, mock_sleep):
        """500 error should trigger 5 retry attempts."""
        with self.assertRaises(errors.Http500RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(502))
    def test_502_retries_5_times(self, mock_request, mock_sleep):
        """502 error should trigger 5 retry attempts."""
        with self.assertRaises(errors.Http502RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(503))
    def test_503_retries_5_times(self, mock_request, mock_sleep):
        """503 error should trigger 5 retry attempts."""
        with self.assertRaises(errors.Http503RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(504))
    def test_504_retries_5_times(self, mock_request, mock_sleep):
        """504 error should trigger 5 retry attempts."""
        with self.assertRaises(errors.Http504RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 5)


class TestMakeRequestOtherFailureWithRetry(TestCase):
    """Test that connection-level errors trigger retries."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
    ENDPOINT = "https://circleci.com/api/v2/test"

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=ConnectionResetError)
    def test_connection_reset_error_retries(self, mock_request, mock_sleep):
        """ConnectionResetError should trigger 5 retry attempts."""
        with self.assertRaises(ConnectionResetError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=ConnectionError)
    def test_connection_error_retries(self, mock_request, mock_sleep):
        """ConnectionError should trigger 5 retry attempts."""
        with self.assertRaises(ConnectionError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=ChunkedEncodingError)
    def test_chunked_encoding_error_retries(self, mock_request, mock_sleep):
        """ChunkedEncodingError should trigger 5 retry attempts."""
        with self.assertRaises(ChunkedEncodingError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 5)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=Timeout)
    def test_timeout_retries(self, mock_request, mock_sleep):
        """Timeout should trigger 5 retry attempts."""
        with self.assertRaises(Timeout):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, 5)


class TestRaiseForErrorMessageBranches(TestCase):
    """Test that raise_for_error composes error messages correctly
    when the response body contains 'error' or 'message' keys."""

    def test_uses_error_field_from_response_body(self):
        """When response JSON has an 'error' key, use it in the message."""
        resp = Mockresponse(400, text={"error": "bad input data"})
        with self.assertRaises(errors.Http400RequestError) as ctx:
            raise_for_error(resp, endpoint="https://circleci.com/api/v2/test")
        self.assertIn("bad input data", str(ctx.exception))
        self.assertIn("endpoint: https://circleci.com/api/v2/test", str(ctx.exception))

    def test_uses_message_field_from_response_body(self):
        """When response JSON has a 'message' key but no 'error', use it."""
        resp = Mockresponse(500, text={"message": "internal failure"})
        with self.assertRaises(errors.Http500RequestError) as ctx:
            raise_for_error(resp, endpoint="https://circleci.com/api/v2/test")
        self.assertIn("internal failure", str(ctx.exception))
        self.assertIn("endpoint: https://circleci.com/api/v2/test", str(ctx.exception))

    def test_falls_back_to_mapping_message(self):
        """When response JSON has neither 'error' nor 'message', fall back to mapping."""
        resp = Mockresponse(401, text={})
        with self.assertRaises(errors.Http401RequestError) as ctx:
            raise_for_error(resp)
        self.assertEqual(
            str(ctx.exception),
            "HTTP-error-code: 401, Error: Invalid credentials provided",
        )

    def test_error_field_without_endpoint(self):
        """Error field message without endpoint info."""
        resp = Mockresponse(403, text={"error": "forbidden action"})
        with self.assertRaises(errors.Http403RequestError) as ctx:
            raise_for_error(resp)
        self.assertIn("forbidden action", str(ctx.exception))
        self.assertNotIn("endpoint:", str(ctx.exception))

    def test_unmapped_5xx_raises_server5xx(self):
        """An unmapped 5xx code should raise Server5xxError."""
        resp = Mockresponse(599, text={"message": "unknown server error"})
        with self.assertRaises(errors.Server5xxError) as ctx:
            raise_for_error(resp)
        self.assertIn("unknown server error", str(ctx.exception))
