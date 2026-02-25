"""Unit tests for 5xx server error handling, retry logic, and backoff."""
import enum
import json
from unittest import TestCase, mock

from requests import HTTPError, Response

import tap_circle_ci.exceptions as errors
from tap_circle_ci.client import Client, raise_for_error, MAX_5XX_RETRIES


class Mockresponse(Response):
    """Mock HTTP response for testing."""

    def __init__(self, status_code, response=None, raise_error=True):
        super().__init__()
        self.status_code = status_code
        self._content = str.encode(json.dumps(response or {}))
        self.encoding = None
        self.raise_error = raise_error

    def raise_for_status(self):
        if not self.raise_error:
            return self.status_code
        raise HTTPError(f"HTTP {self.status_code} error")


class TestServer5xxExceptionHierarchy(TestCase):
    """Test that all 5xx exceptions inherit from Server5xxError."""

    def test_500_is_server_5xx(self):
        """Http500RequestError should be a subclass of Server5xxError."""
        self.assertTrue(issubclass(errors.Http500RequestError, errors.Server5xxError))

    def test_502_is_server_5xx(self):
        """Http502RequestError should be a subclass of Server5xxError."""
        self.assertTrue(issubclass(errors.Http502RequestError, errors.Server5xxError))

    def test_503_is_server_5xx(self):
        """Http503RequestError should be a subclass of Server5xxError."""
        self.assertTrue(issubclass(errors.Http503RequestError, errors.Server5xxError))

    def test_504_is_server_5xx(self):
        """Http504RequestError should be a subclass of Server5xxError."""
        self.assertTrue(issubclass(errors.Http504RequestError, errors.Server5xxError))

    def test_server_5xx_is_client_error(self):
        """Server5xxError should be a subclass of ClientError."""
        self.assertTrue(issubclass(errors.Server5xxError, errors.ClientError))

    def test_generic_5xx_includes_status_code(self):
        """Server5xxError message should include the status code."""
        err = errors.Server5xxError(status_code=505)
        self.assertIn("505", str(err))

    def test_generic_5xx_includes_endpoint(self):
        """Server5xxError message should include the endpoint."""
        err = errors.Server5xxError(endpoint="https://api.example.com/data")
        self.assertIn("https://api.example.com/data", str(err))

    def test_specific_5xx_includes_status_code_in_message(self):
        """Specific 5xx errors should include status code in message."""
        err = errors.Http500RequestError()
        self.assertIn("500", str(err))
        self.assertIn("Server Fault", str(err))


class TestRaiseForError5xx(TestCase):
    """Test raise_for_error function for 5xx status codes."""

    def test_raises_http500_for_500(self):
        """Should raise Http500RequestError for 500 response."""
        resp = Mockresponse(500)
        with self.assertRaises(errors.Http500RequestError):
            raise_for_error(resp, endpoint="https://api.test.com/endpoint")

    def test_raises_http502_for_502(self):
        """Should raise Http502RequestError for 502 response."""
        resp = Mockresponse(502)
        with self.assertRaises(errors.Http502RequestError):
            raise_for_error(resp, endpoint="https://api.test.com/endpoint")

    def test_raises_http503_for_503(self):
        """Should raise Http503RequestError for 503 response."""
        resp = Mockresponse(503)
        with self.assertRaises(errors.Http503RequestError):
            raise_for_error(resp, endpoint="https://api.test.com/endpoint")

    def test_raises_http504_for_504(self):
        """Should raise Http504RequestError for 504 response."""
        resp = Mockresponse(504)
        with self.assertRaises(errors.Http504RequestError):
            raise_for_error(resp, endpoint="https://api.test.com/endpoint")

    def test_raises_generic_5xx_for_505(self):
        """Should raise Server5xxError for unrecognized 5xx codes."""
        resp = Mockresponse(505)
        with self.assertRaises(errors.Server5xxError):
            raise_for_error(resp, endpoint="https://api.test.com/endpoint")

    def test_raises_generic_5xx_for_507(self):
        """Should raise Server5xxError for HTTP 507."""
        resp = Mockresponse(507)
        with self.assertRaises(errors.Server5xxError) as ctx:
            raise_for_error(resp, endpoint="https://api.test.com/data")
        self.assertIn("507", str(ctx.exception))

    def test_raises_generic_5xx_for_599(self):
        """Should raise Server5xxError for HTTP 599."""
        resp = Mockresponse(599)
        with self.assertRaises(errors.Server5xxError):
            raise_for_error(resp, endpoint="https://api.test.com/endpoint")

    def test_5xx_error_includes_endpoint_in_message(self):
        """5xx error should include endpoint context."""
        resp = Mockresponse(503)
        with self.assertRaises(errors.Server5xxError) as ctx:
            raise_for_error(resp, endpoint="https://api.test.com/resource")
        self.assertIn("https://api.test.com/resource", str(ctx.exception))

    def test_non_5xx_errors_not_caught_as_server5xx(self):
        """400-level errors should not be raised as Server5xxError."""
        resp = Mockresponse(400)
        with self.assertRaises(errors.Http400RequestError):
            raise_for_error(resp)

        # Verify it's NOT a Server5xxError
        try:
            raise_for_error(Mockresponse(400))
        except errors.Server5xxError:
            self.fail("400 should not raise Server5xxError")
        except errors.ClientError:
            pass  # Expected


class TestRetryOn5xx(TestCase):
    """Test that 5xx errors trigger retries with backoff."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
    ENDPOINT = "https://circleci.com/api/v2/test"

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(500))
    def test_500_retries_max_times(self, mock_request, mock_sleep):
        """500 error should trigger MAX_5XX_RETRIES attempts before raising."""
        with self.assertRaises(errors.Http500RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, MAX_5XX_RETRIES)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(502))
    def test_502_retries_max_times(self, mock_request, mock_sleep):
        """502 error should trigger MAX_5XX_RETRIES attempts before raising."""
        with self.assertRaises(errors.Http502RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, MAX_5XX_RETRIES)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(503))
    def test_503_retries_max_times(self, mock_request, mock_sleep):
        """503 error should trigger MAX_5XX_RETRIES attempts before raising."""
        with self.assertRaises(errors.Http503RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, MAX_5XX_RETRIES)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(504))
    def test_504_retries_max_times(self, mock_request, mock_sleep):
        """504 error should trigger MAX_5XX_RETRIES attempts before raising."""
        with self.assertRaises(errors.Http504RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, MAX_5XX_RETRIES)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(505))
    def test_generic_5xx_retries_max_times(self, mock_request, mock_sleep):
        """Unregistered 5xx codes (e.g. 505) should also retry."""
        with self.assertRaises(errors.Server5xxError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        self.assertEqual(mock_request.call_count, MAX_5XX_RETRIES)


class TestRetryRecovery(TestCase):
    """Test that requests succeed after transient 5xx then recovery."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
    ENDPOINT = "https://circleci.com/api/v2/test"

    @mock.patch("time.sleep")
    def test_recovers_after_transient_500(self, mock_sleep):
        """Request should succeed if 500s stop before max retries."""
        success_response = Mockresponse(200, response={"items": [{"id": 1}]}, raise_error=False)
        fail_response = Mockresponse(500)

        # Fail twice, then succeed
        side_effects = [fail_response, fail_response, success_response]
        with mock.patch("requests.Session.request", side_effect=side_effects) as mock_request:
            result = self.client_obj.get(self.ENDPOINT, {}, {})
            self.assertEqual(result, {"items": [{"id": 1}]})
            self.assertEqual(mock_request.call_count, 3)

    @mock.patch("time.sleep")
    def test_recovers_after_transient_503(self, mock_sleep):
        """Request should succeed if 503s stop before max retries."""
        success_response = Mockresponse(200, response={"items": []}, raise_error=False)
        fail_response = Mockresponse(503)

        side_effects = [fail_response, success_response]
        with mock.patch("requests.Session.request", side_effect=side_effects) as mock_request:
            result = self.client_obj.get(self.ENDPOINT, {}, {})
            self.assertEqual(result, {"items": []})
            self.assertEqual(mock_request.call_count, 2)


class TestBackoffTiming(TestCase):
    """Test that exponential backoff occurs between retries."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
    ENDPOINT = "https://circleci.com/api/v2/test"

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(500))
    def test_exponential_backoff_sleep_called(self, mock_request, mock_sleep):
        """Backoff should call time.sleep between retries."""
        with self.assertRaises(errors.Http500RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        # backoff calls time.sleep for each retry after the first attempt
        # With max_tries=5, there should be 4 sleep calls (retries 2-5)
        self.assertEqual(mock_sleep.call_count, MAX_5XX_RETRIES - 1)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(500))
    def test_backoff_delays_increase(self, mock_request, mock_sleep):
        """Backoff delays should increase exponentially."""
        with self.assertRaises(errors.Http500RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})
        # Verify sleep was called with increasing delays
        sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
        for i in range(1, len(sleep_calls)):
            self.assertGreater(
                sleep_calls[i], sleep_calls[i - 1],
                f"Backoff delay {i} ({sleep_calls[i]}) should be greater than "
                f"delay {i-1} ({sleep_calls[i-1]})"
            )


class TestBackoffLogging(TestCase):
    """Test structured logging during retry attempts."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
    ENDPOINT = "https://circleci.com/api/v2/test"

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(500))
    @mock.patch("tap_circle_ci.client.logger")
    def test_retry_attempts_logged(self, mock_logger, mock_request, mock_sleep):
        """Each retry attempt should be logged as a warning."""
        with self.assertRaises(errors.Http500RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})

        # Check that warning log was called for retry attempts
        warning_calls = mock_logger.warning.call_args_list
        retry_logs = [c for c in warning_calls if "Retry attempt" in str(c)]
        # Should have (MAX_5XX_RETRIES - 1) retry attempt logs
        self.assertEqual(len(retry_logs), MAX_5XX_RETRIES - 1)

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(503))
    @mock.patch("tap_circle_ci.client.logger")
    def test_giveup_logged_on_final_failure(self, mock_logger, mock_request, mock_sleep):
        """Final failure after all retries should be logged as error."""
        with self.assertRaises(errors.Http503RequestError):
            self.client_obj.get(self.ENDPOINT, {}, {})

        # Check that error log was called for giveup
        error_calls = mock_logger.error.call_args_list
        giveup_logs = [c for c in error_calls if "retries exhausted" in str(c)]
        self.assertGreaterEqual(len(giveup_logs), 1, "Expected giveup log after all retries failed")


class TestStateIntegrity(TestCase):
    """Test that state/bookmark integrity is maintained during 5xx errors."""

    @mock.patch("time.sleep")
    @mock.patch("requests.Session.request", side_effect=lambda *_, **__: Mockresponse(500))
    def test_client_returns_default_response_not_corrupted(self, mock_request, mock_sleep):
        """The default response should remain unchanged after 5xx errors."""
        client = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
        original_default = client.default_response.copy()

        with self.assertRaises(errors.Http500RequestError):
            client.get("https://api.test.com/test", {}, {})

        # Default response should be unchanged
        self.assertEqual(client.default_response, original_default)

    def test_404_returns_default_response_during_5xx_era(self):
        """Even after 5xx errors, 404s should still return default response."""
        client = Client({"api_key": enum.auto(), "api_secret": enum.auto()})

        with mock.patch("requests.Session.request",
                        side_effect=lambda *_, **__: Mockresponse(404)):
            result = client.get("https://api.test.com/not-found", {}, {})
            self.assertEqual(result, {"items": []})


class TestIdempotency(TestCase):
    """Test that retries are idempotent and safe for GET requests."""

    client_obj = Client({"api_key": enum.auto(), "api_secret": enum.auto()})
    ENDPOINT = "https://circleci.com/api/v2/test"

    @mock.patch("time.sleep")
    def test_get_retries_are_idempotent(self, mock_sleep):
        """GET retries should call the same endpoint with same params each time."""
        call_args_list = []

        def capture_request(*args, **kwargs):
            call_args_list.append((args, kwargs))
            return Mockresponse(500)

        with mock.patch("requests.Session.request", side_effect=capture_request):
            with self.assertRaises(errors.Http500RequestError):
                self.client_obj.get(self.ENDPOINT, {"page-token": "abc"}, {})

        # All calls should have identical arguments
        for i in range(1, len(call_args_list)):
            self.assertEqual(
                call_args_list[0][1],  # kwargs of first call
                call_args_list[i][1],  # kwargs of nth call
                f"Retry call {i} had different parameters than call 0"
            )
