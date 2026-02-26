"""tap-circle-ci client module."""
from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import requests
from requests import session
from singer import get_logger

from . import exceptions as errors

logger = get_logger()

# Maximum number of retries for 5xx server errors
MAX_5XX_RETRIES = 5


def _log_backoff_attempt(details):
    """Structured logging handler for backoff retry attempts."""
    exc = details.get("exception", details.get("value", "unknown"))
    logger.warning(
        "Retry attempt %d/%d for %s after %.1fs wait | exception: %s",
        details["tries"],
        MAX_5XX_RETRIES,
        details.get("args", ("",))[1] if len(details.get("args", ())) > 1 else "unknown endpoint",
        details["wait"],
        exc,
    )


def _log_backoff_giveup(details):
    """Structured logging handler for backoff giveup (all retries exhausted)."""
    exc = details.get("exception", details.get("value", "unknown"))
    logger.error(
        "All %d retries exhausted for %s | final exception: %s",
        details["tries"],
        details.get("args", ("",))[1] if len(details.get("args", ())) > 1 else "unknown endpoint",
        exc,
    )


def raise_for_error(response: requests.Response, endpoint: str = None) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    For 5xx errors, raises a specific exception if one exists (e.g. Http500RequestError),
    otherwise raises the generic Server5xxError with the status code.

    :param response: requests.Response object
    :param endpoint: the URL endpoint for contextual logging
    """
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as http_err:
        try:
            error_code = response.status_code

            # Handle 5xx errors with the Server5xxError hierarchy
            if 500 <= error_code <= 599:
                specific_class = getattr(errors, f"Http{error_code}RequestError", None)
                if specific_class and issubclass(specific_class, errors.Server5xxError):
                    raise specific_class(endpoint=endpoint) from None  # pylint: disable=not-callable
                # Generic 5xx for codes without a dedicated class (e.g. 505, 507, 599)
                raise errors.Server5xxError(
                    message=f"Server error (HTTP {error_code})",
                    status_code=error_code,
                    endpoint=endpoint,
                ) from None

            # Non-5xx errors
            client_exception = getattr(
                errors, f"Http{error_code}RequestError", errors.ClientError(message="Undefined Exception")
            )
            raise client_exception from None
        except (ValueError, TypeError, AttributeError):
            raise errors.ClientError(http_err) from None


class Client:
    """
    A Wrapper class with support for CircleCi api.
    ~~~
    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    default_response = {"items":[]}

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self._circle_token = self.config.get("token")
        self.shared_pipeline_ids = None
        self.shared_workflow_ids = None

    def authenticate(self, headers: Optional[dict], params: Optional[dict]) -> Tuple[Dict, Dict]:
        """Updates Headers and Params based on api version of the stream."""
        headers.update({"Circle-Token": self._circle_token})
        return headers, params

    @backoff.on_exception(wait_gen=backoff.expo, exception=(errors.Http401RequestError,), jitter=None, max_tries=1)
    def get(self, endpoint: str, params: Dict, headers: Dict) -> Any:
        """Calls the make_request method with a prefixed method type `GET`"""
        headers, params = self.authenticate(headers, params)
        return self.__make_request("GET", endpoint, headers=headers, params=params)

    def post(self, endpoint: str, params: Dict, headers: Dict, body: Dict) -> Any:
        """Calls the make_request method with a prefixed method type `POST`"""
        # pylint: disable=R0913
        headers, params = self.authenticate(headers, params)
        self.__make_request("POST", endpoint, headers=headers, params=params, data=body)

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            errors.Http400RequestError,
            errors.Server5xxError,
            requests.ConnectionError,
        ),
        jitter=None,
        max_tries=MAX_5XX_RETRIES,
        on_backoff=_log_backoff_attempt,
        on_giveup=_log_backoff_giveup,
    )
    @backoff.on_exception(
        wait_gen=backoff.expo, exception=errors.Http429RequestError, jitter=None, max_time=60, max_tries=6
    )
    def __make_request(self, method: str, endpoint: str, **kwargs) -> Optional[Mapping[Any, Any]]:
        """
        Performs HTTP Operations
        Args:
            method (str): represents the state file for the tap.
            endpoint (str): url of the resource that needs to be fetched
            params (dict): A mapping for url params eg: ?name=Avery&age=3
            headers (dict): A mapping for the headers that need to be sent
            body (dict): only applicable to post request, body of the request

        Returns:
            Dict,List,None: Returns a `Json Parsed` HTTP Response or None if exception
        """
        response = self._session.request(method, endpoint, **kwargs)
        if response.status_code == 201:
            return response
        if response.status_code != 200:
            try:
                logger.error(
                    "HTTP %s error | endpoint: %s | method: %s | response: %s",
                    response.status_code,
                    endpoint,
                    method,
                    response.text,
                )
            except AttributeError:
                pass
            try:
                raise_for_error(response, endpoint=endpoint)
            except errors.Http401RequestError as err:
                logger.info("Authorization Failure, attempting to regenerate token")
                raise err
            except errors.Http404RequestError:
                logger.error("Resource Not Found %s", response.url or "")
                return self.default_response
            except errors.Server5xxError:
                # Re-raise so that the backoff decorator can handle retries
                raise
            return None
        return response.json()
