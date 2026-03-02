"""tap-circle-ci client module."""
from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import requests
from requests import session
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from singer import get_logger

from .exceptions import (
    ERROR_CODE_EXCEPTION_MAPPING, ClientError, CircleCiBackoffError,
    Http401RequestError, Http404RequestError
)

logger = get_logger()


def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param response: requests.Response object
    """
    try:
        response_json = response.json()
    except Exception:
        response_json = {}
    if not isinstance(response_json, dict):
        response_json = {}
    if response.status_code not in [200, 201, 204]:
        if response_json.get("error"):
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('error')}"
        else:
            error_message = ERROR_CODE_EXCEPTION_MAPPING.get(
                response.status_code, {}
            ).get("message", "Unknown Error")
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('message', error_message)}"

        # For 5xx errors, use backoff exception if not specifically mapped
        if 500 <= response.status_code < 600:
            exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
                "raise_exception", CircleCiBackoffError
            )
        else:
            exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
                "raise_exception", ClientError
            )
        raise exc(message, response) from None


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

    @backoff.on_exception(wait_gen=backoff.expo, exception=(CircleCiBackoffError,), jitter=None, max_tries=1)
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
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            CircleCiBackoffError,
        ),
        max_tries=5,
        factor=2,
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
                raise_for_error(response)
            except CircleCiBackoffError:
                raise
            except Http401RequestError as err:
                logger.info("Authorization Failure, attempting to regenerate token")
                raise err
            except Http404RequestError:
                logger.error("Resource Not Found %s", response.url or "")
                return self.default_response
            return None
        return response.json()
