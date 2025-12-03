"""tap-circle-ci client module."""
from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import requests
from requests import session, Response
from singer import get_logger

from . import exceptions as errors
from .streams.deploy import LOGGER

logger = get_logger()


def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param resp: requests.Response object
    """
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as http_err:
        try:
            error_code = response.status_code
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

    def get_org_id(self):
        url = "https://circleci.com/api/v2/organization"
        headers, params = self.authenticate({}, {})
        response = self.__make_request(
            "POST",
            url,
            headers=headers,
            params=params,
            json={
                "name": "singer-io",
                "vcs_type": "github"
            }
        )
        if not response:
            raise Exception("CircleCI returned empty response for organization API")
        org_id = response.json().get("id")
        if not org_id:
            raise Exception("Unable to fetch org_id from CircleCI API")

        return org_id

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
            errors.Http500RequestError,
            errors.Http502RequestError,
            errors.Http503RequestError,
            errors.Http504RequestError,
            requests.ConnectionError,
        ),
        jitter=None,
        max_tries=5,
    )
    @backoff.on_exception(
        wait_gen=backoff.expo, exception=errors.Http429RequestError, jitter=None, max_time=60, max_tries=6
    )
    def __make_request(self, method: str, endpoint: str, **kwargs) -> Response | dict[str, list[Any]] | None | Any:
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
                logger.error("Status: %s Message: %s", response.status_code, response.text)
            except AttributeError:
                pass
            try:
                raise_for_error(response)
            except errors.Http401RequestError as err:
                logger.info("Authorization Failure, attempting to regenrate token")
                raise err
            except errors.Http404RequestError:
                logger.error("Resource Not Found %s", response.url or "")
                return self.default_response
            return None
        return response.json()
