"""tap-circle-ci exception classes module."""

class ClientError(Exception):
    """class representing Generic Http error."""

    message = None

    def __init__(self, message=None, response=None):
        super().__init__(message or self.message)
        self.response = response


class Http400RequestError(ClientError):
    """class representing 400 status code."""

    message = "Unable to process request"


class Http401RequestError(ClientError):
    """class representing 401 status code."""

    message = "Invalid credentials provided"


class Http403RequestError(ClientError):
    """class representing 403 status code."""

    message = "Insufficient permission to access resource"


class Http404RequestError(ClientError):
    """class representing 404 status code."""

    message = "Resource not found"


class Http429RequestError(ClientError):
    """class representing 429 status code."""

    message = "The API limit exceeded"


class Server5xxError(ClientError):
    """Base class for all 5xx server errors (500-599).

    This serves as the catch-all for any server-side error, enabling
    unified retry logic for all 5xx status codes.
    """

    status_code = None
    message = "Server error"

    def __init__(self, message=None, response=None, status_code=None, endpoint=None):
        self.status_code = status_code or self.__class__.status_code
        self.endpoint = endpoint
        final_message = message or self.message
        if self.status_code:
            final_message = f"HTTP {self.status_code}: {final_message}"
        if self.endpoint:
            final_message = f"{final_message} (endpoint: {self.endpoint})"
        super().__init__(message=final_message, response=response)


class Http500RequestError(Server5xxError):
    """class representing 500 status code."""

    status_code = 500
    message = "Server Fault, Unable to process request"


class Http502RequestError(Server5xxError):
    """class representing 502 status code."""

    status_code = 502
    message = "Bad Gateway"


class Http503RequestError(Server5xxError):
    """class representing 503 status code."""

    status_code = 503
    message = "Service is currently unavailable"


class Http504RequestError(Server5xxError):
    """class representing 504 status code."""

    status_code = 504
    message = "API service time out"
