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


class Http500RequestError(ClientError):
    """class representing 500 status code."""

    message = "Server Fault, Unable to process request"


class Http502RequestError(ClientError):
    """class representing 502 status code."""

    message = "Bad Gateway"


class Http503RequestError(ClientError):
    """class representing 503 status code."""

    message = "Service is currently unavailable"


class Http504RequestError(ClientError):
    """class representing 504 status code."""

    message = "API service time out"
