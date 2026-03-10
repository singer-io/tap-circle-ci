"""tap-circle-ci exception classes module."""


class ClientError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class CircleCiBackoffError(ClientError):
    """class representing backoff error handling."""
    pass


class Http400RequestError(ClientError):
    """class representing 400 status code."""
    pass


class Http401RequestError(ClientError):
    """class representing 401 status code."""
    pass


class Http403RequestError(ClientError):
    """class representing 403 status code."""
    pass


class Http404RequestError(ClientError):
    """class representing 404 status code."""
    pass


class Http429RequestError(ClientError):
    """class representing 429 status code."""
    pass


class Http500RequestError(CircleCiBackoffError):
    """class representing 500 status code."""
    pass


class Http502RequestError(CircleCiBackoffError):
    """class representing 502 status code."""
    pass


class Http503RequestError(CircleCiBackoffError):
    """class representing 503 status code."""
    pass


class Http504RequestError(CircleCiBackoffError):
    """class representing 504 status code."""
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": Http400RequestError,
        "message": "Unable to process request"
    },
    401: {
        "raise_exception": Http401RequestError,
        "message": "Invalid credentials provided"
    },
    403: {
        "raise_exception": Http403RequestError,
        "message": "Insufficient permission to access resource"
    },
    404: {
        "raise_exception": Http404RequestError,
        "message": "Resource not found"
    },
    429: {
        "raise_exception": Http429RequestError,
        "message": "The API limit exceeded"
    },
    500: {
        "raise_exception": Http500RequestError,
        "message": "Server Fault, Unable to process request"
    },
    502: {
        "raise_exception": Http502RequestError,
        "message": "Bad Gateway"
    },
    503: {
        "raise_exception": Http503RequestError,
        "message": "Service is currently unavailable"
    },
    504: {
        "raise_exception": Http504RequestError,
        "message": "API service time out"
    }
}
