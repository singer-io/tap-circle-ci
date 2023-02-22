"""tap-circle-ci helper functions module."""
import os

from singer import get_logger

LOGGER = get_logger()


def get_abs_path(path: str) -> str:
    """Returns absolute path for URL."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def wait_gen():
    """Returns a generator that is passed to backoff decorator to indicate how
    long to backoff for in seconds."""
    while True:
        LOGGER.info("API exception occurred sleeping for 60 seconds")
        yield 60
