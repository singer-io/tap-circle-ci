"""tap-circle-ci product-reviews stream module."""
from typing import Dict, List, Tuple

from singer import (
    Transformer,
    clear_bookmark,
    get_bookmark,
    get_logger,
    metrics,
    write_record,
    write_state,
)

from .abstracts import FullTableStream
from .workflows import Workflows

LOGGER = get_logger()


class PolicyManagement(FullTableStream):
    """class for jobs stream."""

    stream = "policy_management"
    tap_stream_id = "policy_management"
    key_properties = ["id"]
    url_endpoint = "https://circleci.com/api/v2/workflow/WORKFLOW_ID/job"
    project = None
