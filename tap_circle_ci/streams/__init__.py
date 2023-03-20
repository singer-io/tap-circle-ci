"""tap-circle-ci streams module."""

from .jobs import Jobs
from .pipelines import Pipelines
from .workflows import Workflows

STREAMS = {
    Pipelines.tap_stream_id: Pipelines,
    Workflows.tap_stream_id: Workflows,
    Jobs.tap_stream_id: Jobs,
}
