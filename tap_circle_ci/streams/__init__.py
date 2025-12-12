"""tap-circle-ci streams module."""

from .jobs import Jobs
from .pipelines import Pipelines
from .workflows import Workflows
from .context import Context
from .deploy import Deploy
from .groups import Groups
from .pipeline_definition import PipelineDefinition
from .project import Project
from .schedule import Schedule
from .trigger import Trigger
from .collaborations import Collaborations

STREAMS = {
    Pipelines.tap_stream_id: Pipelines,
    Workflows.tap_stream_id: Workflows,
    Jobs.tap_stream_id: Jobs,
    Context.tap_stream_id: Context,
    Deploy.tap_stream_id: Deploy,
    Groups.tap_stream_id: Groups,
    PipelineDefinition.tap_stream_id: PipelineDefinition,
    Project.tap_stream_id: Project,
    Schedule.tap_stream_id: Schedule,
    Trigger.tap_stream_id: Trigger,
    Collaborations.tap_stream_id: Collaborations,
}
