#!/usr/bin/env python3
"""tap-circle-ci module."""
import sys

from singer import get_logger, utils
from singer.metrics import Counter, Timer

from tap_circle_ci.discover import discover
from tap_circle_ci.sync import sync

REQUIRED_CONFIG_KEYS = ["start_date", "token", "project_slugs"]
LOGGER = get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    """performs sync and/or discovery."""
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    if args.discover:
        discover().dump()
    else:
        sync(args.config, args.state, args.catalog or discover(args.config))


if __name__ == "__main__":
    main()

