"""Check if is necessary to update the indicator data."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import INDICATORS

logger = get_logger(__name__, sys.stdout)


@click.command()
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--indicator",
    type=str,
    required=True,
    help=f"Indicator ({', '.join(INDICATORS)})",
)
def main(
    db_url: str,
    indicator: str,
):
    """Check if is necessary to update the indicator data."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    _ = db
    _ = indicator

    print("true")
