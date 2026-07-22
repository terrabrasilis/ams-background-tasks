"""Check if is necessary to update the indicator data."""

from __future__ import annotations

import os
import sys
from datetime import datetime

import click
import pytz

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
@click.option(
    "--frequency",
    type=int,
    required=False,
    default=0,
    help="Frequency in seconds to update the indicator.",
)
@click.option(
    "--hour-force-update",
    type=click.IntRange(0, 23),
    required=False,
    default=0,
    help="Hour in UTC after which one update is forced.",
)
def main(db_url: str, indicator: str, frequency: int, hour_force_update: int):
    """Check if is necessary to update the indicator data."""
    assert indicator in INDICATORS

    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    name = "processing"
    schema = "public"

    sql = f"""
        SELECT start_process from {schema}.{name}
        WHERE
            indicator='{indicator}'
            AND process='update'
            AND status='completed'
            ORDER BY start_process DESC
        LIMIT 1;
    """

    res: datetime = db.fetchone(query=sql)

    if res is None:
        print(True)
        return

    logger.info(res)

    now = datetime.now(pytz.UTC)

    force_update_at = now.replace(
        hour=hour_force_update,
        minute=0,
        second=0,
        microsecond=0,
    )

    # should_force_update = (now >= force_update_at) and (res < force_update_at)
    should_force_update = res < force_update_at <= now

    should_update_by_frequency = (now - res).total_seconds() > frequency

    status = should_force_update or should_update_by_frequency

    print(status)
