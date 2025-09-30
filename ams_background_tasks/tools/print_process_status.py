"""Print the last database update status."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime

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
    "--start",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d %H:%M:%S"]),
    help="Datetime of processing start.",
)
def main(
    db_url: str,
    start: datetime,
):
    """Print the last database update status."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    name = "processing"
    schema = "public"

    res = {}

    for indicator in INDICATORS:
        if "ibama" in indicator:
            continue

        res[indicator] = {}
        for process in ["update", "classification-ams", "classification-ppcdam"]:
            sql = f"""
                SELECT status from {schema}.{name}
                WHERE
                    indicator='{indicator}'
                    AND process='{process}'
                    AND start_process>='{start.strftime("%Y-%m-%d %H:%M:%S")}'
                ORDER BY start_process DESC
                LIMIT 1;
            """

            _res = db.fetchone(query=sql)

            res[indicator][process] = _res is not None and _res == "completed"

    print(json.dumps(res))
