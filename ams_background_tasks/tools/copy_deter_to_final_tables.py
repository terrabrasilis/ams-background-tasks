"""Update the AMS DETER tables."""

from __future__ import annotations

import logging
import os

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.tools.common import DETER_INDICATOR, finalize_processing

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--all-data",
    required=False,
    is_flag=True,
    default=False,
    help="if True, all data of external database will be processed.",
)
def main(db_url: str, all_data: bool):
    """Copy the temporary data tables to the final tables."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    tables = (
        ("deter", "deter_auth", "deter_history")
        if all_data
        else ("deter", "deter_auth")
    )

    prefix = "tmp_"

    for table in tables:
        db.truncate(table=f"deter.{table}")
        db.copy_table(src=f"deter.{prefix}{table}", dst=f"deter.{table}")

    table = "deter_publish_date"
    db.truncate(table=f"deter.{table}")
    db.copy_table(src=f"deter.{prefix}{table}", dst=f"deter.{table}")

    # is it still necessary?
    create_tmp_table(db=db, all_data=all_data, truncate=True)

    finalize_processing(
        db=db, indicator=DETER_INDICATOR, process="update", status="completed"
    )

    db.commit()


def create_tmp_table(db: DatabaseFacade, all_data: bool, truncate: bool):
    """Create a temporary table with DETER alerts to ensure gist index creation."""
    name = "tmp_data"
    table = f"deter.{name}"

    if truncate:
        db.truncate(table=table)

    union = ""
    if all_data:
        union = """
            UNION
            SELECT gid, classname, date, areamunkm, geom, geocode, biome
            FROM deter.deter_history
        """

    sql = f"""
        INSERT INTO {table} (
            gid, classname, date, areamunkm, geom, geocode, biome
        )
        SELECT gid, classname, date, areamunkm, geom, geocode, biome
        FROM deter.deter_auth
        {union}            
    """

    db.execute(sql=sql)

    logger.info("The DETER temporary table has been created.")
