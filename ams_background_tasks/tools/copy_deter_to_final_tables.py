"""Update the AMS DETER tables."""

from __future__ import annotations

import logging
import os

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.tools.common import (
    DETER_INDICATOR,
    analyze_table,
    finalize_processing,
    optimize_table,
    prepare_table_to_update,
)

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

    prefix = "tmp_"

    index_columns = [
        "classname:btree",
        "view_date:btree",
        "biome:btree",
        "geocode:btree",
        "biome,view_date:btree",
        "geom:gist",
    ]

    table = "deter_auth"
    db.truncate(table=f"deter.{table}")
    prepare_table_to_update(db=db, schema="deter", name=table, columns=index_columns)
    db.copy_table(
        src=f"deter.{prefix}{table}",
        dst=f"deter.{table}",
        cols_to_ignore=[],
        with_commit=False,
    )
    optimize_table(db=db, schema="deter", name=table, columns=index_columns)

    table = "deter_publish_date"
    db.truncate(table=f"deter.{table}")
    db.copy_table(
        src=f"deter.{prefix}{table}",
        dst=f"deter.{table}",
        cols_to_ignore=[],
        with_commit=False,
    )

    # updating deter.deter from deter.deter_auth
    dst = "deter"
    src = "deter_auth"

    table = dst

    db.truncate(table=f"deter.{table}")
    prepare_table_to_update(db=db, schema="deter", name=table, columns=index_columns)

    sql = "SELECT biome from deter.deter_publish_date;"
    biomes = [_[0] for _ in db.fetchall(sql)]

    for biome in biomes:
        sql = f"SELECT date from deter.deter_publish_date WHERE biome='{biome}';"
        publish_date = db.fetchone(sql)
        db.copy_table(
            src=f"deter.{src}",
            dst=f"deter.{dst}",
            cols_to_ignore=[],
            with_commit=False,
            conditions=f"WHERE view_date<='{publish_date}'::date AND biome='{biome}'",
        )

    optimize_table(db=db, schema="deter", name=table, columns=index_columns)

    # is it still necessary?
    create_tmp_table(db=db, all_data=all_data, truncate=True)

    finalize_processing(
        db=db, indicator=DETER_INDICATOR, process="update", status="completed"
    )

    db.commit()

    for table in ("deter", "deter_auth", "tmp_data"):
        analyze_table(db=db, schema="deter", name=table)


def create_tmp_table(db: DatabaseFacade, all_data: bool, truncate: bool):
    """Create a temporary table with DETER alerts to ensure gist index creation."""
    _ = all_data  # no warn

    name = "tmp_data"
    table = f"deter.{name}"

    index_columns = [
        "classname:btree",
        "view_date:btree",
        "biome:btree",
        "geocode:btree",
        "biome,view_date:btree",
        "geom:gist",
    ]

    if truncate:
        db.truncate(table=table)
        prepare_table_to_update(db=db, schema="deter", name=name, columns=index_columns)

    union = ""

    # if all_data:
    #    union = """
    #        UNION
    #        SELECT gid, classname, date, areamunkm, geom, geocode, biome
    #        FROM deter.deter_history
    #    """

    sql = f"""
        INSERT INTO {table} (
            gid, classname, view_date, area_km, geom, geocode, biome
        )
        SELECT gid, classname, view_date, area_km, geom, geocode, biome
        FROM deter.deter_auth
        {union}
    """

    db.execute(sql=sql)

    logger.info("The DETER temporary table has been created.")

    # drop indexes

    optimize_table(db=db, schema="deter", name=name, columns=index_columns)
