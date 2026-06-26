"""Copy the PRODES data from land use tables to ams."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import (
    LAND_USE_TYPES,
    PRODES_CLASSNAMES,
    optimize_land_use_table,
    prepare_to_update_land_use_table,
    read_spatial_units,
)
from ams_background_tasks.tools.prodes_utils import get_land_use_table_name

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
    "--land-use-type",
    required=True,
    type=click.Choice(LAND_USE_TYPES),
    help="Land use categories type.",
)
def main(db_url: str, land_use_type: str):
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    classnames = ",".join([f"'{_}'" for _ in PRODES_CLASSNAMES])

    for spatial_unit in read_spatial_units(db=db):
        prodes_land_use_table = get_land_use_table_name(
            spatial_unit=spatial_unit, land_use_type=land_use_type, schema="prodes"
        )

        ckey = {
            "cs_5km": "id",
            "cs_25km": "id",
            "cs_150km": "id",
            "municipalities": "geocode",
            "states": "name",
        }[spatial_unit]

        sql = f"""
           UPDATE {prodes_land_use_table} AS lu
           SET suid = su.suid
           FROM public.{spatial_unit} AS su
           WHERE lu.su_name = su.{ckey};
        """
        db.execute(sql=sql, log=True)

        ams_land_use_table = get_land_use_table_name(
            spatial_unit=spatial_unit, land_use_type=land_use_type
        )

        logger.info("copying from %s to %s.", prodes_land_use_table, ams_land_use_table)

        prepare_to_update_land_use_table(db=db, table=ams_land_use_table)

        sql = f"DELETE FROM {ams_land_use_table} WHERE classname IN ({classnames});"
        db.execute(sql=sql, log=True)

        db.copy_table(
            src=prodes_land_use_table,
            dst=ams_land_use_table,
            cols_to_ignore=["id", "risk", "score", "units", "name"],
            with_commit=False,
        )

        optimize_land_use_table(db=db, table=ams_land_use_table)

    db.commit()
