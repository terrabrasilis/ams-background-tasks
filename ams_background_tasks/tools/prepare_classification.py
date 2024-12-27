"""Prepare the database to perform the classification."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import (
    ACTIVE_FIRES_INDICATOR,
    DETER_INDICATOR,
    INDICATORS,
    RISK_INDICATOR,
    create_land_structure_table,
    reset_land_use_tables,
)

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
    multiple=True,
    default=INDICATORS,
    help=f"Indicator ({', '.join(INDICATORS)})",
)
def main(
    db_url: str,
    indicator: str,
):
    """Prepare the database to perform the classification."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    reset_land_use_tables(db_url=db_url, is_temp=True, force_recreate=True)

    if ACTIVE_FIRES_INDICATOR in indicator:
        create_land_structure_table(
            db_url=db_url, table="tmp_fires_land_structure", force_recreate=True
        )
        create_land_structure_table(
            db_url=db_url, table="fires_land_structure", force_recreate=True
        )

    if DETER_INDICATOR in indicator:
        create_land_structure_table(
            db_url=db_url, table="tmp_deter_land_structure", force_recreate=True
        )
        create_land_structure_table(
            db_url=db_url, table="deter_land_structure", force_recreate=True
        )

    if RISK_INDICATOR in indicator:
        create_land_structure_table(
            db_url=db_url, table="tmp_risk_land_structure", force_recreate=True
        )
        create_land_structure_table(
            db_url=db_url, table="risk_land_structure", force_recreate=True
        )
