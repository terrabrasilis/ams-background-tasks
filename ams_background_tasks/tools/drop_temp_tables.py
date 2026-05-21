"""Cross the indicators with the land use image and group them by spatial units."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger

logger = get_logger(__name__, sys.stdout)


@click.command("drop-temp-tables")
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
def main(db_url: str):
    """Drop the temporary tables."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.create(db_url=db_url)

    tables = db.fetchall(
        query="""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
            AND table_name LIKE 'tmp_%';
        """
    )

    tables = [f"public.{_[0]}" for _ in tables]

    for table in tables:
        db.drop_table(table=table)

    db.commit()
