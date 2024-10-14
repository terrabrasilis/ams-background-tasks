"""Import the municipalities group into the database."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger

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
    "--municipalities-group-file",
    required=True,
    type=click.Path(exists=True, resolve_path=True, file_okay=True),
    help="Land use image path.",
)
def main(db_url: str, municipalities_group_file: str):
    """Import the municipalities group into the database."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    logger.debug(municipalities_group_file)

    assert Path(municipalities_group_file).exists()

    groups: dict[str, list[str]] = {}
    with open(str(municipalities_group_file), "r", encoding="utf-8") as src:
        groups = json.load(src)

    db = DatabaseFacade.from_url(db_url=db_url)

    valid_geocodes = [
        _[0] for _ in db.fetchall("SELECT geocode from public.municipalities")
    ]

    for _ in groups:
        name = groups[_]["name"]
        geocodes = groups[_]["geocodes"]

        for geocode in geocodes:
            assert geocode in valid_geocodes, f"invalid geocode '{geocode}'"

        table = "public.municipalities_group"

        sql = f"""
            INSERT INTO {table} (name) VALUES ('{name}');
        """

        db.execute(sql)

        query = f"SELECT id FROM {table} WHERE name = '{name}'"

        group_id = db.fetchone(query=query)

        table = "public.municipalities_group_members"

        values = ",".join([f"({group_id},'{_}')" for _ in geocodes])

        sql = f"""
            INSERT INTO {table} (group_id, geocode)
            VALUES {values};
        """

        db.execute(sql)
