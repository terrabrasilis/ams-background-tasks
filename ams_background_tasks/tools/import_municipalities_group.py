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
@click.argument("municipalities_group_file", type=click.Path(exists=True, file_okay=True,))
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
def main(db_url: str, municipalities_group_file: str):
    """Import the municipalities group into the database."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    logger.debug(municipalities_group_file)

    assert Path(municipalities_group_file).exists()

    groups: dict[str, list[str]] = {}
    with open(str(municipalities_group_file), "r", encoding="utf-8") as src:
        groups = json.load(src)

    db = DatabaseFacade.create(db_url=db_url)

    valid_geocodes = [
        _[0] for _ in db.fetchall("SELECT geocode from public.municipalities")
    ]

    for name in list(groups.keys()):
        geocodes = groups[name]

        for geocode in geocodes:
            assert geocode in valid_geocodes, f"invalid geocode '{geocode}'"

        gtype = "user-defined"

        with db.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO public.municipalities_group (name, type)
                VALUES (%s, %s)
                RETURNING id;
                """,
                (name, gtype),
            )
            group_id = cursor.fetchone()[0]

            cursor.executemany(
                """
                INSERT INTO public.municipalities_group_members (group_id, geocode)
                VALUES (%s, %s);
                """,
                [(group_id, geocode) for geocode in geocodes],
            )

    db.commit()
