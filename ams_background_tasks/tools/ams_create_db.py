"""Create AMS database."""

from __future__ import annotations

import logging

import click

from ams_background_tasks.database_utils import DatabaseFacade

logger = logging.getLogger(__name__)


@click.command("create-db")
@click.argument(
    "db_url",
    required=True,
    type=str,
)
@click.option(
    "--force-recreate",
    required=False,
    is_flag=True,
    default=False,
    help="Force to recreate the AMS database.",
)
def main(db_url: str, force_recreate: bool):
    """Create the AMS database.

    DB_URL: AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).
    """
    db = DatabaseFacade.from_url(db_url=db_url)
    # municipalities
    create_municipalities_table(db=db, schema="public", force_recreate=force_recreate)


def create_municipalities_table(
    db: DatabaseFacade, schema: str, force_recreate: bool = False
):
    """Create the municipalities table."""
    columns = [
        "id integer",
        "name character varying(150) COLLATE pg_catalog.default",
        "geocode character varying(80) COLLATE pg_catalog.default",
        "year integer",
        "geometry geometry(MultiPolygon, 4674)",
    ]

    db.create_table(
        schema=schema,
        name="municipalities",
        columns=columns,
        force_recreate=force_recreate,
    )
