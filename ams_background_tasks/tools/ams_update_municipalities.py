"""Update the AMS municipalities table."""

from __future__ import annotations

import logging

import click

from ams_background_tasks.database_utils import DatabaseFacade

logger = logging.getLogger(__name__)


@click.command("update-municipalities")
@click.argument(
    "db_url",
    required=True,
    type=str,
)
@click.argument(
    "aux_db_url",
    required=True,
    type=str,
)
def main(db_url: str, aux_db_url: str):
    """Update the municipalities table.

    DB_URL: AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).
    AUX_DB_URL: Auxiliary database url (postgresql://<username>:<password>@<host>:<port>/<database>).
    """
    db = DatabaseFacade.from_url(db_url=db_url)

    # municipalities
    aux_db = DatabaseFacade.from_url(db_url=aux_db_url)
    update_municipalities_table(schema="public", db=db, aux_db=aux_db)


def update_municipalities_table(
    schema: str, db: DatabaseFacade, aux_db: DatabaseFacade
):
    """Update the municipalities table."""
    select_query = "SELECT id, nome, geocodigo, anoderefer, ST_AsText(geoms) from amazonia_legal.municipalities"
    data = aux_db.fetchall(query=select_query)

    table = f"{schema}.municipalities"

    db.truncate(table=table)

    insert_query = f"""
        INSERT INTO {table} (id, name, geocode, year, geometry)
        VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4674))
    """
    db.insert(query=insert_query, data=data)
