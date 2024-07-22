"""Update the AMS database tables."""

from __future__ import annotations

import logging
import os

import click

from ams_background_tasks.database_utils import DatabaseFacade

logger = logging.getLogger(__name__)


@click.command("update-municipalities")
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--aux-db-url",
    required=False,
    type=str,
    default="",
    help="Auxiliary database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
def main(db_url: str, aux_db_url: str):
    """Update the municipalities table."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.info(db_url)
    print(db_url)
    assert db_url
    db = DatabaseFacade.from_url(db_url=db_url)

    aux_db_url = os.getenv("AMS_AUX_DB_URL") if not aux_db_url else aux_db_url
    logger.info(aux_db_url)
    print(aux_db_url)
    assert aux_db_url
    aux_db = DatabaseFacade.from_url(db_url=aux_db_url)

    update_municipalities_table(db=db, aux_db=aux_db)


def update_municipalities_table(db: DatabaseFacade, aux_db: DatabaseFacade):
    """Update the municipalities table."""
    logger.info("updating the municipalities table")

    select_query = """
        SELECT id, nome, geocodigo, anoderefer, ST_AsText(geoms)
        FROM amazonia_legal.municipalities
    """
    data = aux_db.fetchall(query=select_query)

    table = "public.municipalities"

    db.truncate(table=table)

    insert_query = f"""
        INSERT INTO {table} (id, name, geocode, year, geom)
        VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4674))
    """

    db.insert(query=insert_query, data=data)
