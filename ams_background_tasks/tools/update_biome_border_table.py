"""Update the AMS biome_border table."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import BIOMES

logger = get_logger(__name__, sys.stdout)


@click.command("update-biome-border")
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
    """Update the biome border table."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.info(db_url)
    assert db_url
    db = DatabaseFacade.from_url(db_url=db_url)

    aux_db_url = os.getenv("AMS_AUX_DB_URL") if not aux_db_url else aux_db_url
    logger.info(aux_db_url)
    assert aux_db_url
    aux_db = DatabaseFacade.from_url(db_url=aux_db_url)

    update_biome_border_table(db=db, aux_db=aux_db)


def update_biome_border_table(db: DatabaseFacade, aux_db: DatabaseFacade):
    """Update the biome_border table."""
    logger.info("updating the biome_border table")

    table = "public.biome_border"

    db.truncate(table=table)

    select_query = f"""
        SELECT id, bioma, cd_bioma, area_km, ST_AsText(geom)
        FROM public.lm_bioma_250
        WHERE bioma IN {tuple(BIOMES)}
    """
    data = aux_db.fetchall(query=select_query)

    insert_query = f"""
        INSERT INTO {table} (id, biome, cd_biome, area_km, geom)
        VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4674))
        
    """
    db.insert(query=insert_query, data=data)
