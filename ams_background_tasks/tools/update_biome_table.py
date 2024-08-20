"""Update the AMS biome_border table."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import BIOMES, read_biomes

logger = get_logger(__name__, sys.stdout)


@click.command("update-biome")
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
@click.option(
    "--biome", type=click.Choice(BIOMES), required=True, help="Biome.", multiple=True
)
def main(db_url: str, aux_db_url: str, biome: tuple):
    """Update the biome border table."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.info(db_url)
    assert db_url
    db = DatabaseFacade.from_url(db_url=db_url)

    aux_db_url = os.getenv("AMS_AUX_DB_URL") if not aux_db_url else aux_db_url
    logger.info(aux_db_url)
    assert aux_db_url
    aux_db = DatabaseFacade.from_url(db_url=aux_db_url)

    update_biome_border_table(db=db, aux_db=aux_db, biome_list=list(biome))


def update_biome_border_table(
    db: DatabaseFacade, aux_db: DatabaseFacade, biome_list: list
):
    """Update the biome_border table."""
    logger.info("updating the biome tables")

    cur_biomes = read_biomes(db=db)

    biome_list = [_ for _ in biome_list if _ not in cur_biomes]

    if not len(biome_list):
        return

    table = "public.biome"

    values = [f"('{_}')" for _ in biome_list]

    insert_query = f"""
        INSERT INTO {table} (biome)
        VALUES {','.join(values)};
    """

    db.execute(sql=insert_query)

    table = "public.biome_border"

    select_query = f"""
        SELECT bioma, area_km, ST_AsText(geom)
        FROM public.lm_bioma_250
        WHERE bioma IN {tuple(biome_list)}
    """
    data = aux_db.fetchall(query=select_query)

    insert_query = f"""
        INSERT INTO {table} (biome, area_km, geom)
        VALUES (%s, %s, ST_GeomFromText(%s, 4674))
        
    """
    db.insert(query=insert_query, data=data)
