"""Update the AMS spatial units table."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import (
    BIOMES,
    CELLS,
    is_valid_biome,
    is_valid_cell,
)

logger = get_logger(__name__, sys.stdout)


@click.command("update-spatial-units")
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
    logger.debug(db_url)
    assert db_url
    db = DatabaseFacade.from_url(db_url=db_url)

    aux_db_url = os.getenv("AMS_AUX_DB_URL") if not aux_db_url else aux_db_url
    logger.debug(aux_db_url)
    assert aux_db_url
    aux_db = DatabaseFacade.from_url(db_url=aux_db_url)

    update_spatial_units_table(db=db)

    for index, biome in enumerate(BIOMES):
        ignore_conflict = index
        truncate = not index        
        update_states_table(db=db, aux_db=aux_db, biome=biome, ignore_conflict=ignore_conflict, truncate=truncate)
        update_municipalities_table(db=db, aux_db=aux_db, biome=biome, ignore_conflict=ignore_conflict, truncate=truncate)
        for cell in CELLS:
            update_cells_table(db=db, aux_db=aux_db, biome=biome, cell=cell, ignore_conflict=ignore_conflict, truncate=truncate)


def update_spatial_units_table(db: DatabaseFacade):
    """Update the public.spatial_units table."""
    table = "public.spatial_units"

    db.truncate(table=table)

    sql = f"""
        INSERT INTO
            {table} (id, dataname, as_attribute_name, center_lat, center_lng, description)
        VALUES
            (1, 'cs_150km', 'id', -5.491382969006503, -58.467185764253415, 'Célula 150x150 km²'),
            (2, 'cs_25km', 'id', -5.510617783522636, -58.397927203480116, 'Célula 25x25 km²'),
            (3, 'states', 'nome', -6.384962796500002, -58.97111531179317, 'Estado'),
            (4, 'municipalities', 'nome', -6.384962796413522, -58.97111531172743, 'Município');
    """

    db.execute(sql)


def update_states_table(
    db: DatabaseFacade,
    aux_db: DatabaseFacade,
    biome: str,
    ignore_conflict: bool,
    truncate: bool,
):
    logger.info("updating the states tables from the auxiliary database.")

    assert is_valid_biome(biome=biome)

    table1 = "public.states"
    table2 = "public.states_biome"

    if truncate:
        db.truncate(table=table2)
        db.truncate(table=table1)

    select_query = f"""
        SELECT a.id, a.nome, a.geocodigo, a.sigla, ST_AsText(a.geom)
        FROM
            public.lml_unidade_federacao_a a,
            lm_bioma_250 b
        WHERE b.bioma='{biome}' AND ST_Intersects(a.geom, b.geom)
    """
    data = aux_db.fetchall(query=select_query)

    conflict = "ON CONFLICT (id) DO NOTHING" if ignore_conflict else ""

    insert_query = f"""
        INSERT INTO {table1} (id, name, geocode, acronym, geom)
        VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4674))
        {conflict}
    """

    db.insert(query=insert_query, data=data)

    select_query = f"""
        SELECT a.nome
        FROM
            public.lml_unidade_federacao_a a,
            lm_bioma_250 b
        WHERE b.bioma='{biome}' AND ST_Intersects(a.geom, b.geom)
    """
    data = aux_db.fetchall(query=select_query)

    insert_query = f"""
        INSERT INTO {table2} (name, biome)
        VALUES (%s, '{biome}')
    """
    db.insert(query=insert_query, data=data)


def update_municipalities_table(
    db: DatabaseFacade,
    aux_db: DatabaseFacade,
    biome: str,
    ignore_conflict: bool,
    truncate: bool,
):
    """Update the municipalities table."""
    logger.info("updating the municipalities tables from the auxiliary database.")

    assert is_valid_biome(biome=biome)

    table1 = "public.municipalities"
    table2 = "public.municipalities_biome"

    if truncate:
        db.truncate(table=table2)
        db.truncate(table=table1)

    select_query = f"""
        SELECT a.id, a.nome, a.geocodigo, a.uf_sigla, a.nm_uf, ST_AsText(a.geom)
        FROM
            public.municipio_test a,
            public.lm_bioma_250 b
        WHERE b.bioma='{biome}' AND ST_Intersects(a.geom, b.geom)
    """
    data = aux_db.fetchall(query=select_query)

    conflict = "ON CONFLICT (id) DO NOTHING" if ignore_conflict else ""

    insert_query = f"""
        INSERT INTO {table1} (id, name, geocode, state_acr, state_name, geom)
        VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4674))
        {conflict}
    """

    db.insert(query=insert_query, data=data)

    select_query = f"""
        SELECT a.geocodigo
        FROM
            public.municipio_test a,
            public.lm_bioma_250 b
        WHERE b.bioma='{biome}' AND ST_Intersects(a.geom, b.geom)
    """
    data = aux_db.fetchall(query=select_query)

    insert_query = f"""
        INSERT INTO {table2} (geocode, biome)
        VALUES (%s, '{biome}')
    """
    db.insert(query=insert_query, data=data)


def update_cells_table(
    db: DatabaseFacade,
    aux_db: DatabaseFacade,
    biome: str,
    cell: str,
    ignore_conflict: bool,
    truncate: bool,
):
    """Update the cells tables (25 and 150 km)."""
    logger.info("updating the cell tables from the auxiliary database.")

    assert is_valid_biome(biome=biome)
    assert is_valid_cell(cell=cell)

    table1 = f"cs_{cell}"
    table2 = f"cs_{cell}_biome"

    if truncate:
        db.truncate(table=table2)
        db.truncate(table=table1)

    select_query = f"""
        SELECT id, col, row, area, ST_AsText(geometry) FROM public."{table1}";
    """
    data = aux_db.fetchall(query=select_query)

    conflict = "ON CONFLICT (id) DO NOTHING" if ignore_conflict else ""

    insert_query = f"""
        INSERT INTO {table1} (id, col, row, area, geometry)
        VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4674))
        {conflict}
    """
    db.insert(query=insert_query, data=data)

    select_query = f'SELECT id, biome FROM public."{table2}";'
    data = aux_db.fetchall(query=select_query)

    insert_query = f"""
        INSERT INTO {table2} (id, biome)
        VALUES (%s, %s)
    """
    db.insert(query=insert_query, data=data)
