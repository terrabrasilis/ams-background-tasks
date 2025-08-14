"""Update the AMS spatial units table."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.municipalities_groups import MunicipalitiesGroupHandler
from ams_background_tasks.tools.common import (
    BIOMES,
    CELLS,
    get_biome_acronym,
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
@click.option(
    "--biome", type=click.Choice(BIOMES), required=True, help="Biome.", multiple=True
)
def main(db_url: str, aux_db_url: str, biome: tuple):
    """Update the spatial units tables."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url
    db = DatabaseFacade.from_url(db_url=db_url)

    check_count_rows(db=db)

    aux_db_url = os.getenv("AMS_AUX_DB_URL") if not aux_db_url else aux_db_url
    logger.debug(aux_db_url)
    assert aux_db_url
    aux_db = DatabaseFacade.from_url(db_url=aux_db_url)

    for index, _biome in enumerate(biome):
        assert is_valid_biome(biome=_biome)
        ignore_conflict = index
        # truncate = not index
        update_states_table(
            db=db,
            aux_db=aux_db,
            biome=_biome,
            ignore_conflict=ignore_conflict,
            # truncate=truncate,
        )
        update_municipalities_table(
            db=db,
            aux_db=aux_db,
            biome=_biome,
            ignore_conflict=ignore_conflict,
            # truncate=truncate,
        )
        for cell in CELLS:
            assert aux_db.table_exist(
                schema="public", table=f"cs_{get_biome_acronym(biome=_biome)}_{cell}"
            )

            update_cells_table(
                db=db,
                aux_db=aux_db,
                cell=cell,
                ignore_conflict=ignore_conflict,
                biome=_biome,
                # truncate=truncate,
            )

    update_municipalities_groups(db_url=db_url, aux_db_url=aux_db_url)


def check_count_rows(db: DatabaseFacade):
    assert not db.count_rows(table="public.states")
    assert not db.count_rows(table="public.states_biome")
    assert not db.count_rows(table="public.municipalities")
    assert not db.count_rows(table="public.municipalities_biome")

    for cell in CELLS:
        assert db.table_exist(schema="public", table=f"cs_{cell}")
        assert not db.count_rows(table=f"cs_{cell}")
        assert not db.count_rows(table=f"cs_{cell}_biome")


def update_states_table(
    db: DatabaseFacade,
    aux_db: DatabaseFacade,
    biome: str,
    ignore_conflict: bool,
    # truncate: bool,
):
    logger.info("updating the states tables from the auxiliary database.")

    table1 = "public.states"
    table2 = "public.states_biome"

    # if truncate:
    # db.truncate(table=table2)
    # db.truncate(table=table1, cascade=True)

    select_query = f"""
        SELECT a.id, a.nome, a.geocodigo, a.sigla, ST_AsText(a.geom), ST_AsText(a.geom)
        FROM
            public.lml_unidade_federacao_a a,
            lm_bioma_250 b
        WHERE b.bioma='{biome}' AND ST_Intersects(a.geom, b.geom)
    """
    data = aux_db.fetchall(query=select_query)

    conflict = "ON CONFLICT (suid) DO NOTHING" if ignore_conflict else ""

    insert_query = f"""
        INSERT INTO {table1} (suid, name, geocode, acronym, geometry, area)
        VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4674),
            ST_Area(
                ST_GeomFromText(%s, 4674)::geography
            ) / 1000000.
        )
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
    # truncate: bool,
):
    """Update the municipalities table."""
    logger.info("updating the municipalities tables from the auxiliary database.")

    table1 = "public.municipalities"
    table2 = "public.municipalities_biome"

    # if truncate:
    #    db.truncate(table=table2)
    #    db.truncate(table=table1, cascade=True)

    select_query = f"""
        SELECT a.id, a.nome, a.geocodigo, a.uf_sigla, a.nm_uf, ST_AsText(a.geom), ST_AsText(a.geom)
        FROM
            public.municipio_test a,
            public.lm_bioma_250 b
        WHERE b.bioma='{biome}' AND ST_Intersects(a.geom, b.geom)
    """
    data = aux_db.fetchall(query=select_query)

    conflict = "ON CONFLICT (suid) DO NOTHING" if ignore_conflict else ""

    insert_query = f"""
        INSERT INTO {table1} (suid, name, geocode, state_acr, state_name, geometry, area)
        VALUES (%s, %s, %s, %s, %s, ST_GeomFromText(%s, 4674),
            ST_Area(
                ST_GeomFromText(%s, 4674)::geography
            ) / 1000000.
        )
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


def update_municipalities_groups(db_url: str, aux_db_url: str):
    """Update the municipalities groups."""
    db = DatabaseFacade.from_url(db_url=db_url)

    mgh = MunicipalitiesGroupHandler(db_url=aux_db_url)

    valid_geocodes = [
        _[0] for _ in db.fetchall("SELECT geocode from public.municipalities")
    ]

    for gkey, group in mgh.list_groups():
        table = "public.municipalities_group"
        if db.count_rows(table=f"{table}", conditions=f"name='{group}'"):
            continue

        sql = f"""
            INSERT INTO {table} (name, type) VALUES ('{group}', '{gkey}');
        """

        db.execute(sql)

        # getting group_id
        sql = f"""
            SELECT id from {table} WHERE name='{group}';
        """

        group_id = db.fetchone(query=sql)
        assert group_id

        logger.debug(group_id)

        geocodes = mgh.get_geocodes(gkey=gkey, group=group)

        invalid_geocodes = []
        for geocode in geocodes:
            if not geocode in valid_geocodes:
                invalid_geocodes.append(geocode)

        logger.debug("invalid geocodes %s", invalid_geocodes)

        if len(invalid_geocodes) > 0:
            logger.warning(
                "there are invalid geocodes for the group %s: %s",
                group,
                invalid_geocodes,
            )

        values = ",".join([f"({group_id},'{_}')" for _ in geocodes])

        table = "public.municipalities_group_members"
        sql = f"""
            INSERT INTO {table} (group_id, geocode)
            VALUES {values};
        """

        db.execute(sql)


def update_cells_table(
    db: DatabaseFacade,
    aux_db: DatabaseFacade,
    cell: str,
    ignore_conflict: bool,
    biome: str,
    # truncate: bool,
):
    """Update the cells tables (25 and 150 km)."""
    logger.info("updating the cell tables from the auxiliary database.")

    table1 = f"cs_{cell}"
    table2 = f"cs_{cell}_biome"

    assert is_valid_cell(cell=cell)

    # if truncate:
    #    db.truncate(table=table2)
    #    db.truncate(table=table1, cascade=True)

    select_query = f"""
        SELECT id, col, row, area, ST_AsText(geometry)
        FROM public.cs_{get_biome_acronym(biome=biome)}_{cell};
    """
    data = aux_db.fetchall(query=select_query)

    _ = ignore_conflict  # no warn
    conflict = "ON CONFLICT (id) DO NOTHING"  # if ignore_conflict else ""

    insert_query = f"""
        INSERT INTO {table1} (id, col, row, area, geometry)
        VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4674))
        {conflict}
    """
    db.insert(query=insert_query, data=data)

    select_query = f"SELECT id, biome FROM public.cs_{get_biome_acronym(biome=biome)}_{cell}_biome;"
    data = aux_db.fetchall(query=select_query)

    insert_query = f"""
        INSERT INTO {table2} (id, biome)
        VALUES (%s, %s)
    """
    db.insert(query=insert_query, data=data)
