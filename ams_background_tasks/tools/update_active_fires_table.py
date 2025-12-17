"""Update the AMS active fires tables."""

from __future__ import annotations

import logging
import os
from time import sleep

import click

from ams_background_tasks.database_utils import (
    DatabaseFacade,
    get_connection_components,
)
from ams_background_tasks.tools.common import (
    ACTIVE_FIRES_INDICATOR,
    BIOMES,
    analyze_table,
    create_processing,
    finalize_processing,
    optimize_table,
    prepare_table_to_update,
)

logger = logging.getLogger(__name__)


@click.command("update-active-fires")
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--af-db-url",
    required=False,
    type=str,
    default="",
    help="External active fires database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--fc-db-url",
    required=False,
    type=str,
    default="",
    help="External fires class database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--all-data",
    required=False,
    is_flag=True,
    default=False,
    help="if True, all data of external database will be processed.",
)
@click.option(
    "--biome", type=click.Choice(BIOMES), required=True, help="Biome.", multiple=True
)
@click.option(
    "--limit",
    required=False,
    type=int,
    default=0,
    help="Restrict the number of rows to update (test purpose).",
)
def main(
    db_url: str,
    af_db_url: str,
    fc_db_url: str,
    all_data: bool,
    biome: tuple,
    limit: int,
):
    """Update the active fires table."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    af_db_url = os.getenv("AMS_AF_DB_URL", "") if not af_db_url else af_db_url
    logger.debug(af_db_url)
    assert af_db_url

    fc_db_url = os.getenv("AMS_FC_DB_URL", "") if not fc_db_url else fc_db_url
    logger.debug(fc_db_url)
    assert fc_db_url

    logger.debug(all_data)

    db = DatabaseFacade.create(db_url=db_url)

    create_processing(
        db=db, indicator=ACTIVE_FIRES_INDICATOR, process="update", status="processing"
    )

    update_active_fires_table(
        db=db,
        af_db_url=af_db_url,
        fc_db_url=fc_db_url,
        all_data=all_data,
        biome_list=list(biome),
        limit=limit,
    )

    finalize_processing(
        db=db, indicator=ACTIVE_FIRES_INDICATOR, process="update", status="completed"
    )

    db.commit()

    analyze_table(db=db, schema="fires", name="active_fires")


def update_active_fires_table(
    db: DatabaseFacade,
    af_db_url: str,
    fc_db_url: str,
    all_data: bool,
    biome_list: list,
    limit: int,
):
    logger.info("updating the active_fires table")

    assert all_data

    index_columns = [
        "view_date:btree",
        "uuid:btree",
        "biome:btree",
        "geocode:btree",
        "prodes_class:btree",
        "biome,view_date:btree",
        "view_date,id,biome:btree",
        "geom:gist",
    ]

    prepare_table_to_update(
        db=db, schema="fires", name="active_fires", columns=index_columns
    )

    # creating a sql view for the external database
    logger.info("creating the sql view")
    user, password, host, port, db_name = get_connection_components(db_url=af_db_url)

    sql = f"""
        CREATE OR REPLACE VIEW public.raw_active_fires AS
        SELECT
            remote_data.id,
            remote_data.uuid,
            remote_data.view_date,
            remote_data.satelite,
            remote_data.estado,
            remote_data.municipio,
            remote_data.biome,
            remote_data.geom
        FROM
            dblink('hostaddr={host} port={port} dbname={db_name} user={user} password={password}'::text,
                   'SELECT fid as id, uuid, data as view_date, satelite, estado, municipio, bioma, geom FROM public.focos_aqua_referencia'::text)
        AS remote_data(id integer, uuid character varying(254), view_date date, satelite character varying(254), estado character varying(254), municipio character varying(254), biome character varying(254), geom geometry(Point,4674));
    """

    db.execute(sql)

    # inserting data
    logger.info("inserting data from view")

    table = "fires.active_fires"

    by_date = "a.view_date > '2018-08-01'::date "

    db.truncate(table=table)

    limit_sql = f"LIMIT {limit}" if limit > 0 else ""

    sql = f"""
        INSERT INTO {table} (
            uuid, view_date, prodes_class, satelite, estado, municipio, biome, geom
        )
        SELECT a.uuid, a.view_date, 'Nao Categorizado', a.satelite, a.estado, a.municipio, a.biome, a.geom
        FROM public.raw_active_fires a
        WHERE {by_date} AND a.biome IN ({",".join(repr(_) for _ in biome_list)})
        {limit_sql};
    """

    db.execute(sql)

    # creating the essentials indices to update some columns
    tmp_index_columns = [
        "biome:btree",
        "uuid:btree",
        "geom:gist",
    ]
    db.create_indexes(
        schema="fires",
        name="active_fires",
        columns=tmp_index_columns,
        force_recreate=False,
    )

    # intersecting with municipalities
    logger.info("intersecting with municipalities")

    sql = f"""
        UPDATE {table} AS ac
        SET geocode = a.geocode	
        FROM (
            SELECT 
                ac2.id, ac2.biome, mun.geocode
            FROM 
                {table} AS ac2
            JOIN 
                public.municipalities_biome mub
                ON ac2.biome=mub.biome
            JOIN 
                public.municipalities mun
                ON mun.geocode=mub.geocode
                AND ac2.geom && mun.geometry
                AND ST_Within(ac2.geom, mun.geometry)
        ) AS a
        WHERE 
            ac.id = a.id
            AND ac.biome=a.biome;
    """

    db.execute(sql)

    # sql view for retrieve fire classes
    logger.info("creating a sql view to qualify fires from prodes class")
    user, password, host, port, db_name = get_connection_components(db_url=fc_db_url)

    view = "public.fires_class"

    sql = f"""
        CREATE OR REPLACE VIEW {view}
        AS
        SELECT remote_data.id,
            remote_data.uuid,
            remote_data.classe_prodes
        FROM dblink(
                'hostaddr={host} port={port} dbname={db_name} user={user} password={password}'::text,
                'SELECT id, uuid, classe_prodes FROM public.focos_aqua_referencia'::text
            )
            AS remote_data(
                id integer,
                uuid varchar,
                classe_prodes varchar
            );
    """

    db.execute(sql)

    stop = False
    tries = 0

    while not stop:
        sql = f"""
            UPDATE {table} AS af
            SET prodes_class = fc.classe_prodes
            FROM {view} fc
            WHERE af.uuid=fc.uuid;
        """

        db.execute(sql)

        count = db.count_rows(table=table, conditions="prodes_class='0'")

        logger.info(count)

        stop = count == 0 or tries >= 30
        tries += 1

        if not stop:
            sleep(300)

    logger.debug(tries)

    logger.debug(
        db.count_rows(table=table, conditions="prodes_class='Nao Categorizado'")
    )

    optimize_table(db=db, schema="fires", name="active_fires", columns=index_columns)
