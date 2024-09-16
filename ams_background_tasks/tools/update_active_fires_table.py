"""Update the AMS active fires tables."""

from __future__ import annotations

import logging
import os

import click

from ams_background_tasks.database_utils import (
    DatabaseFacade,
    get_connection_components,
)
from ams_background_tasks.tools.common import BIOMES

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
    "--all-data",
    required=False,
    is_flag=True,
    default=False,
    help="if True, all data of external database will be processed.",
)
@click.option(
    "--biome", type=click.Choice(BIOMES), required=True, help="Biome.", multiple=True
)
def main(db_url: str, af_db_url: str, all_data: bool, biome: tuple):
    """Update the active fires table."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    af_db_url = os.getenv("AMS_AF_DB_URL") if not af_db_url else af_db_url
    logger.debug(af_db_url)
    assert af_db_url

    logger.debug(all_data)

    update_active_fires_table(
        db_url=db_url, af_db_url=af_db_url, all_data=all_data, biome_list=list(biome)
    )


def update_active_fires_table(
    db_url: str, af_db_url: str, all_data: bool, biome_list: list
):
    logger.info("updating the active_fires table")

    # creating a sql view for the external database
    logger.info("creating the sql view")
    print("creating the sql view")
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

    db = DatabaseFacade.from_url(db_url=db_url)
    db.execute(sql)

    # inserting data
    logger.info("inserting data from view")
    print("inserting data from view")

    table = "fires.active_fires"

    by_date = "a.view_date > '2016-01-01'::date "

    if all_data:
        db.truncate(table=table)
    else:
        by_date = f"a.view_date > (SELECT MAX(view_date) FROM {table})"

    sql = f"""
        INSERT INTO {table} (
            id, uuid, view_date, satelite, estado, municipio, biome, geom
        )
        SELECT a.id, a.uuid, a.view_date, a.satelite, a.estado, a.municipio, a.biome, a.geom
        FROM public.raw_active_fires a
        WHERE {by_date} AND a.biome IN ({",".join(repr(_) for _ in biome_list)})
    """

    db.execute(sql)

    # intersecting with municipalities
    logger.info("intersecting with municipalities")
    print("intersecting with municipalities")
    municipalities_table = "public.municipalities"

    sql = f"""
        UPDATE {table}
        SET geocode = (
            SELECT mun.geocode
            FROM {municipalities_table} mun
            WHERE ST_Within({table}.geom, mun.geometry)
            LIMIT 1
        );
    """

    db.execute(sql)
