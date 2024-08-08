"""Update the AMS DETER tables."""

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


@click.command("update-deter")
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--deter-b-db-url",
    required=False,
    type=str,
    default="",
    help="External DETER-B database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--biome",
    required=True,
    type=click.Choice(BIOMES),
    help="Biome.",
)
@click.option(
    "--all-data",
    required=False,
    is_flag=True,
    default=False,
    help="if True, all data of external database will be processed.",
)
@click.option(
    "--truncate",
    required=False,
    is_flag=True,
    default=False,
    help="If True, truncate the table before update.",
)
def main(db_url: str, deter_b_db_url: str, biome: str, all_data: bool, truncate: bool):
    """Update the DETER tables from official databases using SQL views."""
    assert not all_data  # need to be fixed

    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    deter_b_db_url = (
        os.getenv("AMS_DETER_B_DB_URL") if not deter_b_db_url else deter_b_db_url
    )
    logger.debug(deter_b_db_url)
    assert deter_b_db_url

    update_deter(
        db_url=db_url,
        deter_b_db_url=deter_b_db_url,
        biome=biome,
        all_data=all_data,
        truncate=truncate,
    )

    update_publish_date(
        db_url=db_url, deter_db_url=deter_b_db_url, biome=biome, truncate=truncate
    )

    create_tmp_table(db_url=db_url, all_data=all_data, truncate=truncate, biome=biome)


def update_deter(
    db_url: str, deter_b_db_url: str, biome: str, all_data: bool, truncate: bool
):
    """Update the DETER tables (deter, deter_auth, deter_history)."""
    tables = (
        ("deter", "deter_auth", "deter_history")
        if all_data
        else ("deter", "deter_auth")
    )

    for name in tables:
        _update_deter_table(
            db_url=db_url,
            deter_b_db_url=deter_b_db_url,
            name=name,
            biome=biome,
            truncate=truncate,
        )


def _update_deter_table(
    db_url: str, deter_b_db_url: str, name: str, biome: str, truncate: bool
):
    """Update the deter.{name} table."""
    logger.info("updating the deter.%s table", name)

    db = DatabaseFacade.from_url(db_url=db_url)

    # creating sql view for the external database
    view = f"public.{biome.lower()[:3]}_{name}"
    logger.info("creating the sql view %s.", view)

    user, password, host, port, db_name = get_connection_components(
        db_url=deter_b_db_url
    )

    sql = f"""
        CREATE OR REPLACE VIEW {view} AS
        SELECT
            remote_data.gid,
            remote_data.origin_gid,
            remote_data.classname,
            remote_data.quadrant,
            remote_data.orbitpoint,
            remote_data.date,
            remote_data.sensor,
            remote_data.satellite,
            remote_data.areatotalkm,
            remote_data.areamunkm,
            remote_data.areauckm,
            remote_data.mun,
            remote_data.uf,
            remote_data.uc,
            remote_data.geom,
            remote_data.month_year,
            remote_data.geocode
        FROM
            dblink('hostaddr={host} port={port} dbname={db_name} user={user} password={password}'::text,
                   'SELECT gid, origin_gid, classname, quadrant, orbitpoint, date, sensor, satellite, areatotalkm, areamunkm, areauckm, mun, uf, uc, geom, month_year, geocod FROM public.deter_ams'::text)
        AS remote_data(gid text, origin_gid integer, classname character varying(254), quadrant character varying(5), orbitpoint character varying(10), date date, sensor character varying(10), satellite character varying(13), areatotalkm double precision, areamunkm double precision, areauckm double precision, mun character varying(254), uf character varying(2), uc character varying(254), geom geometry(MultiPolygon,4674), month_year character varying(10), geocode character varying(80));
    """

    db.execute(sql)

    # inserting data
    logger.info("inserting data from view %s", view)

    table = f"deter.{name}"

    if truncate:
        db.truncate(table=table)

    sql = f"""
        INSERT INTO deter.{name}(
            gid, origin_gid, classname, quadrant, orbitpoint, date, sensor, satellite,
            areatotalkm, areamunkm, areauckm, mun, uf, uc, geom, month_year, geocode, biome            
        )
        SELECT deter.gid, deter.origin_gid, deter.classname, deter.quadrant, deter.orbitpoint, deter.date,
            deter.sensor, deter.satellite, deter.areatotalkm,
            deter.areamunkm, deter.areauckm, deter.mun, deter.uf, deter.uc, deter.geom, deter.month_year,
            deter.geocode, '{biome}'::text as biome
        FROM {view} as deter, public.biome_border as border
        WHERE ST_Within(deter.geom, border.geom) AND border.biome='{biome}';
    """

    db.execute(sql)


def update_publish_date(db_url: str, deter_db_url: str, biome: str, truncate: bool):
    """Update the deter.deter_publish_date."""
    db = DatabaseFacade.from_url(db_url=db_url)
    user, password, host, port, db_name = get_connection_components(db_url=deter_db_url)
    
    name = "deter_public_date"

    # creating sql view for the external database
    view = f"public.{biome.lower()[:3]}_{name}"
    logger.info("creating the sql view %s.", view)

    sql = f"""
        CREATE OR REPLACE VIEW {view} AS
        SELECT
            remote_data.date
        FROM
            dblink('hostaddr={host} port={port} dbname={db_name} user={user} password={password}'::text,
            'SELECT date FROM public.deter_publish_date'::text)
        AS remote_data(date date);
    """

    db.execute(sql)

    # inserting data
    logger.info("inserting data from view deter.deter_publish_date")

    table = "deter.deter_publish_date"

    if truncate:
        db.truncate(table=table)

    sql = f"""
        INSERT INTO {table} (
            date, biome
        )
        SELECT
            deter.date,
            '{biome}'::text as biome
        FROM {view} as deter
    """

    db.execute(sql)


def create_tmp_table(db_url: str, all_data: bool, truncate: bool, biome: str):
    """Create a temporary table with DETER alerts to ensure gist index creation."""
    db = DatabaseFacade.from_url(db_url=db_url)

    name = "tmp_data"
    table = f"deter.{name}"

    if truncate:
        db.truncate(table=table)

    union = ""
    if all_data:
        union = f"""
            UNION
            SELECT gid, classname, date, areamunkm, geom, geocode, biome
            FROM deter.deter_history
            WHERE biome = '{biome}'
        """

    sql = f"""
        INSERT INTO {table} (
            gid, classname, date, areamunkm, geom, geocode, biome
        )
        SELECT gid, classname, date, areamunkm, geom, geocode, biome
        FROM deter.deter_auth
        WHERE biome = '{biome}'
        {union}            
    """

    db.execute(sql=sql)

    logger.info("The DETER temporary table has been created.")
