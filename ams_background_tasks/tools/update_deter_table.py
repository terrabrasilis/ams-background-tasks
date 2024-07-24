"""Update the AMS DETER tables."""

from __future__ import annotations

import logging
import os

import click

from ams_background_tasks.database_utils import (
    DatabaseFacade,
    get_connection_components,
)

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
    "--deter-db-url",
    required=False,
    type=str,
    default="",
    help="External DETER database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--biome",
    required=True,
    type=str,
    help="Biome.",
)
@click.option(
    "--all-data",
    required=False,
    is_flag=True,
    default=False,
    help="if True, all data of external database will be processed.",
)
def main(db_url: str, deter_db_url: str, biome: str, all_data: bool):
    """Update the deter table."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    print(db_url)
    assert db_url

    deter_db_url = os.getenv("AMS_DETER_DB_URL") if not deter_db_url else deter_db_url
    logger.debug(deter_db_url)
    print(deter_db_url)
    assert deter_db_url

    update_deter(db_url=db_url, deter_db_url=deter_db_url, biome=biome)

    all_data=all_data  # no warn


def update_deter(db_url: str, deter_db_url: str, biome: str):
    """Update the deter tables."""
    for name in ("deter", "deter_auth", "deter_history"):
        _update_deter_table(
            db_url=db_url, deter_db_url=deter_db_url, name=name, biome=biome
        )


def _update_deter_table(db_url: str, deter_db_url: str, name: str, biome: str):
    """Update the deter.{name} table."""
    logger.info("updating the deter %s table", name)
    print(f"updating the deter.{name} table")

    db = DatabaseFacade.from_url(db_url=db_url)

    # creating a sql view for the external database
    logger.info("creating the sql view public.%s", name)
    print(f"creating the sql view public.{name}")

    user, password, host, port, db_name = get_connection_components(db_url=deter_db_url)

    sql = f"""
        DROP VIEW IF EXISTS public.{name};
        CREATE VIEW public.{name} AS
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
    logger.info("inserting data from view deter.%s", name)
    print(f"inserting data from view deter.{name}")

    table = f"deter.{name}"

    db.truncate(table=table)

    sql = f"""
        INSERT INTO deter.{name}(
            gid, origin_gid, classname, quadrant, orbitpoint, date, sensor, satellite,
            areatotalkm, areamunkm, areauckm, mun, uf, uc, geom, month_year, geocode,
            ncar_ids, car_imovel, continuo, velocidade, deltad, est_fund, dominio, tp_dominio, biome
        )
        SELECT deter.gid, deter.origin_gid, deter.classname, deter.quadrant, deter.orbitpoint, deter.date,
            deter.sensor, deter.satellite, deter.areatotalkm,
            deter.areamunkm, deter.areauckm, deter.mun, deter.uf, deter.uc, deter.geom, deter.month_year,
            deter.geocode,
            0::integer as ncar_ids, ''::text as car_imovel,
            0::integer as continuo, 0::numeric as velocidade,
            0::integer as deltad, ''::character varying(254) as est_fund,
            ''::character varying(254) as dominio, ''::character varying(254) as tp_dominio,
            '{biome}'::text as biome
        FROM public.{name} as deter
    """

    db.execute(sql)
