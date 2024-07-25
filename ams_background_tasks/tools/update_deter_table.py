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
    "--deter-b-db-url",
    required=False,
    type=str,
    default="",
    help="External DETER-B database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--deter-r-db-url",
    required=False,
    type=str,
    default="",
    help="External DETER-R database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
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
def main(
    db_url: str, deter_b_db_url: str, deter_r_db_url: str, biome: str, all_data: bool
):
    """Update the deter table."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    logger.debug(db_url)
    print(db_url)
    assert db_url

    deter_b_db_url = (
        os.getenv("AMS_DETER_B_DB_URL") if not deter_b_db_url else deter_b_db_url
    )
    logger.debug(deter_b_db_url)
    print(deter_b_db_url)
    assert deter_b_db_url

    deter_r_db_url = (
        os.getenv("AMS_DETER_R_DB_URL") if not deter_r_db_url else deter_r_db_url
    )
    logger.debug(deter_r_db_url)
    print(deter_r_db_url)
    assert deter_r_db_url

    update_deter(
        db_url=db_url,
        deter_b_db_url=deter_b_db_url,
        deter_r_db_url=deter_r_db_url,
        biome=biome,
        all_data=all_data,
    )

    update_publish_date(db_url=db_url, deter_db_url=deter_b_db_url, biome=biome)


def update_deter(
    db_url: str, deter_b_db_url: str, deter_r_db_url: str, biome: str, all_data: bool
):
    """Update the deter tables."""
    tables = (
        ("deter", "deter_auth", "deter_history")
        if all_data
        else ("deter", "deter_auth")
    )

    for name in tables:
        _update_deter_table(
            db_url=db_url,
            deter_b_db_url=deter_b_db_url,
            deter_r_db_url=deter_r_db_url,
            name=name,
            biome=biome,
        )


def _update_deter_table(
    db_url: str, deter_b_db_url: str, deter_r_db_url: str, name: str, biome: str
):
    """Update the deter.{name} table."""
    logger.info("updating the deter %s table", name)
    print(f"updating the deter.{name} table")

    db = DatabaseFacade.from_url(db_url=db_url)

    # creating sql views for the external database
    view = f"public.{name}"
    logger.info("creating the sql view %s.", view)
    print(f"creating the sql view {name}")

    user, password, host, port, db_name = get_connection_components(
        db_url=deter_b_db_url
    )

    sql = f"""
        DROP VIEW IF EXISTS {view};
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

    view = "public.deter_aggregated_ibama"
    logger.info("creating the sql view %s.", view)
    print(f"creating the sql view {view}")

    user, password, host, port, db_name = get_connection_components(
        db_url=deter_r_db_url
    )

    sql = f"""
        DROP VIEW IF EXISTS {view};
        CREATE OR REPLACE VIEW {view} AS
        SELECT
            remote_data.origin_gid,
            remote_data.date,
            remote_data.areamunkm,
            remote_data.classname,
            remote_data.ncar_ids,
            remote_data.car_imovel,
            remote_data.continuo,
            remote_data.velocidade,
            remote_data.deltad,
            remote_data.est_fund,
            remote_data.dominio,
            remote_data.tp_dominio
        FROM
            dblink('hostaddr={host} port={port} dbname={db_name} user={user} password={password}'::text,
                   'SELECT origin_gid, view_date as date, areamunkm, classname, ncar_ids, car_imovel, continuo, velocidade, deltad, est_fund, dominio, tp_dominio FROM deter_agregate.deter WHERE areatotalkm>=0.01 AND uf<>''MS'' AND source=''D'''::text)
            AS remote_data(origin_gid integer, date date, areamunkm double precision, classname character varying(254), ncar_ids integer, car_imovel character varying(2048), continuo integer, velocidade numeric, deltad integer, est_fund character varying(254), dominio character varying(254), tp_dominio character varying(254));
    """

    # db.execute(sql)

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

    sql = f"""
        UPDATE deter.{table}
        SET
            ncar_ids=ibama.ncar_ids, car_imovel=ibama.car_imovel, velocidade=ibama.velocidade,
            deltad=ibama.deltad, est_fund=ibama.est_fund, dominio=ibama.dominio, tp_dominio=ibama.tp_dominio
        FROM public.deter_aggregated_ibama as ibama
        WHERE deter.{table}.origin_gid=ibama.origin_gid
            AND deter.{table}.areamunkm=ibama.areamunkm
            AND (ibama.ncar_ids IS NOT NULL OR ibama.est_fund IS NOT NULL OR ibama.dominio IS NOT NULL);
    """

    if biome == "Amazônia":
        # db.execute(sql)
        pass


def update_publish_date(db_url: str, deter_db_url: str, biome: str):
    """Update the deter.deter_publish_date."""
    logger.info("updating the deter.deter_publis_date table")
    print("updating the deter.deter_publis_date table")

    db = DatabaseFacade.from_url(db_url=db_url)

    # creating a sql view for the external database
    logger.info("creating the sql view public.deter_publish_date")
    print("creating the sql view public.deter_publish_date")

    user, password, host, port, db_name = get_connection_components(db_url=deter_db_url)

    sql = f"""
        DROP VIEW IF EXISTS public.deter_publish_date;
        CREATE OR REPLACE VIEW public.deter_publish_date AS
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
    print("inserting data from view deter.deter_publish_date")

    table = "deter.deter_publish_date"

    db.truncate(table=table)

    sql = f"""
        INSERT INTO {table} (
            date, biome
        )
        SELECT
            deter.date,
            '{biome}'::text as biome
        FROM public.deter_publish_date as deter
    """

    db.execute(sql)
