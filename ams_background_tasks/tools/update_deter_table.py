"""Update the AMS DETER tables."""

from __future__ import annotations

import logging
import os

import click

from ams_background_tasks.database_utils import (
    DatabaseFacade,
    get_connection_components,
)
from ams_background_tasks.tools.common import (
    BIOMES,
    DETER_INDICATOR,
    create_processing,
    get_biome_acronym,
    get_biome_name,
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
@click.option(
    "--limit",
    required=False,
    type=int,
    default=0,
    help="Restrict the number of rows to update (test purpose).",
)
@click.option(
    "--create-processing-flag",
    required=True,
    is_flag=True,
    default=False,
    help="If true, create a process in the processing table.",
)
def main(
    db_url: str,
    deter_b_db_url: str,
    biome: str,
    all_data: bool,
    truncate: bool,
    limit: int,
    create_processing_flag: bool,
):
    """Update the DETER tables from official databases using SQL views."""
    db_url = os.getenv("AMS_DB_URL", "") if not db_url else db_url
    logger.debug(db_url)
    assert db_url

    deter_b_db_url = (
        os.getenv("AMS_DETER_B_DB_URL", "") if not deter_b_db_url else deter_b_db_url
    )
    logger.debug(deter_b_db_url)
    assert deter_b_db_url

    db = DatabaseFacade.create(db_url=db_url)

    if create_processing_flag:
        create_processing(
            db=db, indicator=DETER_INDICATOR, process="update", status="processing"
        )

    update_deter(
        db=db,
        deter_b_db_url=deter_b_db_url,
        biome=biome,
        all_data=all_data,
        truncate=truncate,
        limit=limit,
    )

    update_publish_date(
        db=db,
        name="tmp_deter_publish_date",
        deter_db_url=deter_b_db_url,
        biome=biome,
        truncate=truncate,
    )

    db.commit()


def update_deter(
    db: DatabaseFacade,
    deter_b_db_url: str,
    biome: str,
    all_data: bool,
    truncate: bool,
    limit: int,
):
    """Update the DETER tables (deter, deter_auth, deter_history)."""
    _ = all_data  # no warn

    tables = ("deter", "deter_auth")
    ext_tables = (
        f"deter_{get_biome_name(biome)}",
        f"deter_{get_biome_name(biome)}_auth",
    )

    prefix = "tmp_"

    for index, name in enumerate(tables):
        _update_deter_table(
            db=db,
            name=name,
            deter_b_db_url=deter_b_db_url,
            ext_table=ext_tables[index],
            biome=biome,
            truncate=truncate,
            prefix=prefix,
            limit=limit,
        )
        # _update_classname(db=db, prefix=prefix, name=name)


def _optimize_for_update(db: DatabaseFacade, name: str):
    # disable autovacuum
    db.execute(f"ALTER TABLE deter.{name} SET (autovacuum_enabled = off);")

    # drop indexes
    columns = [
        "classname",
        "image_date",
        "biome",
        "geocode",
        "geom",
    ]

    db.drop_indexes(schema="deter", name=name, columns=columns)


def _finalize_update(db: DatabaseFacade, name: str):
    # enable autovacuum
    db.execute(f"ALTER TABLE deter.{name} SET (autovacuum_enabled = on);")

    # recreate indexes
    columns = [
        "classname:btree",
        "image_date:btree",
        "biome:btree",
        "geocode:btree",
        "geom:gist",
    ]

    db.create_indexes(schema="deter", name=name, columns=columns, force_recreate=False)


def _update_deter_table(
    *,
    db: DatabaseFacade,
    deter_b_db_url: str,
    name: str,
    biome: str,
    truncate: bool,
    ext_table: str,
    prefix: str,
    limit: int,
):
    """Update the deter.{name} table."""
    name = f"{prefix}{name}"

    logger.info("updating the deter.%s table", name)

    _optimize_for_update(db=db, name=name)

    # creating sql view for the external database
    view = f"public.{get_biome_acronym(biome=biome)}_{name}"
    logger.info("creating the sql view %s.", view)

    user, password, host, port, db_name = get_connection_components(
        db_url=deter_b_db_url
    )

    sql = f"DROP VIEW IF EXISTS {view}"
    db.execute(sql)

    sql = f"""
        CREATE OR REPLACE VIEW {view} AS
        SELECT
            remote_data.uuid::TEXT AS origin_gid,
            remote_data.image_date AS image_date,
            remote_data.class_name AS classname,
            remote_data.satellite AS satellite,
            remote_data.sensor AS sensor,
            remote_data.path_row AS path_row,
            remote_data.area_km AS area_km,
            remote_data.geom AS geom
        FROM
            dblink(
                'hostaddr={host} port={port} dbname={db_name} user={user} password={password}'::text,
                'SELECT uuid, image_date, class_name, satellite, sensor, path_row, area_km, geom FROM public.{ext_table}'::text
            ) AS remote_data(
                uuid varchar,
                image_date date,
                class_name varchar(254),
                satellite varchar(13),
                sensor varchar(10),
                path_row varchar(10),
                area_km double precision,
                geom geometry(MultiPolygon,4674)
            );    
    """

    db.execute(sql)

    # inserting data
    logger.info("inserting data from view %s", view)

    table = f"deter.{name}"

    if truncate:
        db.truncate(table=table)

    limit_sql = f"LIMIT {limit}" if limit > 0 else ""

    sql = f"""
        INSERT INTO deter.{name}(
            origin_gid, image_date, classname, satellite, sensor, path_row, area_km, geom, biome
        )
        SELECT deter.origin_gid, deter.image_date, deter.classname, deter.satellite,
            deter.sensor, deter.path_row, deter.area_km, deter.geom, '{biome}'::text as biome
        FROM {view} as deter, public.biome_border as border
        WHERE ST_Within(deter.geom, border.geom) AND border.biome='{biome}'
        {limit_sql};
    """

    db.execute(sql)

    # intersecting with municipalities
    logger.info("intersecting with municipalities")

    years = db.fetchall(
        f"""
        SELECT DISTINCT EXTRACT(YEAR FROM image_date)::int AS year
        FROM {table}
        WHERE biome='{biome}'
        ORDER BY year;
    """
    )

    years = [_[0] for _ in years]

    for year in years:
        sql = f"""
            UPDATE {table} AS dt
            SET geocode=a.geocode
            FROM (
                SELECT 
                    dt2.gid, mun.geocode
                FROM 
                    {table} AS dt2
                JOIN 
                    public.municipalities mun 
                    ON dt2.geom && mun.geometry
                    AND ST_Within(ST_PointOnSurface(dt2.geom), mun.geometry)
                WHERE
                    dt2.biome='{biome}'
                    AND EXTRACT(YEAR FROM dt2.image_date)::int = {year}
            ) AS a
            WHERE 
                dt.gid = a.gid
                AND dt.biome='{biome}';
        """

        db.execute(sql)

    _finalize_update(db=db, name=name)


def update_publish_date(
    db: DatabaseFacade, name: str, deter_db_url: str, biome: str, truncate: bool
):
    """Update the deter.deter_publish_date."""
    user, password, host, port, db_name = get_connection_components(db_url=deter_db_url)

    # creating sql view for the external database
    view = f"public.{get_biome_acronym(biome=biome)}_{name}"
    logger.info("creating the sql view %s.", view)

    sql = f"""
        CREATE OR REPLACE VIEW {view} AS
        SELECT
            remote_data.date
        FROM
            dblink('hostaddr={host} port={port} dbname={db_name} user={user} password={password}'::text,
            'SELECT publish_date FROM public.deter_publish_date'::text)
        AS remote_data(date date);
    """

    db.execute(sql)

    # inserting data
    logger.info("inserting data from view deter.%s", name)

    table = f"deter.{name}"

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
