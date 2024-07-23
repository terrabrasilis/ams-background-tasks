"""Create the AMS database."""

from __future__ import annotations

import logging
import os

import click

from ams_background_tasks.database_utils import DatabaseFacade

logger = logging.getLogger(__name__)


@click.command("create-db")
@click.option(
    "--db-url",
    required=False,
    type=str,
    default="",
    help="AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).",
)
@click.option(
    "--force-recreate",
    required=False,
    is_flag=True,
    default=False,
    help="Force to recreate the AMS database.",
)
def main(db_url: str, force_recreate: bool):
    """Create the AMS database."""
    db_url = os.getenv("AMS_DB_URL") if not db_url else db_url
    print(db_url)
    assert db_url

    db = DatabaseFacade.from_url(db_url=db_url)

    # schemas
    db.create_schema(name="fires", force_recreate=False)
    db.create_schema(name="deter", force_recreate=False)

    # tables

    # municipalities
    create_municipalities_table(db=db, force_recreate=force_recreate)

    # active_fires
    create_active_fires_table(db=db, force_recreate=force_recreate)

    # deter
    create_deter_tables(db=db, force_recreate=force_recreate)


def create_municipalities_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the public.municipalities table."""
    columns = [
        "id integer PRIMARY KEY",
        "name character varying(150) COLLATE pg_catalog.default",
        "geocode character varying(80) COLLATE pg_catalog.default UNIQUE",
        "year integer",
        "geom geometry(MultiPolygon, 4674)",
    ]

    schema = "public"
    name = "municipalities"

    db.create_table(
        schema=schema,
        name="name",
        columns=columns,
        force_recreate=force_recreate,
    )

    columns = [
        "geocode:btree",
        "geom:gist",
    ]

    db.create_indexes(
        schema=schema, name=name, columns=columns, force_recreate=force_recreate
    )


def create_active_fires_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the fires.active_fires table."""
    columns = [
        "id integer PRIMARY KEY",
        "view_date date",
        "satelite character varying(254) COLLATE pg_catalog.default",
        "estado character varying(254) COLLATE pg_catalog.default",
        "municipio character varying(254) COLLATE pg_catalog.default",
        "diasemchuva integer",
        "precipitacao double precision",
        "riscofogo double precision",
        "geom geometry(Point, 4674)",
        "geocode character varying(80) COLLATE pg_catalog.default",
        "biome character varying(254) COLLATE pg_catalog.default",
    ]

    schema = "fires"
    name = "active_fires"

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )

    columns = [
        "view_date:btree",
        "biome:btree",
        "geocode:btree",
        "geom:gist",
    ]

    db.create_indexes(
        schema=schema, name=name, columns=columns, force_recreate=force_recreate
    )


def _create_deter_table(db: DatabaseFacade, name: str, force_recreate: bool):
    """Create the deter.{name} table."""
    columns = [
        "gid character varying(254) COLLATE pg_catalog.default NOT NULL PRIMARY KEY",
        "origin_gid integer",
        "classname character varying(254) COLLATE pg_catalog.default",
        "quadrant character varying(5) COLLATE pg_catalog.default",
        "orbitpoint character varying(10) COLLATE pg_catalog.default",
        "date date",
        "sensor character varying(10) COLLATE pg_catalog.default",
        "satellite character varying(13) COLLATE pg_catalog.default",
        "areatotalkm double precision",
        "areamunkm double precision",
        "areauckm double precision",
        "mun character varying(254) COLLATE pg_catalog.default",
        "uf character varying(2) COLLATE pg_catalog.default",
        "uc character varying(254) COLLATE pg_catalog.default",
        "geom geometry(MultiPolygon, 4674)",
        "month_year character varying(10) COLLATE pg_catalog.default",
        "ncar_ids integer",
        "car_imovel text COLLATE pg_catalog.default",
        "continuo integer",
        "velocidade numeric",
        "deltad integer",
        "est_fund character varying(254) COLLATE pg_catalog.default",
        "dominio character varying(254) COLLATE pg_catalog.default",
        "tp_dominio character varying(254) COLLATE pg_catalog.default",
        "biome character varying(254) COLLATE pg_catalog.default",
        "geocode character varying(80) COLLATE pg_catalog.default",
    ]

    schema = "deter"

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )

    columns = [
        "classname:btree",
        "date:btree",
        "biome:btree",
        "geocode:btree",
        "geom:gist",
    ]

    db.create_indexes(
        schema=schema, name=name, columns=columns, force_recreate=force_recreate
    )


def create_deter_tables(db: DatabaseFacade, force_recreate: bool = False):
    """Create the deter.[deter, deter_auth, deter_history] tables."""
    names = ("deter", "deter_auth", "deter_history")
    for name in names:
        _create_deter_table(db=db, name=name, force_recreate=force_recreate)
