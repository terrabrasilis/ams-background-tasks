"""Create AMS database."""

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

    # tables

    # municipalities
    create_municipalities_table(db=db, force_recreate=force_recreate)

    # active_fires
    create_active_fires_table(db=db, force_recreate=force_recreate)


def create_municipalities_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the public.municipalities table."""
    columns = [
        "id integer PRIMARY KEY",
        "name character varying(150) COLLATE pg_catalog.default",
        "geocode character varying(80) COLLATE pg_catalog.default UNIQUE",
        "year integer",
        "geom geometry(MultiPolygon, 4674)",
    ]

    db.create_table(
        schema="public",
        name="municipalities",
        columns=columns,
        force_recreate=force_recreate,
    )

    # index municipalities_geocode_idx
    db.create_index(
        schema="public",
        name="municipalities_geocode_idx",
        table="municipalities",
        method="btree",
        column="geocode",
        force_recreate=force_recreate,
    )

    # index municipalities_geom_idx
    db.create_index(
        schema="public",
        name="municipalities_geom_idx",
        table="municipalities",
        method="gist",
        column="geom",
        force_recreate=force_recreate,
    )


def create_active_fires_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the fires.active_fires table."""
    columns = [
        "id integer PRIMARY KEY",
        "view_date date",
        "bioma character varying(254) COLLATE pg_catalog.default",
        "satelite character varying(254) COLLATE pg_catalog.default",
        "estado character varying(254) COLLATE pg_catalog.default",
        "municipio character varying(254) COLLATE pg_catalog.default",
        "diasemchuva integer",
        "precipitacao double precision",
        "riscofogo double precision",
        "geom geometry(Point, 4674)",
        "geocode character varying(80) COLLATE pg_catalog.default",
    ]

    db.create_table(
        schema="fires",
        name="active_fires",
        columns=columns,
        force_recreate=force_recreate,
    )

    # index active_fires_view_date_idx
    db.create_index(
        schema="fires",
        name="active_fires_view_date_idx",
        table="active_fires",
        method="btree",
        column="view_date ASC NULLS LAST",
        force_recreate=force_recreate,
    )

    # index active_fires_bioma_idx
    db.create_index(
        schema="fires",
        name="active_fires_bioma_idx",
        table="active_fires",
        method="btree",
        column="bioma",
        force_recreate=force_recreate,
    )

    # index active_fires_geocode_idx
    db.create_index(
        schema="fires",
        name="active_fires_geocode_idx",
        table="active_fires",
        method="btree",
        column="geocode",
        force_recreate=force_recreate,
    )

    # index idx_fires_active_fires_geom
    db.create_index(
        schema="fires",
        name="fires_active_fires_geom_idx",
        table="active_fires",
        method="gist",
        column="geom",
        force_recreate=force_recreate,
    )
