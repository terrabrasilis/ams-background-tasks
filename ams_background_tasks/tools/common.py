"""Common definitions."""

from __future__ import annotations

import sys

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger

logger = get_logger(__name__, sys.stdout)


# cells
CELL_25KM = "25km"
CELL_150KM = "150km"
CELLS = [CELL_25KM, CELL_150KM]

# biomes
AMAZONIA = "Amazônia"
CERRADO = "Cerrado"
PANTANAL = "Pantanal"

BIOMES = [AMAZONIA, CERRADO, PANTANAL]

# constants
PIXEL_LAND_USE_AREA = 29.875 * 29.875 * (10**-6)

# classnames
ACTIVE_FIRES_CLASSNAME = "AF"
RISK_CLASSNAME = "RK"

# indicators
DETER_INDICATOR = "deter"
ACTIVE_FIRES_INDICATOR = "focos"

INDICATORS = [
    DETER_INDICATOR,
    ACTIVE_FIRES_INDICATOR,
]


def is_valid_biome(biome: str):
    return biome in BIOMES


def is_valid_cell(cell: str):
    return cell in CELLS


def get_prefix(is_temp: bool):
    return "tmp_" if is_temp else ""


def read_spatial_units(db: DatabaseFacade):
    return dict(
        db.fetchall(
            query="SELECT dataname, as_attribute_name FROM public.spatial_units"
        )
    )


def get_biome_acronym(biome: str):
    assert is_valid_biome(biome=biome)
    return {
        AMAZONIA: "amz",
        CERRADO: "cer",
        PANTANAL: "pan",
    }[biome]


def recreate_spatial_table(db: DatabaseFacade, spatial_unit: str, is_temp: bool):
    table = f"{get_prefix(is_temp=is_temp)}{spatial_unit}_land_use"

    logger.info("recreating %s.", table)

    db.create_table(
        schema="public",
        name=table,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "suid int NOT NULL",
            "land_use_id int NOT NULL",
            "classname varchar(2) NOT NULL",
            "date date NOT NULL",
            "area double precision",
            "percentage double precision",
            "counts int4",
            "risk double precision NOT NULL DEFAULT 0.0",
            "geocode character varying(80)",
            "biome character varying(254)",
        ],
        force_recreate=True,
    )

    db.create_indexes(
        schema="public",
        name=table,
        columns=[
            "classname:btree",
            "date:btree",
            "biome:btree",
            "geocode:btree",
        ],
        force_recreate=False,
    )


def reset_land_use_tables(db_url: str, is_temp: bool):
    db = DatabaseFacade.from_url(db_url=db_url)
    for spatial_unit in read_spatial_units(db=db):
        recreate_spatial_table(db=db, spatial_unit=spatial_unit, is_temp=is_temp)
