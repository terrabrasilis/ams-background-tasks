"""Common definitions."""

from __future__ import annotations

import sys

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger

logger = get_logger(__name__, sys.stdout)


# cells
CELL_5KM = "5km"
CELL_25KM = "25km"
CELL_150KM = "150km"
CELLS = [CELL_5KM, CELL_25KM, CELL_150KM]

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
RISK_INDICATOR = "risco"

INDICATORS = [
    DETER_INDICATOR,
    ACTIVE_FIRES_INDICATOR,
    RISK_INDICATOR,
]

# land_use_type
AMS = "ams"
PPCDAM = "ppcdam"

LAND_USE_TYPES = [AMS, PPCDAM]


def is_valid_biome(biome: str):
    return biome in BIOMES


def is_valid_cell(cell: str):
    return cell in CELLS


def is_valid_indicator(indicator: str):
    return indicator in INDICATORS


def is_valid_land_use_type(land_use_type: str):
    return land_use_type in LAND_USE_TYPES


def get_prefix(is_temp: bool):
    return "tmp_" if is_temp else ""


def read_spatial_units(db: DatabaseFacade):
    return dict(
        db.fetchall(
            query="SELECT dataname, as_attribute_name FROM public.spatial_units"
        )
    )


def read_biomes(db: DatabaseFacade):
    res = db.fetchall(query="SELECT biome FROM public.biome")
    return [_[0] for _ in res]


def get_biome_acronym(biome: str):
    assert is_valid_biome(biome=biome)
    return {
        AMAZONIA: "amz",
        CERRADO: "cer",
        PANTANAL: "pan",
    }[biome]


def recreate_spatial_table(
    db: DatabaseFacade,
    spatial_unit: str,
    is_temp: bool,
    land_use_type: str,
    force_recreate: bool = True,
):
    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

    table = (
        f"{get_prefix(is_temp=is_temp)}{spatial_unit}_land_use{land_use_type_suffix}"
    )

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
        force_recreate=force_recreate,
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
        force_recreate=force_recreate,
    )


def create_land_structure_table(db_url: str, table: str, force_recreate: bool):
    logger.info("creating %s.", table)
    logger.debug("%s:%s", "force_recreate", force_recreate)

    db = DatabaseFacade.from_url(db_url=db_url)

    db.create_table(
        schema="public",
        name=table,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "gid varchar(254) NOT NULL",
            "land_use_id int4 NULL",
            "num_pixels int4 NULL",
            "geocode varchar(80) NULL",
            "biome varchar(254) NULL",
            "UNIQUE (gid, biome, land_use_id, geocode)",
        ],
        force_recreate=force_recreate,
    )

    db.create_indexes(
        schema="public",
        name=table,
        columns=[
            "gid:hash",
            "biome:btree",
            "geocode:btree",
            "gid,biome:btree",
        ],
        force_recreate=force_recreate,
    )


def reset_land_use_tables(
    db_url: str, is_temp: bool, force_recreate: bool, land_use_type: str
):
    db = DatabaseFacade.from_url(db_url=db_url)
    for spatial_unit in read_spatial_units(db=db):
        recreate_spatial_table(
            db=db,
            spatial_unit=spatial_unit,
            is_temp=is_temp,
            force_recreate=force_recreate,
            land_use_type=land_use_type,
        )
