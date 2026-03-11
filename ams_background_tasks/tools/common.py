"""Common definitions."""

from __future__ import annotations

import sys
from datetime import datetime
from urllib.parse import urlparse, urlunparse

import pytz

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
CAATINGA = "Caatinga"
MATA_ATLANTICA = "Mata Atlântica"
PAMPA = "Pampa"

BIOMES = [AMAZONIA, CERRADO, PANTANAL, CAATINGA, MATA_ATLANTICA, PAMPA]

# constants
FIRE_SPREADING_RISK_PIXEL_AREA = 423.0 * 423.0 * (10**-6)  # km^2
PIXEL_LAND_USE_AREA = 29.875 * 29.875 * (10**-6)  # km^2
RISK_SCALE_FACTOR = 1

# classnames
ACTIVE_FIRES_CLASSNAME = "AF"
RISK_IBAMA_CLASSNAME = "RK"
RISK_INPE_CLASSNAME = "RI"
FIRE_SPREADING_RISK_CLASSNAME = "FS"
ACTIVE_FIRES_TODAY_CLASSNAME = "FT"

# indicators
DETER_INDICATOR = "deter"
ACTIVE_FIRES_INDICATOR = "focos"
RISK_IBAMA_INDICATOR = "risco-ibama"
RISK_INPE_INDICATOR = "risco"
FIRE_SPREADING_RISK_INDICATOR = "risco-espalhamento-fogo"
ACTIVE_FIRES_TODAY_INDICATOR = "focos-hoje"

RISK_INDICATORS = [RISK_IBAMA_INDICATOR, RISK_INPE_INDICATOR]

INDICATORS = [
    DETER_INDICATOR,
    ACTIVE_FIRES_INDICATOR,
    RISK_IBAMA_INDICATOR,
    RISK_INPE_INDICATOR,
    FIRE_SPREADING_RISK_INDICATOR,
    ACTIVE_FIRES_TODAY_INDICATOR,
]

# land_use_type
AMS = "ams"
PPCDAM = "ppcdam"
PRODES = "prodes"

LAND_USE_TYPES = [AMS, PPCDAM, PRODES]


def is_valid_biome(biome: str):
    return biome in BIOMES


def is_valid_cell(cell: str):
    return cell in CELLS


def is_valid_indicator(indicator: str):
    return indicator in INDICATORS


def map_indicator_to_table_name(indicator):
    assert is_valid_indicator(indicator)
    return {
        DETER_INDICATOR: "deter",
        FIRE_SPREADING_RISK_INDICATOR: "fire_sr",
        ACTIVE_FIRES_INDICATOR: "fires",
        ACTIVE_FIRES_TODAY_INDICATOR: "fires_today",
        RISK_INPE_INDICATOR: "risk",
        RISK_IBAMA_INDICATOR: "risk_ibama",
    }[indicator]


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
        CAATINGA: "caa",
        MATA_ATLANTICA: "maa",
        PAMPA: "pap",
    }[biome]


def get_biome_name(biome: str):
    assert is_valid_biome(biome=biome)
    return {
        AMAZONIA: "amazonia",
        CERRADO: "cerrado",
        PANTANAL: "pantanal",
        CAATINGA: "caatinha",
        MATA_ATLANTICA: "mata_atlantica",
        PAMPA: "pampa",
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
            "score double precision NOT NULL DEFAULT 0.0",
            "units int4",
            "geocode character varying(80)",
            "biome character varying(254)",
        ],
        force_recreate=force_recreate,
    )


def prepare_to_update_land_use_table(db: DatabaseFacade, table: str):
    index_columns = [
        "classname:btree",
        # "date:btree",
        # "biome:btree",
        "geocode:btree",
        # "suid:btree",
        # "land_use_id:btree",
        "geocode,classname,land_use_id,date:btree",
        "suid,classname,date:btree",
        "biome,classname,date:btree",
        "biome,classname,land_use_id,date:btree",
        "classname,land_use_id,geocode,date:btree",
    ]
    prepare_table_to_update(db=db, schema="public", name=table, columns=index_columns)


def optimize_land_use_table(db: DatabaseFacade, table: str):
    index_columns = [
        "classname:btree",
        # "date:btree",
        # "biome:btree",
        "geocode:btree",
        # "suid:btree",
        # "land_use_id:btree",
        "geocode,classname,land_use_id,date:btree",
        "suid,classname,date:btree",
        "biome,classname,date:btree",
        "biome,classname,land_use_id,date:btree",
        "classname,land_use_id,geocode,date:btree",
    ]
    optimize_table(db=db, schema="public", name=table, columns=index_columns)


def create_land_structure_table(db: DatabaseFacade, table: str, force_recreate: bool):
    logger.info("creating %s.", table)
    logger.debug("%s:%s", "force_recreate", force_recreate)

    cname = f"{table}_gblg"

    db.create_table(
        schema="public",
        name=table,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "gid int4 NOT NULL",
            "land_use_id int4 NULL",
            "num_pixels int4 NULL",
            "geocode varchar(80) NULL",
            "biome varchar(254) NULL",
            f"CONSTRAINT {cname} UNIQUE (gid, biome, land_use_id, geocode)",
        ],
        force_recreate=force_recreate,
    )


def prepare_to_update_land_structure_table(db: DatabaseFacade, table: str):
    index_columns = [
        "gid:btree",
        "biome:btree",
        "geocode:btree",
        "land_use_id:btree",
        "gid,biome:btree",
    ]
    prepare_table_to_update(db=db, schema="public", name=table, columns=index_columns)


def optimize_land_structure_table(db: DatabaseFacade, table: str):
    index_columns = [
        "gid:btree",
        "biome:btree",
        "geocode:btree",
        "land_use_id:btree",
        "gid,biome:btree",
    ]
    optimize_table(db=db, schema="public", name=table, columns=index_columns)


def reset_land_use_tables(
    db: DatabaseFacade, is_temp: bool, force_recreate: bool, land_use_type: str
):
    for spatial_unit in read_spatial_units(db=db):
        recreate_spatial_table(
            db=db,
            spatial_unit=spatial_unit,
            is_temp=is_temp,
            force_recreate=force_recreate,
            land_use_type=land_use_type,
        )


def delete_land_use_tables_from_tmp(db: DatabaseFacade, land_use_type: str):
    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

    for spatial_unit in read_spatial_units(db=db):
        table = (
            f"{get_prefix(is_temp=False)}{spatial_unit}_land_use{land_use_type_suffix}"
        )
        tmp_table = (
            f"{get_prefix(is_temp=True)}{spatial_unit}_land_use{land_use_type_suffix}"
        )

        classnames = ",".join(
            [
                f"'{_}'"
                for _ in get_classnames_from_land_use_table(db=db, table=tmp_table)
            ]
        )

        if len(classnames) == 0:
            continue

        sql = f"DELETE FROM public.{table} WHERE classname IN ({classnames});"
        db.execute(sql=sql, log=True)


def get_indicators_from_tmp(db: DatabaseFacade, land_use_type: str):
    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

    classnames_list = []
    for spatial_unit in read_spatial_units(db=db):
        tmp_table = (
            f"{get_prefix(is_temp=True)}{spatial_unit}_land_use{land_use_type_suffix}"
        )

        classnames_list += [
            f"'{_}'" for _ in get_classnames_from_land_use_table(db=db, table=tmp_table)
        ]

    classnames = ",".join(list(set(classnames_list)))

    if not classnames:
        return []

    titles = [
        _[0]
        for _ in db.fetchall(
            query=f"SELECT DISTINCT title FROM public.class_group WHERE name in ({classnames});"
        )
    ]

    indicators = []
    for title in titles:
        if "deter" in title.lower():
            indicators.append(DETER_INDICATOR)
            continue

        if "focos de hoje" in title.lower():
            indicators.append(ACTIVE_FIRES_TODAY_INDICATOR)
            continue

        if "focos" in title.lower():
            indicators.append(ACTIVE_FIRES_INDICATOR)
            continue

        if "risco de espalhamento" in title.lower():
            indicators.append(FIRE_SPREADING_RISK_INDICATOR)
            continue

        if "risco" in title.lower():
            indicators.append(RISK_INPE_INDICATOR)
            continue

    return list(set(indicators))


def delete_land_use_tables(
    db: DatabaseFacade, land_use_type: str, is_temp: bool, indicator: str
):
    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

    classnames = ",".join(
        [f"'{_}'" for _ in get_classnames_from_indicator(db=db, indicator=indicator)]
    )

    for spatial_unit in read_spatial_units(db=db):
        table = f"{get_prefix(is_temp=is_temp)}{spatial_unit}_land_use{land_use_type_suffix}"
        sql = f"DELETE FROM public.{table} WHERE classname IN ({classnames});"
        db.execute(sql=sql, log=True)


def get_classnames_from_land_use_table(db: DatabaseFacade, table: str):
    sql = f"SELECT DISTINCT classname FROM public.{table};"
    classnames = db.fetchall(sql)

    logger.debug(classnames)

    return [_[0] for _ in classnames]


def get_classnames_from_indicator(db: DatabaseFacade, indicator: str):
    sql = f"""
        SELECT name FROM public.class_group
	    WHERE LOWER(title) like '{indicator}%';
    """
    classnames = db.fetchall(sql)

    logger.debug(classnames)

    return [_[0] for _ in classnames]


def parse_url(url):
    parsed_url = urlparse(url)
    fixed_url = "/".join(part for part in parsed_url.path.split("/") if part)
    return urlunparse(
        (
            parsed_url.scheme,
            parsed_url.netloc,
            fixed_url,
            parsed_url.params,
            parsed_url.query,
            parsed_url.fragment,
        )
    )


def create_processing(
    db: DatabaseFacade, indicator: str, process: str, status: str = "pending"
):
    schema = "public"
    name = "processing"

    utc_now = datetime.now(pytz.UTC)
    now = utc_now.replace(tzinfo=None) - utc_now.utcoffset()
    now = now.isoformat(sep=" ")

    sql = f"""
        INSERT INTO {schema}.{name} (date, start_process, indicator, process, status)
        VALUES (CURRENT_DATE, '{now}', '{indicator}', '{process}', '{status}');
    """

    db.execute(sql=sql)


def finalize_processing(db: DatabaseFacade, indicator: str, process: str, status: str):
    schema = "public"
    name = "processing"

    utc_now = datetime.now(pytz.UTC)
    now = utc_now.replace(tzinfo=None) - utc_now.utcoffset()
    now = now.isoformat(sep=" ")

    sql = f"""
        UPDATE {schema}.{name}
        SET end_process='{now}', status='{status}'
        WHERE id=(SELECT MAX(id) FROM {schema}.{name} WHERE indicator='{indicator}' AND process='{process}');
    """

    db.execute(sql=sql)


def get_land_use_type_suffix(land_use_type: str):
    return {
        AMS: "",
        PPCDAM: "_ppcdam",
        PRODES: "_prodes",
    }[land_use_type]


def prepare_table_to_update(
    db: DatabaseFacade,
    schema: str,
    name: str,
    columns: list,
):
    """Prepare table to update the data."""
    # disable autovacuum
    # db.execute(f"ALTER TABLE deter.{name} SET (autovacuum_enabled = off);")
    db.drop_indexes(schema=schema, name=name, columns=columns)


def optimize_table(db: DatabaseFacade, schema: str, name: str, columns: list):
    # enable autovacuum
    # db.execute(f"ALTER TABLE deter.{name} SET (autovacuum_enabled = on);")
    db.create_indexes(schema=schema, name=name, columns=columns, force_recreate=False)


def analyze_table(db: DatabaseFacade, schema: str, name: str):
    """Update the table stats."""
    db.analyze(schema=schema, table=name)
