"""Create the AMS database."""

from __future__ import annotations

import os
import sys

import click

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger
from ams_background_tasks.tools.common import (
    AMS,
    CELL_5KM,
    CELL_25KM,
    CELL_150KM,
    PPCDAM,
    is_valid_cell,
    is_valid_land_use_type,
)

logger = get_logger(__name__, sys.stdout)


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
    logger.debug(db_url)
    assert db_url

    db = DatabaseFacade.from_url(db_url=db_url)

    db.create_postgis_extension()
    db.create_dblink_extension()

    db.create_schema(name="fires", force_recreate=False)
    db.create_schema(name="deter", force_recreate=False)

    # spatial units
    create_spatial_units_table(db=db, force_recreate=force_recreate)

    create_states_table(db=db, force_recreate=force_recreate)
    create_states_function(db=db, force_recreate=force_recreate)

    create_municipalities_table(db=db, force_recreate=force_recreate)
    create_municipalities_function(db=db, force_recreate=force_recreate)

    create_cell_table(db=db, cell=CELL_5KM, force_recreate=force_recreate)
    create_cell_function(db=db, cell=CELL_5KM, force_recreate=force_recreate)

    create_cell_table(db=db, cell=CELL_25KM, force_recreate=force_recreate)
    create_cell_function(db=db, cell=CELL_25KM, force_recreate=force_recreate)

    create_cell_table(db=db, cell=CELL_150KM, force_recreate=force_recreate)
    create_cell_function(db=db, cell=CELL_150KM, force_recreate=force_recreate)

    # biome border
    create_biome_tables(db=db, force_recreate=force_recreate)

    # active_fires
    create_active_fires_table(db=db, force_recreate=force_recreate)

    # deter
    create_deter_tables(db=db, force_recreate=force_recreate)

    # classnames
    create_class_tables(db=db, force_recreate=force_recreate)

    # land use
    create_land_use_table(db=db, force_recreate=force_recreate, land_use_type=AMS)
    create_land_use_table(db=db, force_recreate=force_recreate, land_use_type=PPCDAM)

    # municipalities group
    create_municipalities_group_tables(db=db, force_recreate=force_recreate)

    # risk
    create_risk_tables(db=db, force_recreate=force_recreate)


def create_municipalities_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the public.municipalities and public.municipalities_biome tables."""
    logger.info("creating the municipalities tables.")

    schema = "public"

    if force_recreate:
        db.drop_table(f"{schema}.municipalities_biome")
        db.drop_table(f"{schema}.municipalities")

    name = "municipalities"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "suid serial NOT NULL PRIMARY KEY",
            "name varchar(150)",
            "geocode varchar(80) UNIQUE",
            "area double precision",
            "state_acr varchar(2)",
            "state_name varchar(256)",
            "geometry geometry(MultiPolygon, 4674)",
            "FOREIGN KEY (state_acr) REFERENCES public.states (acronym)",
        ],
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geocode:btree", "geometry:gist", "state_acr:btree"],
        force_recreate=force_recreate,
    )

    name = "municipalities_biome"
    columns = [
        "bid serial NOT NULL PRIMARY KEY",
        "geocode varchar(80)",
        "biome varchar(254)",
        "FOREIGN KEY (geocode) REFERENCES public.municipalities (geocode)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geocode:btree", "biome:btree"],
        force_recreate=force_recreate,
    )


def create_municipalities_function(db: DatabaseFacade, force_recreate: bool):
    if force_recreate:
        sql = """
            DROP FUNCTION IF EXISTS public.ams_get_municipalities(
                character varying, date, date, date, integer[], character varying[]
            );
        """
        db.execute(sql=sql)

    sql = """
        CREATE OR REPLACE FUNCTION public.ams_get_municipalities(
                clsname character varying,
                startdate date,
                enddate date,
                publish_date date,
                land_use_ids integer[],
                biomes character varying[],
                municipality_group_name character varying,
	        geocodes character varying[],
                riskThreshold float,
                isAuthenticated boolean DEFAULT False                
            )
            RETURNS TABLE(suid integer, name character varying, geometry geometry, classname character varying, date date, percentage double precision, area double precision, counts bigint) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
                    DECLARE
                        effective_publish_date date;

                    BEGIN
                        IF isAuthenticated THEN
                            effective_publish_date := CURRENT_DATE;
                        ELSE
                            SELECT MAX(dpd.date)
                            INTO effective_publish_date
                            FROM deter.deter_publish_date dpd
                            WHERE ('ALL' = ANY(biomes) OR dpd.biome = ANY(biomes));
                        END IF;

                        RETURN QUERY
                        SELECT 
                            mun.suid AS suid, 
                            mun.name AS name, 
                            mun.geometry AS geometry, 
                            mlu_j.classname AS classname, 
                            mlu_j.date AS date, 
                            COALESCE(mlu_j.perc, 0) AS percentage, 
                            COALESCE(mlu_j.total, 0) AS area, 
                            COALESCE(mlu_j.counts, 0) AS counts
                        FROM public."municipalities" mun
                        INNER JOIN (
                            SELECT 
                                mlu.suid, 
                                mlu.classname, 
                                MAX(mlu.date) AS date, 
                                SUM(mlu.percentage) AS perc, 
                                SUM(mlu.area) AS total, 
                                SUM(mlu.counts) AS counts
                            FROM public."municipalities_land_use" mlu
                            WHERE
                                (mlu.date <= effective_publish_date OR clsname IN ('AF', 'RK'))
                                AND mlu.land_use_id = ANY (land_use_ids)
                                AND mlu.classname = clsname
                                AND mlu.date > enddate
                                AND mlu.date <= startdate
                                AND mlu.risk >= riskThreshold
                                AND ('ALL' = ANY (biomes) OR mlu.biome = ANY (biomes))
                                AND (municipality_group_name = 'ALL' OR mlu.geocode =
                                    ANY(
                                        SELECT geocode
                                        FROM public.municipalities_group_members mgm
                                        WHERE mgm.group_id = (
                                              SELECT mg.id
                                              FROM public.municipalities_group mg
                                              WHERE mg.name=municipality_group_name
                                        )
                                   )
                                   OR mlu.geocode = ANY(geocodes)   
                               )
                            GROUP BY mlu.suid, mlu.classname
                        ) AS mlu_j
                        ON mun.suid = mlu_j.suid
                        ORDER BY COALESCE(mlu_j.perc, 0) DESC;
                    END;
        
        $BODY$;
    """

    db.execute(sql=sql)


def create_states_table(db: DatabaseFacade, force_recreate: bool):
    """Create the states and states_biome tables."""

    schema = "public"

    if force_recreate:
        db.drop_table(f"{schema}.states_biome")
        db.drop_table(f"{schema}.states", cascade=True)

    name = "states"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "suid serial NOT NULL PRIMARY KEY",
            "acronym varchar(2) UNIQUE",
            "name varchar(80) UNIQUE",
            "geocode varchar(80) UNIQUE",
            "area double precision",
            "geometry geometry(MultiPolygon, 4674)",
        ],
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geometry:gist", "acronym:btree", "geocode:btree"],
        force_recreate=force_recreate,
    )

    name = "states_biome"
    columns = [
        "bid serial NOT NULL PRIMARY KEY",
        "name varchar(80)",
        "biome varchar(254)",
        f"FOREIGN KEY (name) REFERENCES {schema}.states (name)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["biome:btree"],
        force_recreate=force_recreate,
    )


def create_states_function(db: DatabaseFacade, force_recreate: bool):
    if force_recreate:
        sql = """
            DROP FUNCTION IF EXISTS public.ams_get_states(
                character varying, date, date, date, integer[], character varying[]
            );
        """
        db.execute(sql=sql)

    sql = """
        CREATE OR REPLACE FUNCTION public.ams_get_states(
                clsname character varying,
                startdate date,
                enddate date,
                publish_date date,
                land_use_ids integer[],
                biomes character varying[],
                municipality_group_name character varying,
	        geocodes character varying[],
                riskThreshold float,
                isAuthenticated boolean DEFAULT False                
            )
            RETURNS TABLE(suid integer, name character varying, geometry geometry, classname character varying, date date, percentage double precision, area double precision, counts bigint) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
                    DECLARE
                        effective_publish_date date;

                    BEGIN

                        IF isAuthenticated THEN
                            effective_publish_date := CURRENT_DATE;
                        ELSE
                            SELECT MAX(dpd.date)
                            INTO effective_publish_date
                            FROM deter.deter_publish_date dpd
                            WHERE ('ALL' = ANY(biomes) OR dpd.biome = ANY(biomes));
                        END IF;

                        RETURN QUERY
                        SELECT
                            sta.suid AS suid, 
                            sta.name AS name, 
                            sta.geometry AS geometry, 
                            slu_j.classname AS classname, 
                            slu_j.date AS date, 
                            COALESCE(slu_j.perc, 0) AS percentage, 
                            COALESCE(slu_j.total, 0) AS area, 
                            COALESCE(slu_j.counts, 0) AS counts
                        FROM public."states" sta
                        INNER JOIN (
                            SELECT slu.suid, 
                                   slu.classname, 
                                   MAX(slu.date) AS date, 
                                   SUM(slu.percentage) AS perc, 
                                   SUM(slu.area) AS total, 
                                   SUM(slu.counts) AS counts
                            FROM public."states_land_use" slu
                            WHERE (slu.date <= effective_publish_date OR clsname IN ('AF', 'RK'))
                                AND slu.land_use_id = ANY (land_use_ids)
                                AND slu.classname = clsname
                                AND slu.date > enddate
                                AND slu.date <= startdate
                                AND slu.risk >= riskThreshold
                                AND ('ALL' = ANY (biomes) OR slu.biome = ANY (biomes))
                                AND (municipality_group_name = 'ALL' OR slu.geocode =
                                    ANY(
                                        SELECT geocode
                                        FROM public.municipalities_group_members mgm
                                        WHERE mgm.group_id = (
                                              SELECT mg.id
                                              FROM public.municipalities_group mg
                                              WHERE mg.name=municipality_group_name
                                        )
                                   )
                                   OR slu.geocode = ANY(geocodes)
                               )
                            GROUP BY slu.suid, slu.classname
                        ) AS slu_j
                        ON sta.suid = slu_j.suid
                        ORDER BY COALESCE(slu_j.perc, 0) DESC;
                    END;

        $BODY$;
    """

    db.execute(sql=sql)


def create_cell_table(db: DatabaseFacade, cell: str, force_recreate: bool):
    """Create the cs_{25|50}km and cs_{25|50}km_biome tables."""
    assert is_valid_cell(cell=cell)

    if force_recreate:
        db.drop_table(f"public.cs_{cell}_biome")
        db.drop_table(f"public.cs_{cell}")

    schema = "public"
    name = f"cs_{cell}"
    columns = [
        "suid serial NOT NULL PRIMARY KEY",
        "id varchar(10) UNIQUE",
        "col int4",
        "row int4",
        "area double precision",
        "geometry geometry(Polygon, 4674)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["id:btree", "geometry:gist"],
        force_recreate=force_recreate,
    )

    name = f"cs_{cell}_biome"
    columns = [
        "bid serial NOT NULL PRIMARY KEY",
        "id varchar(10)",
        "biome varchar(254)",
        f"FOREIGN KEY (id) REFERENCES public.cs_{cell} (id)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["id:btree", "biome:btree"],
        force_recreate=force_recreate,
    )


def create_cell_function(db: DatabaseFacade, cell: str, force_recreate: bool):
    """Create the get_cs_{25|50}km_function."""
    assert is_valid_cell(cell=cell)

    if force_recreate:
        sql = f"""
            DROP FUNCTION IF EXISTS public.ams_get_cs_{cell}(
                character varying, date, date, date, integer[], character varying[]
            );
        """
        db.execute(sql=sql)

    sql = f"""
        CREATE OR REPLACE FUNCTION public.ams_get_cs_{cell}(
                clsname character varying,
                startdate date,
                enddate date,
                publish_date date,
                land_use_ids integer[],
                biomes character varying[],
                municipality_group_name character varying,
	        geocodes character varying[],
                riskThreshold float,
                isAuthenticated boolean DEFAULT False
        )
            RETURNS TABLE(suid integer, name character varying, geometry geometry, classname character varying, date date, percentage double precision, area double precision, counts bigint) 
            LANGUAGE 'plpgsql'
            COST 100
            VOLATILE PARALLEL UNSAFE
            ROWS 1000

        AS $BODY$
                        DECLARE
                            effective_publish_date date;

                        BEGIN
                                IF isAuthenticated THEN
                                    effective_publish_date := CURRENT_DATE;
                                ELSE
                                    SELECT MAX(dpd.date)
                                    INTO effective_publish_date
                                    FROM deter.deter_publish_date dpd
                                    WHERE ('ALL' = ANY(biomes) OR dpd.biome = ANY(biomes));
                                END IF;                        

                                RETURN QUERY
                                SELECT
                                        cel.suid AS suid, 
                                        cel.id AS name, 
                                        cel.geometry AS geometry, 
                                        cls_j.classname AS classname, 
                                        cls_j.date AS date, 
                                        COALESCE(cls_j.perc, 0) AS percentage, 
                                        COALESCE(cls_j.total, 0) AS area, 
                                        COALESCE(cls_j.counts, 0) AS counts
                                FROM public."cs_{cell}" cel
                                LEFT JOIN (
                                        SELECT cls.suid, 
                                               cls.classname, 
                                               MAX(cls.date) AS date, 
                                               SUM(cls.percentage) AS perc, 
                                               SUM(cls.area) AS total, 
                                               SUM(cls.counts) AS counts
                                        FROM public."cs_{cell}_land_use" cls
                                        WHERE (cls.date <= effective_publish_date OR clsname IN ('AF', 'RK'))
                                            AND cls.land_use_id = ANY (land_use_ids)
                                            AND cls.classname = clsname
                                            AND cls.date > enddate
                                            AND cls.date <= startdate
                                            AND cls.risk >= riskThreshold
                                            AND ('ALL' = ANY (biomes) OR cls.biome = ANY (biomes))
                                            AND (municipality_group_name = 'ALL' OR cls.geocode =
                                                ANY(
                                                    SELECT geocode
                                                    FROM public.municipalities_group_members mgm
                                                    WHERE mgm.group_id = (
                                                        SELECT mg.id
                                                        FROM public.municipalities_group mg
                                                        WHERE mg.name=municipality_group_name
                                                    )
                                                )
                                                OR cls.geocode = ANY(geocodes)   
                                            )

                                        GROUP BY cls.suid, cls.classname
                                ) AS cls_j
                                ON cel.suid = cls_j.suid
                                ORDER BY COALESCE(cls_j.perc, 0) DESC;
                        END;
        
        $BODY$;
    """

    db.execute(sql=sql)


def create_active_fires_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the fires.active_fires table."""
    columns = [
        "id int4 NOT NULL",
        "uuid character varying(254)",
        "biome varchar(254)",
        "view_date date",
        "satelite varchar(254)",
        "estado varchar(254)",
        "municipio varchar(254)",
        "geom geometry(Point, 4674)",
        "geocode varchar(80)",
        "PRIMARY KEY (id, biome)",
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
        "gid varchar(254) NOT NULL",
        "biome varchar(254)",
        "origin_gid int4",
        "classname varchar(254)",
        "quadrant varchar(5)",
        "orbitpoint varchar(10)",
        "date date",
        "sensor varchar(10)",
        "satellite varchar(13)",
        "areatotalkm double precision",
        "areamunkm double precision",
        "areauckm double precision",
        "mun varchar(254)",
        "uf varchar(2)",
        "uc varchar(254)",
        "geom geometry(MultiPolygon, 4674)",
        "month_year varchar(10)",
        "geocode varchar(80)",
        "PRIMARY KEY (gid, biome)",
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


def _create_tmp_data_table(db: DatabaseFacade, force_recreate: bool):
    """Create the deter.tmp_data table."""
    columns = [
        "gid varchar(254) NOT NULL",
        "biome varchar(254)",
        "classname varchar(254)",
        "date date",
        "areamunkm double precision",
        "geom geometry(MultiPolygon, 4674)",
        "geocode varchar(80)",
        "PRIMARY KEY (gid, biome)",
    ]

    schema = "deter"
    name = "tmp_data"

    db.create_table(
        schema=schema,
        name="tmp_data",
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
    # deter, deter_auth, deter_history
    names = ("deter", "deter_auth", "deter_history")
    for name in names:
        _create_deter_table(db=db, name=name, force_recreate=force_recreate)

    # deter_publish_date
    db.create_table(
        schema="deter",
        name="deter_publish_date",
        columns=[
            "date date",
            "biome varchar(254)",
        ],
        force_recreate=force_recreate,
    )

    # tmp_data
    _create_tmp_data_table(db=db, force_recreate=force_recreate)


def create_spatial_units_table(db: DatabaseFacade, force_recreate: bool = False):
    """Create the public.spatial_units table."""
    schema = "public"

    # spatial_units table
    if force_recreate:
        db.drop_table("public.spatial_units_subsets")
        db.drop_table("public.spatial_units")

    name = "spatial_units"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "dataname varchar NOT NULL UNIQUE",
            "as_attribute_name varchar NOT NULL",
            "center_lat double precision NOT NULL",
            "center_lng double precision NOT NULL",
            "description varchar NOT NULL",
        ],
        force_recreate=force_recreate,
    )

    table = f"{schema}.{name}"

    sql = f"""
        INSERT INTO
            {table} (id, dataname, as_attribute_name, center_lat, center_lng, description)
        VALUES
            (1, 'cs_150km', 'id', -5.491382969006503, -58.467185764253415, 'Célula 150x150 km²'),
            (2, 'cs_25km', 'id', -5.510617783522636, -58.397927203480116, 'Célula 25x25 km²'),
            (3, 'states', 'name', -6.384962796500002, -58.97111531179317, 'Estado'),
            (4, 'municipalities', 'name', -6.384962796413522, -58.97111531172743, 'Município'),
            (5, 'cs_5km', 'id', -5.510617783522636, -58.397927203480116, 'Célula 5x5 km²');
    """

    db.execute(sql)

    # spatial_units_subsets table
    name = "spatial_units_subsets"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "spatial_unit_id int4 NOT NULL",
            "subset varchar(80) NOT NULL",
            "FOREIGN KEY (spatial_unit_id) REFERENCES public.spatial_units (id)",
        ],
        force_recreate=force_recreate,
    )

    table = f"{schema}.{name}"

    sql = f"""
        INSERT INTO
            {table} (spatial_unit_id, subset)
        VALUES
            (1, 'Bioma'),
            (2, 'Bioma'),
            (3, 'Bioma'),
            (4, 'Bioma'),
            (1, 'Municípios'),
            (2, 'Municípios'),
            (4, 'Municípios'),
            (5, 'Municípios');
    """

    db.execute(sql)


def create_biome_tables(db: DatabaseFacade, force_recreate: bool = False):
    """Create the public.biome and public.biome_border tables."""
    schema = "public"

    if force_recreate:
        db.drop_table(f"{schema}.biome_border")
        db.drop_table(f"{schema}.biome")

    name = "biome"

    columns = [
        "id serial PRIMARY KEY",
        "biome varchar(254) UNIQUE",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )

    name = "biome_border"

    columns = [
        "id serial PRIMARY KEY",
        "biome varchar(254)",
        "area_km double precision",
        "geom geometry(MultiPolygon, 4674)",
        "FOREIGN KEY (biome) REFERENCES public.biome (biome)",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )

    columns = [
        "geom:gist",
    ]

    db.create_indexes(
        schema=schema, name=name, columns=columns, force_recreate=force_recreate
    )


def create_class_tables(db: DatabaseFacade, force_recreate: bool):
    """Create the public.class and public.class_group tables."""
    schema = "public"

    if force_recreate:
        db.drop_table(table=f"{schema}.class")
        db.drop_table(table=f"{schema}.class_group")

    name = "class_group"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "name varchar NOT NULL UNIQUE",
            "title varchar NOT NULL",
            "orderby int4",
        ],
        force_recreate=False,
    )

    sql = f"""
        INSERT INTO
            {schema}.{name} (id, name, title, orderby)
        VALUES
            (1, 'DS', 'DETER Desmatamento', 0),
            (2, 'DG', 'DETER Degradação', 1),
            (3, 'CS', 'DETER Corte seletivo', 2),
            (4, 'MN', 'DETER Mineração', 3),
            (5, 'AF', 'Focos (Programa Queimadas)', 4),
            (6, 'RK', 'Risco de desmatamento (IBAMA)', 5);
    """

    db.execute(sql=sql)

    name = "class"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "name varchar NOT NULL",
            "group_id int4",
            "biome varchar(254)",
            "UNIQUE (name, biome)",
            f"FOREIGN KEY (group_id) REFERENCES {schema}.class_group (id)",
        ],
        force_recreate=False,
    )

    sql = f"""
        INSERT INTO
            {schema}.{name} (id, name, group_id, biome)
            VALUES
                (1, 'DESMATAMENTO_CR', 1, 'Amazônia'),
                (2, 'DESMATAMENTO_VEG', 1, 'Amazônia'),
                (3, 'CICATRIZ_DE_QUEIMADA', 2, 'Amazônia'),
                (4, 'DEGRADACAO', 2, 'Amazônia'),
                (5, 'CS_DESORDENADO', 3, 'Amazônia'),
                (6, 'CS_GEOMETRICO', 3, 'Amazônia'),
                (7, 'MINERACAO', 4, 'Amazônia'),
                (8, 'FOCOS', 5, 'Amazônia'),
                (9, 'RISCO', 6, 'Amazônia'),
                (10, 'DESMATAMENTO_CR', 1, 'Cerrado'),
                (11, 'FOCOS', 5, 'Cerrado');
    """

    db.execute(sql=sql)


def create_land_use_table(
    db: DatabaseFacade, land_use_type=str, force_recreate: bool = False
):
    """Create the public.land_use_ams table."""
    assert is_valid_land_use_type(land_use_type=land_use_type)
    land_use_type_suffix = "" if land_use_type == AMS else f"_{land_use_type}"

    schema = "public"

    name = f"land_use{land_use_type_suffix}"

    columns = [
        "id serial PRIMARY KEY",
        "name varchar(64)",
        "priority INT4",
    ]

    db.create_table(
        schema=schema,
        name=name,
        columns=columns,
        force_recreate=force_recreate,
    )

    table_exists = db.table_exist(schema=schema, table=name)

    if table_exists and not force_recreate:
        return

    land_use_categories = (
        [
            "Terra indígena",
            "Unidade de conservação de proteção integral",
            "Unidade de conservação de uso sustentável (sem APA)",
            "Território quilombola",
            "Assentamento rural",
            "Área de proteção ambiental",
            "Propriedade privada (Dados do SIGEF)",
            "Floresta pública não destinada",
            "Área sem registro fundiário",
        ]
        if land_use_type == AMS
        else [
            "Terra indígena",
            "Unidade de conservação",
            "Território quilombola",
            "Assentamento rural",
            "Área de proteção ambiental",
            "Floresta pública não destinada",
            "CAR sobreposto em terra indígena",
            "CAR sobreposto em unidade de conservação",
            "CAR sobreposto em território quilombola",
            "CAR sobreposto em assentamento rural",
            "CAR sobreposto em área de proteção ambiental",
            "CAR sobreposto em floresta pública não destinada",
            "Propriedade privada (Dados do CAR)",
            "Área sem registro fundiário",
        ]
    )

    values = [
        f"({index+1}, '{value}', {index})"
        for index, value in enumerate(land_use_categories)
    ]

    sql = f"""
        INSERT INTO
            {schema}.{name} (id, name, priority)
        VALUES {",".join(values)};
        """

    db.execute(sql=sql)


def create_municipalities_group_tables(
    db: DatabaseFacade, force_recreate: bool = False
):
    """Create the public.municipalities_group and public.municipalities_group_members tables."""
    logger.info("creating the municipalities group tables.")

    schema = "public"

    if force_recreate:
        db.drop_table(f"{schema}.municipalities_group_members")
        db.drop_table(f"{schema}.municipalities_group")

    name = "municipalities_group"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "name varchar(150) UNIQUE",
        ],
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["name:btree"],
        force_recreate=force_recreate,
    )

    name = "municipalities_group_members"

    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "group_id int4 NOT NULL",
            "geocode varchar(80)",
            "FOREIGN KEY (group_id) REFERENCES public.municipalities_group (id)",
            "UNIQUE (group_id, geocode)",
        ],
        force_recreate=force_recreate,
    )

    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geocode:btree", "group_id:btree"],
        force_recreate=force_recreate,
    )


def create_risk_tables(db: DatabaseFacade, force_recreate: bool):
    """Create the risk tables."""
    schema = "risk"

    db.create_schema(name=schema, force_recreate=False)

    if force_recreate:
        db.drop_table(f"{schema}.weekly_data", cascade=True)

    name = "etl_log_ibama"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "file_name varchar",
            "process_status int4",
            "process_message varchar",
            "file_date date",
            "is_new boolean DEFAULT true",
            "created_at timestamp with time zone NOT NULL DEFAULT now()",
            "processed_at timestamp with time zone",
        ],
        force_recreate=force_recreate,
    )

    name = "matrix_ibama_1km"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "geom geometry(Point, 4674)",
        ],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geom:gist"],
        force_recreate=force_recreate,
    )

    name = "weekly_ibama_tmp"
    db.create_table(
        schema=schema,
        name=name,
        columns=["geometry geometry(Point, 4674)", "data real"],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=["geometry:gist"],
        force_recreate=force_recreate,
    )

    name = "risk_ibama_date"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "file_name varchar UNIQUE",
            "expiration_date date",
            "created_at date NOT NULL DEFAULT now()",
            "risk_date date",
        ],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=["file_name:btree"],
        force_recreate=force_recreate,
    )

    name = "weekly_data"
    db.create_table(
        schema=schema,
        name=name,
        columns=[
            "id serial NOT NULL PRIMARY KEY",
            "date_id int4",
            "geom_id int4",
            "risk double precision",
            "biome varchar(254)",
            "geocode varchar(80)",
            "FOREIGN KEY (date_id) REFERENCES risk.risk_ibama_date (id)",
            "FOREIGN KEY (geom_id) REFERENCES risk.matrix_ibama_1km (id)",
        ],
        force_recreate=force_recreate,
    )
    db.create_indexes(
        schema=schema,
        name=name,
        columns=[
            "date_id:btree",
            "biome:btree",
            "geocode:btree",
        ],
        force_recreate=force_recreate,
    )
