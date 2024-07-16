"""Create AMS database."""

from __future__ import annotations

import logging
from typing import Any, Optional

import click
from psycopg2 import connect
from psycopg2.extensions import connection
from pydantic import BaseModel

logger = logging.getLogger(__name__)


def get_connection_components(db_url: str):
    tmp = db_url.split("//")[1].split("@")
    user, password = tmp[0].split(":")
    host, port, db_name = [tmp[1].split(":")[0]] + tmp[1].split(":")[1].split("/")
    return user, password, host, port, db_name


class DatabaseFacade(BaseModel):
    """Database facade."""

    user: str
    password: str
    host: str
    port: str
    db_name: str
    _conn: Optional[connection] = None

    @classmethod
    def from_url(cls, db_url: str) -> "DatabaseFacade":
        user, password, host, port, db_name = get_connection_components(db_url=db_url)
        return cls(
            user=user,
            password=password,
            host=host,
            port=port,
            db_name=db_name,
        )

    @property
    def db_url(self):
        """Database url."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    @property
    def conn(self):
        """Database connection."""
        if self._conn is None:
            self._conn = connect(self.db_url)
            assert self._conn.status == 1
        return self._conn

    def execute(self, sql: str):
        """Execute a sql string."""
        logger.debug(sql)

        self.conn.cursor().execute(sql)
        self.conn.commit()

    def create_schema(self, name: str, comment: str = "", force_recreate: bool = False):
        """Create a schema."""
        sql = ""

        if force_recreate:
            sql += "DROP SCHEMA IF EXISTS {name};"

        sql += f"CREATE SCHEMA IF NOT EXISTS {name} AUTHORIZATION {self.user};"

        if comment:
            sql += f"COMMENT ON SCHEMA {name} IS {comment};"

        sql += f"GRANT ALL ON SCHEMA {name} TO {self.user};"

        self.execute(sql)

    def create_table(
        self, schema: str, name: str, columns: list, force_recreate: bool = False
    ):
        """Create a database table."""
        table = f"{schema}.{name}"

        sql = ""

        if force_recreate:
            sql += f"DROP TABLE IF EXISTS {table};"

        sql += f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                {", ".join(columns)}
            )
        """

        self.execute(sql)

    def fetchall(self, query):
        logger.debug(query)

        cursor = self.conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        return data

    def insert(self, query: str, data: Any):
        logger.debug(query)

        cursor = self.conn.cursor()
        cursor.executemany(query, data)
        self.conn.commit()
        cursor.close()

    def truncate(self, table: str):
        sql = f"TRUNCATE {table};"
        self.execute(sql=sql)


@click.command("create-db")
@click.argument(
    "db_url",
    required=True,
    type=str,
)
@click.argument(
    "aux_db_url",
    required=True,
    type=str,
)
@click.option(
    "--force-recreate",
    required=False,
    is_flag=True,
    default=False,
    help="Force to recreate the AMS database.",
)
def main(db_url: str, aux_db_url: str, force_recreate: bool):
    """Create the AMS database.

    DB_URL: AMS database url (postgresql://<username>:<password>@<host>:<port>/<database>).
    AUX_DB_URL: Auxiliary database url (postgresql://<username>:<password>@<host>:<port>/<database>).
    """
    db = DatabaseFacade.from_url(db_url=db_url)

    # municipalities
    aux_db = DatabaseFacade.from_url(db_url=aux_db_url)
    create_municipalities_table(db=db, schema="public", force_recreate=force_recreate)
    update_municipalities_table(schema="public", db=db, aux_db=aux_db)


def create_municipalities_table(
    db: DatabaseFacade, schema: str, force_recreate: bool = False
):
    """Create the municipalities table."""
    columns = [
        "id integer",
        "name character varying(150) COLLATE pg_catalog.default",
        "geocode character varying(80) COLLATE pg_catalog.default",
        "year integer",
        "geometry geometry(MultiPolygon, 4674)",
    ]

    db.create_table(
        schema=schema,
        name="municipalities",
        columns=columns,
        force_recreate=force_recreate,
    )


def update_municipalities_table(
    schema: str, db: DatabaseFacade, aux_db: DatabaseFacade
):
    """Update the municipalities table."""
    select_query = "SELECT id, nome, geocodigo, anoderefer, ST_AsText(geoms) from amazonia_legal.municipalities"
    data = aux_db.fetchall(query=select_query)

    table = f"{schema}.municipalities"

    db.truncate(table=table)

    insert_query = f"""
        INSERT INTO {table} (id, name, geocode, year, geometry)
        VALUES (%s, %s, %s, %s, ST_GeomFromText(%s, 4674))
    """
    db.insert(query=insert_query, data=data)
