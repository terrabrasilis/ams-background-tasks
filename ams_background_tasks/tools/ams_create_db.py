"""Create AMS database."""

from __future__ import annotations

import logging
from typing import Optional

import click
from psycopg2 import connect
from psycopg2.extensions import connection
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class AmsDbCreator(BaseModel):
    """AMS database creator class."""

    user: str
    password: str
    host: str
    port: str
    db_name: str
    force_recreate: Optional[bool] = False
    _conn: Optional[connection] = None

    @classmethod
    def from_url(cls, db_url: str, force_recreate: bool) -> "AmsDbCreator":
        tmp = db_url.split("//")[1].split("@")
        user, password = tmp[0].split(":")
        host, port, db_name = [tmp[1].split(":")[0]] + tmp[1].split(":")[1].split("/")
        return cls(
            user=user,
            password=password,
            host=host,
            port=port,
            db_name=db_name,
            force_recreate=force_recreate,
        )

    @property
    def db_url(self):
        """Database url."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"

    @property
    def conn(self):
        """Connect to database."""
        if self._conn is None:
            self._conn = connect(self.db_url)
            assert self._conn.status == 1
        return self._conn

    def create(self):
        """Create the database."""
        self.create_schemas()
        self.create_tables()

    def create_schemas(self):
        """Create all database schemas."""
        # public
        self.create_schema(name="public", user=self.user)

    def create_schema(self, name: str, user: str, comment: str = ""):
        """Create a schema."""
        sql = ""

        if self.force_recreate:
            sql += "DROP SCHEMA IF EXISTS {name};"

        sql += f"CREATE SCHEMA IF NOT EXISTS {name} AUTHORIZATION {user};"

        if comment:
            sql += f"COMMENT ON SCHEMA {name} IS {comment};"

        sql += f"GRANT ALL ON SCHEMA {name} TO {user};"

        logger.debug(sql)

        self.conn.cursor().execute(sql)

    def create_views(self):
        """Create the database views."""
        return

    def create_tables(self):
        """Create the database tables."""
        self.create_municipalities_table(schema="public", user=self.user)

    def create_municipalities_table(self, schema: str, user: str):
        """Create the municipalities table."""
        self.create_schema(name=schema, user=user)

        table = f"{schema}.municipalities"

        sql = ""

        if self.force_recreate:
            sql += "DROP TABLE IF EXISTS {table};"

        sql = f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                id integer,
                name character varying(150) COLLATE pg_catalog.default,
                geocode character varying(80) COLLATE pg_catalog.default,
                year integer,
                geometry geometry(MultiPolygon,4674)
            )
        """
        logger.debug(sql)

        self.conn.cursor().execute(sql)
        self.conn.commit()


@click.command("create-db")
@click.argument(
    "db_url",
    required=True,
    type=str,
)
@click.option(
    "--force-recreate",
    required=False,
    type=bool,
    default=False,
    help="Force to recreate the AMS database.",
)
def main(db_url: str, force_recreate: bool):
    """Create the AMS database.

    DB_URL: database url (postgresql://<username>:<password>@<host>:<port>/<database>).
    """
    db_creator = AmsDbCreator.from_url(db_url=db_url, force_recreate=force_recreate)
    db_creator.create()
