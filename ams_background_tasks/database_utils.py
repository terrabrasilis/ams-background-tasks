"""Database utilities."""

from __future__ import annotations

import sys
from typing import Any, Optional

from psycopg2 import connect
from psycopg2.extensions import connection
from pydantic import BaseModel

from ams_background_tasks.log import get_logger

logger = get_logger(__name__, sys.stdout)


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

    def execute(self, sql: str, log: bool = True):
        """Execute a sql string."""
        if log:
            logger.debug(sql)

        self.conn.cursor().execute(sql)
        self.conn.commit()

    def create_schema(self, name: str, comment: str = "", force_recreate: bool = False):
        """Create a schema."""
        sql = ""

        if force_recreate:
            sql += f"DROP SCHEMA IF EXISTS {name} CASCADE;"

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

    def create_index(
        self,
        schema: str,
        name: str,
        table: str,
        method: str,
        column: str,
        force_recreate: bool = False,
    ):
        """Create an index."""
        sql = ""

        index = f"{schema}.{name}"
        table = f"{schema}.{table}"

        if force_recreate:
            sql += f"DROP INDEX IF EXISTS {index};"

        sql += f"""
            CREATE INDEX IF NOT EXISTS {name}
            ON {table} USING {method}
            ({column});
        """
        self.execute(sql)

    def create_indexes(
        self, schema: str, name: str, columns: tuple, force_recreate: bool
    ):
        """Create an index for each column."""
        for _ in columns:
            col, method = _.split(":")
            self.create_index(
                schema=schema,
                name=f"{name}_{col}_idx",
                table=name,
                method=method,
                column=col,
                force_recreate=force_recreate,
            )

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
        sql = f"TRUNCATE {table} CASCADE;"
        self.execute(sql=sql)

    def copy_table(self, src: str, dst: str):
        self.execute(f"INSERT INTO {dst} SELECT * FROM {src}")

    def drop_table(self, table: str):
        self.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

    def create_postgis_extension(self):
        self.execute("CREATE EXTENSION IF NOT EXISTS POSTGIS")
