"""Database utilities."""

from __future__ import annotations

import sys
from typing import Any, Optional
from urllib.parse import urlparse

from psycopg2 import connect
from psycopg2.extensions import connection
from pydantic import BaseModel

from ams_background_tasks.log import get_logger

logger = get_logger(__name__, sys.stdout)


def get_connection_components(db_url: str):
    parsed_url = urlparse(db_url)

    user = parsed_url.username
    password = parsed_url.password
    host = parsed_url.hostname
    port = parsed_url.port
    db_name = parsed_url.path[1:]
    return user, password, host, str(port), db_name


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
            self._conn = connect(
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                dbname=self.db_name,
            )
            assert self._conn.status == 1
        return self._conn

    def execute(self, sql: str, log: bool = True):
        """Execute a sql string."""
        if log:
            logger.debug(" ".join(sql.split()))

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
        self, *, schema: str, name: str, columns: list, force_recreate: bool = False
    ):
        """Create a database table."""
        table = f'{schema}."{name}"'

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
        *,
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
                name=f"{name}_{col.replace(',', '_')}_idx",
                table=name,
                method=method,
                column=col,
                force_recreate=force_recreate,
            )

    def fetchall(self, query):
        logger.debug(query.strip())

        cursor = self.conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        return data

    def fetchone(self, query):
        logger.debug(query.strip())

        cursor = self.conn.cursor()
        cursor.execute(query)
        data = cursor.fetchone()

        if data and len(data) == 1:
            data = data[0]

        cursor.close()
        return data

    def insert(self, query: str, data: Any):
        logger.debug(query.strip())

        cursor = self.conn.cursor()
        cursor.executemany(query, data)
        self.conn.commit()
        cursor.close()

    def truncate(self, table: str, cascade: bool = False, restart: bool = False):
        _cascade = "CASCADE" if cascade else ""
        _restart = "RESTART IDENTITY" if restart else ""
        sql = f"TRUNCATE {table} {_restart} {_cascade};"
        self.execute(sql=sql)

    def copy_table(self, src: str, dst: str):
        self.execute(f"INSERT INTO {dst} SELECT * FROM {src}")

    def drop_table(self, table: str, cascade: bool = False):
        _cascade = "CASCADE" if cascade else ""
        self.execute(f"DROP TABLE IF EXISTS {table} {_cascade};")

    def create_postgis_extension(self):
        self.execute("CREATE EXTENSION IF NOT EXISTS POSTGIS")

    def create_dblink_extension(self):
        self.execute("CREATE EXTENSION IF NOT EXISTS dblink")

    def count_rows(self, table: str, conditions: str = ""):
        where = ""
        if conditions:
            where = f"WHERE {conditions}"

        query = f"""
            SELECT COUNT(*)
            FROM {table}
            {where};
        """

        return self.fetchone(query=query)

    def table_exist(self, table: str, schema: str):
        query = f"""
            SELECT EXISTS (
	            SELECT 1 
	            FROM information_schema.tables 
	            WHERE table_schema = '{schema}' 
	            AND table_name = '{table}'
            );
        """
        return self.fetchone(query=query)
