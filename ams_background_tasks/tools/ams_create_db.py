"""Create AMS database."""

import click
from psycopg2 import connect
from psycopg2.extensions import connection
from pydantic import BaseModel, ConfigDict


class AmsDbCreator(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    conn: connection = None

    def __init__(self, db_url: str):
        """Ctor."""
        BaseModel.__init__(self)
        self.conn = connect(db_url)
        print(db_url)

    def create_tables(self):
        """Create the database tables."""
        return

    def create_views(self):
        """Create the database views."""
        return


@click.command("create-db")
@click.argument(
    "db_url",
    required=True,
    type=str,
)
def main(db_url: str):
    """Create the AMS database.

    DB_URL: database url (postgresql://<username>:<password>@<host>/<database>).
    """
    AmsDbCreator(db_url=db_url)
