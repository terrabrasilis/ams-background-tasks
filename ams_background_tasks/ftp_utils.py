"""Database utilities."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from aioftp import Client
from pydantic import BaseModel

from ams_background_tasks.log import get_logger
from ams_background_tasks.utils import get_connection_components

logger = get_logger(__name__, sys.stdout)


class FtpFacade(BaseModel):
    """Ftp facade."""

    user: str
    password: str
    host: str
    port: str
    path: str
    _client: Optional[Client] = None

    @classmethod
    def from_url(cls, ftp_url: str) -> "FtpFacade":
        user, password, host, port, path = get_connection_components(url=ftp_url)
        return cls(
            user=user,
            password=password,
            host=host,
            port=port,
            path=path,
        )

    @property
    def ftp_url(self):
        """Database url."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.path}"

    async def list(self, recursive: bool = False):
        try:
            client = Client()
            await client.connect(self.host, int(self.port))
            await client.login(self.user, self.password)

            files = []
            for file_path, file_info in await client.list(
                path=self.path, recursive=recursive
            ):
                files.append(
                    {
                        "path": file_path,
                        "size": int(file_info["size"]),
                        "date": datetime(
                            year=int(file_info["modify"][0:4]),
                            month=int(file_info["modify"][4:6]),
                            day=int(file_info["modify"][6:8]),
                        ).date(),
                    }
                )

            return files

        except Exception as e:  # pylint: disable=broad-except
            return False, f"Error while listing files on the ftp server ({e})."

    async def download(self, src_file: Path, dst_file: Path):
        """Download a file from the ftp server."""
        try:
            print(f"Downloading file {src_file}")
            client = Client()
            await client.connect(self.host, int(self.port))
            await client.login(self.user, self.password)
            await client.download(src_file, dst_file, write_into=True)
            return True, f"File {src_file} downloaded successfully."

        except Exception as e:  # pylint: disable=broad-except
            return False, f"Error while downloading file from ftp server ({e})"
