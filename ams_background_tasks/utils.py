"""Utilities."""

from urllib.parse import urlparse


def get_connection_components(url: str):
    parsed_url = urlparse(url)

    user = parsed_url.username
    password = parsed_url.password
    host = parsed_url.hostname
    port = parsed_url.port
    db_name = parsed_url.path[1:]
    return user, password, host, str(port), db_name
