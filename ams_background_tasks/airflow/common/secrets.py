from airflow.hooks.base import BaseHook  # type: ignore


def get_conn_secrets_uri(names: list):
    """Return a dict with airflow connection secrets uri."""
    env = {}
    for name in names:
        url = BaseHook.get_connection(name).get_uri()
        if "?__extra__=" not in url:
            env[name] = url
        else:
            env[name] = url.split("?__extra__=")[0]
    return env
