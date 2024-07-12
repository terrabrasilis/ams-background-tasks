"""A sample tool."""

from time import sleep

import click


@click.command("sample-tool")
def main():
    print("A sample tool")
    sleep(10)
    return 0
