"""Logger module."""

import logging
from typing import Any


def get_handler(textio: Any):
    handler = logging.StreamHandler(textio)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    return handler


def get_logger(name: str, textio: Any):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_handler(textio=textio))
    return logger
