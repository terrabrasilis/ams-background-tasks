"""Logger module."""

import logging
from typing import Any

PACKAGE_LOGGER_PREFIX = "ams_background_tasks"


def get_handler(textio: Any):
    handler = logging.StreamHandler(textio)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    return handler


def get_logger(name: str, textio: Any, level: int = logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not any(
        isinstance(handler, logging.StreamHandler) and handler.stream is textio
        for handler in logger.handlers
    ):
        logger.addHandler(get_handler(textio=textio))
    return logger


def set_package_log_level(level: int) -> None:
    """Set the level for every already-created logger in this package."""
    root_logger = logging.getLogger(PACKAGE_LOGGER_PREFIX)
    root_logger.setLevel(level)

    for name, logger in logging.Logger.manager.loggerDict.items():
        if isinstance(logger, logging.Logger) and (
            name == PACKAGE_LOGGER_PREFIX
            or name.startswith(f"{PACKAGE_LOGGER_PREFIX}.")
        ):
            logger.setLevel(level)
