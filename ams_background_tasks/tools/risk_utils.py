"""risk utilities module."""

from __future__ import annotations

import sys
from datetime import datetime

from ams_background_tasks.database_utils import DatabaseFacade
from ams_background_tasks.log import get_logger

logger = get_logger(__name__, sys.stdout)


# risk asset
RISK_ASSET_NAME = "XGBOOST25K"

RISK_SRC_IBAMA = "ibama"
RISK_SRC_INPE = "inpe"
RISK_SRCS = [RISK_SRC_IBAMA, RISK_SRC_INPE]


def get_last_risk_file_info(db: DatabaseFacade, src: str, is_new: bool = None):
    """Return the information about the last downloaded risk file (path and date)."""

    _filter = ""
    if is_new is not None:
        _filter = f" AND is_new={is_new}"

    sql = f"""
        SELECT file_name, file_date FROM risk.etl_log_risk
        WHERE process_status=1 AND source='{src}' {_filter}
        ORDER BY created_at DESC LIMIT 1;
    """

    res = db.fetchone(query=sql)

    if not res:
        return None, None

    return res[0], res[1]


def get_risk_date_id(db: DatabaseFacade, file_name: str):
    """Return the date_if of the risk file."""

    sql = f"""
        SELECT id FROM risk.risk_image_date
        WHERE file_name='{file_name}';
    """

    return db.fetchone(query=sql)


def mark_risk_file_as_used(db: DatabaseFacade, file_name: str):
    """Mark the risk file as used (is_new=False)"""
    sql = f"""
        UPDATE risk.etl_log_risk
        SET is_new=False, processed_at=now()
        WHERE file_name='{file_name}'
    """

    db.execute(sql)


def write_log(
    *,
    db: DatabaseFacade,
    msg: str,
    status: int,
    file_date: datetime,
    file_name: str,
    is_new: bool,
    src: str,
):
    """Write log to database."""
    assert not src or src in RISK_SRCS

    dt = (
        file_date.strftime("%Y-%m-%d")
        if file_date is not None
        else datetime.now().strftime("%Y-%m-%d")
    )

    msg = msg.replace("'", '"')

    sql = f"""
        INSERT INTO risk.etl_log_risk (file_name, process_status, process_message, file_date, is_new, source)
        VALUES('{file_name}', {status}, '{msg}', '{dt}', {is_new}, '{src}');
    """

    db.execute(sql)


def write_expiration_date(
    db: DatabaseFacade,
    status: int,
    file_date: datetime,
    file_expiration_date: datetime,
    file_name: str,
    src: str,
):
    """Write an expiration date only if has new risk data."""
    assert status == 1
    assert file_date
    assert file_expiration_date

    dt = file_expiration_date.strftime("%Y-%m-%d")

    sql = f"""
        INSERT INTO risk.risk_image_date (expiration_date,risk_date, file_name, source)
        VALUES('{dt}','{file_date}', '{file_name}', '{src}');
    """

    db.execute(sql)
