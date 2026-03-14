import logging
import os
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))


class ISTFormatter(logging.Formatter):
    """Format all log timestamps in IST (UTC+5:30)."""

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=IST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%d/%m/%y %H:%M:%S")


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = ISTFormatter(
        "%(asctime)s IST | %(name)s | %(levelname)s | %(message)s",
        datefmt="%d/%m/%y %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler("logs/pipeline.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def now_ist():
    """Get current time in IST. Use this everywhere instead of datetime.now()."""
    return datetime.now(IST)
