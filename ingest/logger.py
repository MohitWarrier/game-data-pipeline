import logging
import os


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    os.makedirs("logs", exist_ok=True)
    file_handler = logging.FileHandler("logs/pipeline.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
