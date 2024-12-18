import sys
import logging
from pythonjsonlogger import jsonlogger
import colorlog


def get_logger(id: str, json=False):
    logger = logging.getLogger(id)

    stdout = colorlog.StreamHandler(stream=sys.stdout)

    if not json:
        fmt = colorlog.ColoredFormatter(
            "%(purple)s%(name)s: %(white)s%(asctime)s%(reset)s | %(log_color)s%(levelname)s%(reset)s | %(blue)s%(filename)s:%(lineno)s%(reset)s | %(process)d >>> %(log_color)s%(message)s%(reset)s"
        )
    else:
        fmt = jsonlogger.JsonFormatter(
            "%(name)s %(asctime)s %(levelname)s %(filename)s %(lineno)s %(process)d %(message)s",
            rename_fields={"levelname": "severity", "asctime": "timestamp"},
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )

    stdout.setFormatter(fmt)
    logger.addHandler(stdout)

    logger.setLevel(logging.WARNING)

    return logger
