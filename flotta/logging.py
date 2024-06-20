from pathlib import Path

import logging
import logging.handlers
import os
import sys


def get_log_formatter() -> logging.Formatter:
    return logging.Formatter(
        fmt="%(asctime)s,%(msecs)03d %(levelname)8s %(name)48.48s:%(lineno)-3s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def get_logger(name: str) -> logging.Logger:
    # log level
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = logging.getLevelName(level_name)

    # logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # formatter
    log_formatter = get_log_formatter()

    # file handler
    path = Path("logs")
    os.makedirs(path, exist_ok=True)

    file_handler = logging.handlers.RotatingFileHandler(
        filename=path / "flotta.log",
        maxBytes=1024 * 1024 * 1024,  # 1GB
        backupCount=100,
    )
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(level)
    logger.addHandler(file_handler)

    # console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(level)
    logger.addHandler(console_handler)

    logger.propagate = False

    return logger
