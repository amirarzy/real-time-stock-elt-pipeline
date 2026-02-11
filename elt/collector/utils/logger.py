import logging
import os
from logging.handlers import RotatingFileHandler


def get_logger(name: str, log_path: str) -> logging.Logger:
    """
    Create a logger that logs to both stdout and a rotating file.
    """
    logger = logging.getLogger(name)

    # Prevent adding handlers multiple times if get_logger is called repeatedly
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # File handler with rotation
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    fh = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.propagate = False
    return logger
