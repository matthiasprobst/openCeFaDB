"""Open Centrifugal Fan Database.

OpenCeFaDB is a database for centrifugal fan data. It is designed to store and retrieve data from different sources
and provide a common interface to access the data. The database is based on the Generic Linked Database (GLDB)
framework and uses a combination of SQL or NoSQL databases to store large amounts of raw data and RDF databases
to store the metadata.

Users can choose which specific database to use. There are some predefined configurations available, but users can
define their own database interfaces, leaving room for integration with their own workflows and databases.
"""
import logging
import pathlib
from logging.handlers import RotatingFileHandler
from typing import Union

from .database import con

import appdirs

__this_dir__ = pathlib.Path(__file__).parent

from ._version import __version__

USER_LOG_DIR = pathlib.Path(appdirs.user_log_dir('opencefadb', version=__version__))
USER_DATA_DIR = pathlib.Path(appdirs.user_data_dir('opencefadb', version=__version__))
DB_DATA_DIR = pathlib.Path(appdirs.user_data_dir('opencefadb'))
USER_LOG_DIR.mkdir(parents=True, exist_ok=True)
USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_DATA_DIR.mkdir(parents=True, exist_ok=True)

paths = {
    "setup": DB_DATA_DIR / 'setup.ini',
    "config": DB_DATA_DIR / 'opencefadb-config.ini',
    "global_package_dir": DB_DATA_DIR,
}

DEFAULT_LOGGING_LEVEL = logging.INFO
_formatter = logging.Formatter(
    '%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d_%H:%M:%S')

_stream_handler = logging.StreamHandler()
_stream_handler.setLevel(DEFAULT_LOGGING_LEVEL)
_stream_handler.setFormatter(_formatter)

_file_handler = RotatingFileHandler(USER_LOG_DIR / 'opencefadb.log')
_file_handler.setLevel(logging.DEBUG)  # log everything to file!
_file_handler.setFormatter(_formatter)

logger = logging.getLogger("opencefadb")
logger.setLevel(DEFAULT_LOGGING_LEVEL)
logger.addHandler(_stream_handler)
logger.addHandler(_file_handler)


def set_logging_level(level: Union[int, str]):
    """Set the log level."""
    _logger = logging.getLogger("opencefadb")
    _logger.setLevel(level)
    for handler in _logger.handlers:
        handler.setLevel(DEFAULT_LOGGING_LEVEL)

    return _logger.level


GRAPH_DB_CONFIG_FILENAME = __this_dir__ / "../data/graphdb-config.ttl"

__all__ = ['set_logging_level']
