import pathlib

import appdirs

from opencefadb import __version__

USER_DATA_DIR = pathlib.Path(appdirs.user_data_dir('opencefadb', version=__version__))
DB_DATA_DIR = pathlib.Path(appdirs.user_data_dir('opencefadb'))

USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_DATA_DIR.mkdir(parents=True, exist_ok=True)

GLOBAL_PACKAGE_DIR = DB_DATA_DIR
