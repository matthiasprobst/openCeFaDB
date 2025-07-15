import json
import logging
import pathlib
import sqlite3

import rdflib
from gldb import RawDataStore
from gldb.query import RawDataStoreQuery, QueryResult

from opencefadb.database.stores.filedb.database_resource import DatabaseResource
from opencefadb.ontologies import dcat

logger = logging.getLogger("opencefadb")


class SQLQuery(RawDataStoreQuery):

    def __init__(self, sql_query: str, filters=None):
        self.sql_query = sql_query
        self.filters = filters

    def execute(self, *args, **kwargs) -> QueryResult:
        pass


class HDF5SqlDB(RawDataStore):
    """
    HDF5SQLDB is a SQL database interface that stores data in HDF5 files.
    """

    def __init__(self):
        from ....configuration import get_config
        cfg = get_config()
        self._hdf5_file_table_name = "hdf5_files"
        self._sql_base_uri = "http://local.org/sqlite3/"
        self._db_path = str((cfg.rawdata_directory / "hdf5sql.db").resolve().absolute()).replace('\\', '/')
        self._endpointURL = rf"file://{self._db_path}"
        self._connection = self._initialize_database(self._db_path)
        self._filenames = {}
        self._expected_file_extensions = {".hdf", ".hdf5", ".h5"}

    def __repr__(self):
        return f"<{self.__class__.__name__} (Endpoint URL={self._endpointURL})>"

    def upload_file(self, filename) -> DatabaseResource:
        _id = self._insert_hdf5_reference(self._connection, filename)
        return DatabaseResource(_id, metadata=self.generate_mapping_dataset(str(_id)))

    def execute_query(self, query: SQLQuery) -> QueryResult:
        cursor = self._connection.cursor()
        params = []

        if query.filters:
            for key, value in query.filters.items():
                query += f" AND {key} = ?"
                params.append(value)

        cursor.execute(query.sql_query, params)
        return cursor.fetchall()

    def _initialize_database(self, db_path="hdf5_files.db"):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {self._hdf5_file_table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            metadata TEXT
        )
        """)
        conn.commit()
        return conn

    def reset(self):
        logger.info(f"Resetting the database. Dropping table {self._hdf5_file_table_name}.")
        cursor = self._connection.cursor()
        cursor.execute(f"DROP TABLE {self._hdf5_file_table_name}")
        self._initialize_database(self._db_path)

    @classmethod
    def _insert_hdf5_reference(cls, conn, filename, metadata=None):
        filename = pathlib.Path(filename).resolve().absolute()
        cursor = conn.cursor()
        metadata_dump = json.dumps(metadata) or '{}'
        try:
            cursor.execute("""
            INSERT INTO hdf5_files (file_path, metadata)
            VALUES (?, ?)
            """, (str(filename), metadata_dump))
            conn.commit()
            generated_id = cursor.lastrowid
            logger.debug(f"File {filename} inserted successfully.")
            return generated_id
        except sqlite3.IntegrityError:
            logger.error(f"File {filename} already exists.")

    def __del__(self):
        """Ensure the connection is closed when the object is deleted."""
        if self._connection:
            self._connection.close()

    def generate_data_service_serving_a_dataset(self, dataset_identifier: str):
        endpoint_url = self._endpointURL
        endpoint_url_file_name = endpoint_url.rsplit('/', 1)[-1]
        dataservice_id = f"{self._sql_base_uri}{endpoint_url_file_name}"
        data_service = dcat.DataService(
            id=dataservice_id,
            title="sqlite3 Database",
            endpointURL=endpoint_url,
            servesDataset=dcat.Dataset(
                id=f"{self._sql_base_uri}table/{self._hdf5_file_table_name}",
                title=f"SQL Table '{self._hdf5_file_table_name}'",
                identifier=self._hdf5_file_table_name,
                distribution=dcat.Distribution(
                    id=f"{self._sql_base_uri}12345",
                    identifier=dataset_identifier,
                    mediaType="application/vnd.sqlite3",
                )
            )
        )
        return data_service

    def generate_mapping_dataset(self, dataset_identifier: str):
        data_service = self.generate_data_service_serving_a_dataset(dataset_identifier)
        return rdflib.Graph().parse(data=data_service.model_dump_jsonld(), format="json-ld")
