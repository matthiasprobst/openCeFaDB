import pathlib
import sqlite3
import unittest

from pydantic import AnyUrl

from opencefadb import set_logging_level
from opencefadb.database.query_templates.sparql import SELECT_ALL
from opencefadb.database.stores.filedb.hdf5sqldb import HDF5SqlDB
from opencefadb.database.stores.rdf_stores.graphdb import GraphDBStore

set_logging_level('DEBUG')

__this_dir__ = pathlib.Path(__file__).parent


class TestStores(unittest.TestCase):

    def test_sql_store(self):
        sql_store = HDF5SqlDB()
        sql_row = sql_store.generate_data_service_serving_a_dataset("1")
        conn = sqlite3.connect(AnyUrl(sql_row.endpointURL).path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM hdf5_files")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        conn.close()

    def test_graphdb_store(self):
        """Make sure you configured your graphdb accordingly.
        """
        repoId = GraphDBStore.create(
            config_filename=__this_dir__ / "test-repo-config.ttl",
            host="http://localhost",
            port=7201
        )

        store = GraphDBStore(
            host="localhost",
            port=7201,
            user="admin",
            password="admin",
            repository=repoId
        )

        results = store.execute_query(
            SELECT_ALL
        )
        self.assertTrue(len(results.result) > 0)


        GraphDBStore.delete(repoId, host="localhost", port=7201, auth=("admin", "admin"))
