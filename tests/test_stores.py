import pathlib
import shutil
import sqlite3
import unittest

import requests.exceptions
from gldb.query import SparqlQuery, QueryResult
from pydantic import AnyUrl

from opencefadb import set_logging_level
from opencefadb.database.dbinit import initialize_database
from opencefadb.database.stores.filedb.hdf5sqldb import HDF5SqlDB
from opencefadb.database.stores.rdf_stores.graphdb import GraphDBStore
from opencefadb.database.stores.rdf_stores.graphdb.store import LocalRDFStore

set_logging_level('DEBUG')

__this_dir__ = pathlib.Path(__file__).parent


class TestStores(unittest.TestCase):

    def setUp(self):
        self._test_download_dir = pathlib.Path(__this_dir__ / "test_download")
        self._test_download_dir.mkdir(exist_ok=True, parents=True)

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

    def test_local_rdflib_store(self):
        store = LocalRDFStore()
        for filename in (__this_dir__ / "test_measurements").glob("*.jsonld"):
            shutil.copy(filename, self._test_download_dir / filename.name)

        metadata_dir = initialize_database(self._test_download_dir)
        for filename in metadata_dir:
            store.upload_file(filename)

        for filename in self._test_download_dir.glob("*.jsonld"):
            store.upload_file(filename)

        sparql_query = SparqlQuery(query="SELECT * WHERE { ?s ?p ?o }")

        res = sparql_query.execute(store)
        self.assertIsInstance(res, QueryResult)
        self.assertEqual(res.query, sparql_query)
        self.assertEqual(20130, len(res))

    def test_graphdb_store(self):
        """Make sure you configured your graphdb accordingly.
        """
        config_filename = __this_dir__ / "test-repo-config.ttl"
        self.assertTrue(config_filename.exists(), f"Config file {config_filename} does not exist.")

        try:
            repoId = GraphDBStore.create(
                config_filename=__this_dir__ / "test-repo-config.ttl",
                host="http://localhost",
                port=7201
            )
        except requests.exceptions.ConnectionError as e:
            self.skipTest(f"Failed to create GraphDB repository: {e}")

        for filename in (__this_dir__ / "test_measurements").glob("*.jsonld"):
            shutil.copy(filename, self._test_download_dir / filename.name)

        store = GraphDBStore(
            host="localhost",
            port=7201,
            user="admin",
            password="admin",
            repository=repoId
        )

        store.reset(__this_dir__ / "test-repo-config.ttl")

        metadata_dir = initialize_database(self._test_download_dir)
        for filename in metadata_dir:
            store.upload_file(filename)
        for filename in self._test_download_dir.glob("*.jsonld"):
            store.upload_file(filename)

        sparql_query = SparqlQuery(query="SELECT * WHERE { ?s ?p ?o }")

        res = sparql_query.execute(store)
        self.assertIsInstance(res, QueryResult)
        self.assertEqual(res.query, sparql_query)
        self.assertEqual(31313, len(res))

        # results = store.execute_query(
        #     SELECT_ALL
        # )
        # self.assertTrue(len(results.result) > 0)
        #
        # GraphDBStore.delete(
        #     repoId,
        #     host="localhost",
        #     port=7201,
        #     auth=("admin", "admin")
        # )
