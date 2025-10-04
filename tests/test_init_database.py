import pathlib
import shutil
import unittest

import dotenv
from gldb.query import RemoteSparqlQuery
from gldb.stores import GraphDB

import opencefadb

__this_dir__ = pathlib.Path(__file__).parent

from opencefadb.stores import RDFFileStore, HDF5SqlDB


class TestInitDatabase(unittest.TestCase):

    def setUp(self):
        dotenv.load_dotenv(__this_dir__ / ".env", override=True)
        self.working_dir = pathlib.Path(__this_dir__ / "local-db")

    # def tearDown(self):
    #     if self.working_dir.exists():
    #         shutil.rmtree(self.working_dir)

    def test_db_with_rdflib(self):
        local_db = opencefadb.OpenCeFaDB(
            metadata_store=RDFFileStore(data_dir=self.working_dir / "metadata"),
            hdf_store=HDF5SqlDB(),
            working_directory=self.working_dir,
            config_filename=__this_dir__ / "../opencefadb" / "db-dataset-config-sandbox.ttl"
        )
        metadata_dir = local_db.stores.rdf.data_dir
        metadata_ttl_filenames = metadata_dir.glob("*.ttl")
        metadata_jsonld_filenames = metadata_dir.glob("*.jsonld")
        metadata_filenames = list(metadata_ttl_filenames) + list(metadata_jsonld_filenames)
        self.assertGreaterEqual(len(metadata_filenames), 5)

        # get size of the graph:
        graph = local_db.stores.rdf.graph
        self.assertEqual(len(graph), 7752)

        # also the second time should work (exist_ok=True):
        local_db = opencefadb.OpenCeFaDB(
            metadata_store=RDFFileStore(data_dir="local-db/data/metadata"),
            hdf_store=HDF5SqlDB(),
            working_directory=self.working_dir,
            config_filename=__this_dir__ / "../opencefadb" / "db-dataset-config-sandbox.ttl"
        )
        graph = local_db.stores.rdf.graph
        self.assertEqual(len(graph), 7752)

    def test_graphdb(self):
        gdb = GraphDB(
            endpoint="http://localhost:7201",
            repository="OpenCeFaDB-Sandbox",
            username="admin",
            password="admin"
        )
        res = gdb.get_or_create_repository(__this_dir__ / "graphdb-config-sandbox.ttl")
        self.assertTrue(res)

        repo_info = gdb.get_repository_info("OpenCeFaDB-Sandbox")
        self.assertEqual(repo_info["id"], "OpenCeFaDB-Sandbox")

        res = RemoteSparqlQuery(
            "SELECT * WHERE { ?s ?p ?o }",
            description="Selects all triples in the RDF database"
        ).execute(gdb)
        count = len(res.data["results"]["bindings"])
        tripels = gdb.count_triples(key="total")
        self.assertEqual(count, tripels)

        res = gdb.delete_repository("OpenCeFaDB-Sandbox")
        self.assertTrue(res)
        #
        # with self.assertRaises(RuntimeError):
        #     gdb.get_repository_info("OpenCeFaDB-Sandbox")

        # gdb.delete_repository("OpenCeFaDB-Sandbox")
        # gdb.create_repository(
        #     config_path = __this_dir__ / "graphdb-config-sandbox.ttl"
        # )
        # R_SELECT_ALL = RemoteSparqlQuery(
        #     "SELECT * WHERE { ?s ?p ?o }",
        #     description="Selects all triples in the RDF database"
        # )
        # res = R_SELECT_ALL.execute(gdb)
        # local_db = opencefadb.OpenCeFaDB(
        #     metadata_store=GraphDB(data_dir="local-db/data/metadata"),
        #     hdf_store=HDF5SqlDB(),
        #     working_directory=self.working_dir,
        #     config_filename=__this_dir__ / "../opencefadb" / "db-dataset-config-sandbox.ttl"
        # )
