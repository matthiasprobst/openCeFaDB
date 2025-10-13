import pathlib
import unittest

import dotenv
from gldb.query import RemoteSparqlQuery
from gldb.stores import GraphDB

import opencefadb
from opencefadb.query_templates.sparql import SELECT_FAN_PROPERTIES
from opencefadb.stores import RDFFileStore, HDF5SqlDB

__this_dir__ = pathlib.Path(__file__).parent


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
        self.assertEqual(len(graph), 22122)

        # also the second time should work (exist_ok=True):
        local_db = opencefadb.OpenCeFaDB(
            metadata_store=RDFFileStore(data_dir="local-db/data/metadata"),
            hdf_store=HDF5SqlDB(),
            working_directory=self.working_dir,
            config_filename=__this_dir__ / "../opencefadb" / "db-dataset-config-sandbox.ttl"
        )
        graph = local_db.stores.rdf.graph
        self.assertEqual(len(graph), 22249)

    @unittest.skip("Only test locally with a running GraphDB instance")
    def test_graphdb(self):
        gdb = GraphDB(
            endpoint="http://localhost:7201",
            repository="OpenCeFaDB-Sandbox",
            username="admin",
            password="admin"
        )
        gdb.delete_repository()
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

    def test_db_with_graphdb(self):
        gdb = GraphDB(
            endpoint="http://localhost:7201",
            repository="OpenCeFaDB-Sandbox",
            username="admin",
            password="admin"
        )
        # reset repository:
        if gdb.get_repository_info("OpenCeFaDB-Sandbox"):
            gdb.delete_repository("OpenCeFaDB-Sandbox")
        res = gdb.get_or_create_repository(__this_dir__ / "graphdb-config-sandbox.ttl")
        self.assertTrue(res)

        local_db = opencefadb.OpenCeFaDB(
            metadata_store=gdb,
            hdf_store=HDF5SqlDB(),
            working_directory=self.working_dir,
            config_filename=__this_dir__ / "../opencefadb" / "db-dataset-config-sandbox.ttl"
        )
        res = RemoteSparqlQuery(
            "SELECT * WHERE { ?s ?p ?o }",
            description="Selects all triples in the RDF database"
        ).execute(local_db.stores.rdf)

        count = len(res.data["results"]["bindings"])
        tripels = gdb.count_triples(key="total")
        self.assertEqual(count, tripels)

        res = RemoteSparqlQuery(
            SELECT_FAN_PROPERTIES.query,
            description="Selects all properties of the fan"
        ).execute(local_db.stores.rdf)

        res = RemoteSparqlQuery(
            """
            PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
            PREFIX qudt: <http://qudt.org/vocab/unit#>
            PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
            
            SELECT ?value ?unit
            WHERE {
              ?variable a m4i:NumericalVariable ;
                        ssno:hasStandardName <https://doi.org/10.5281/zenodo.14055811/standard_names/hub_diameter> ;
                        m4i:hasUnit ?unit ;
                        m4i:hasNumericalValue ?value .
            }""",
            description="Selects the hub diameter of the fan"
        ).execute(local_db.stores.rdf)
        print(res.data)
