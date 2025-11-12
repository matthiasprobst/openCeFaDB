import pathlib
import unittest

import dotenv
import pyshacl
import rdflib
from gldb.query import RemoteSparqlQuery
from gldb.stores import GraphDB
from owlrl import DeductiveClosure, RDFS_Semantics  # pip install owlrl

import opencefadb
from opencefadb.query_templates.sparql import SELECT_FAN_PROPERTIES
from opencefadb.stores import RDFFileStore, HDF5SqlDB
from opencefadb.validation.shacl.templates.dcat import MINIMUM_DATASET_SHACL

__this_dir__ = pathlib.Path(__file__).parent

N_M4I_IDENTIFIERS_IN_DB = 7

CONFIG_FILENAME = __this_dir__ / "../opencefadb" / "db-dataset-config-sandbox-3.ttl"


class TestInitDatabase(unittest.TestCase):

    def setUp(self):
        dotenv.load_dotenv(__this_dir__ / ".env", override=True)
        self.working_dir = pathlib.Path(__this_dir__ / "local-db")

    # def tearDown(self):
    #     if self.working_dir.exists():
    #         shutil.rmtree(self.working_dir)

    def test_generate_dataset_from_zenodo_record(self):
        from opencefadb.core import generate_config
        config_filename = generate_config(
            zenodo_records=[
                {"record_id": 17271932, "sandbox": False},
                {"record_id": 344192, "sandbox": True},
                {"record_id": 14551649, "sandbox": False},
            ],
            local_filenames=[
                __this_dir__ / "data" / "test_measurements/2023-11-07-15-33-03_run.jsonld"
            ]
        )
        config_graph = rdflib.Graph()
        config_graph.parse(location=str(config_filename))
        shacl_graph = rdflib.Graph()
        shacl_graph.parse(data=MINIMUM_DATASET_SHACL, format="ttl")
        results = pyshacl.validate(
            data_graph=config_graph,
            shacl_graph=shacl_graph,
            inference='rdfs',
            abort_on_first=False,
            meta_shacl=False,
            advanced=True,
        )
        conforms, results_graph, results_text = results
        if not conforms:
            print("SHACL validation results:")
            print(results_text)

    def test_db_with_rdflib(self):
        local_db = opencefadb.OpenCeFaDB(
            metadata_store=RDFFileStore(data_dir=self.working_dir / "metadata"),
            hdf_store=HDF5SqlDB(data_dir=self.working_dir / "rawdata"),
            working_directory=self.working_dir,
            config_filename=CONFIG_FILENAME
        )
        metadata_dir = local_db.stores.rdf.data_dir
        metadata_ttl_filenames = metadata_dir.glob("*.ttl")
        metadata_jsonld_filenames = metadata_dir.glob("*.jsonld")
        metadata_filenames = list(metadata_ttl_filenames) + list(metadata_jsonld_filenames)
        self.assertGreaterEqual(len(metadata_filenames), 5)

        # also the second time should work (exist_ok=True):
        local_db = opencefadb.OpenCeFaDB(
            metadata_store=RDFFileStore(data_dir="local-db/data/metadata"),
            hdf_store=HDF5SqlDB(),
            working_directory=self.working_dir,
            config_filename=CONFIG_FILENAME
        )
        graph = local_db.stores.rdf.graph

        # Compute RDFS closure (adds entailed triples to the graph)
        DeductiveClosure(RDFS_Semantics).expand(graph)

        q = """
        PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
        SELECT * WHERE {
          ?s m4i:identifier ?o .
        } LIMIT 100
        """

        bindings = graph.query(q)
        for row in bindings:
            print(row.s, row.o)

        # assert with a sparlq query that one person is called Matthias:
        find_first_names = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?name
        WHERE {
            ?s a foaf:Person .
            ?s foaf:name ?name .
        }
        """
        bindings = graph.query(find_first_names)
        names = [str(row.name) for row in bindings]
        self.assertIn("Probst, Matthias", names)

        find_snt_title = """
        PREFIX ex: <https://doi.org/10.5281/zenodo.17271932#>
        PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT ?title
        WHERE {
            ?s a ssno:StandardNameTable .
            ?s dcterms:title ?title .
        }
        """
        bindings = graph.query(find_snt_title)
        titles = [str(row.title) for row in bindings]
        self.assertEqual(
            titles[0],
            "Standard Name Table for the Property Descriptions of Centrifugal Fans"
        )

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
            config_filename=CONFIG_FILENAME
        )
        res = RemoteSparqlQuery(
            "SELECT * WHERE { ?s ?p ?o }",
            description="Selects all triples in the RDF database"
        ).execute(local_db.stores.rdf)

        count = len(res.data)
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

        q = """PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
SELECT * WHERE {
  ?s m4i:identifier ?o .
} LIMIT 100
"""
        res = RemoteSparqlQuery(
            q,
            description="Selects identifiers"
        ).execute(local_db.stores.rdf)

        # self.assertEqual(len(res.data), N_M4I_IDENTIFIERS_IN_DB)

        # find all names of persons in the database
        res = RemoteSparqlQuery(
            """
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT ?name
            WHERE {
                ?s a foaf:Person .
                ?s foaf:name ?name .
            }
            """,
            description="Selects names of all persons in the database"
        ).execute(local_db.stores.rdf)
        print(res.data)
