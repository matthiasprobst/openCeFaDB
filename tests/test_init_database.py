import pathlib
import unittest

import dotenv
import pyshacl
import rdflib
import requests.exceptions
from gldb.query import RemoteSparqlQuery, SparqlQuery
from gldb.stores import GraphDB
from owlrl import DeductiveClosure, RDFS_Semantics  # pip install owlrl

import opencefadb
from opencefadb import OpenCeFaDB
from opencefadb.query_templates.sparql import SELECT_FAN_PROPERTIES
from opencefadb.stores import RDFFileStore, HDF5SqlDB
from opencefadb.validation.shacl.templates.dcat import MINIMUM_DATASET_SHACL
from opencefadb.validation.shacl.templates.person import PERSON_SHACL

__this_dir__ = pathlib.Path(__file__).parent


class TestInitDatabase(unittest.TestCase):

    def setUp(self):
        dotenv.load_dotenv(__this_dir__ / ".env", override=True)
        self.working_dir = pathlib.Path(__this_dir__ / "local-db")

    # def tearDown(self):
    #     if self.working_dir.exists():
    #         shutil.rmtree(self.working_dir)

    def test_database(self):
        config_filename = OpenCeFaDB.pull(
            version="latest",
            target_directory=self.working_dir,
            sandbox=True
        )
        self.assertTrue(config_filename.exists())
        self.assertTrue(config_filename.is_file())
        self.assertEqual("opencefadb-config-sandbox-1-2-0.ttl", config_filename.name)

        out = OpenCeFaDB.initialize(
            working_directory=self.working_dir,
            config_filename=config_filename
        )
        filenames = ([pathlib.Path(o[1]) for o in out])
        self.assertEqual(
            115,  # number of files that are downloaded
            len([f for f in filenames if f.suffix == ".ttl"])
        )
        self.assertEqual(
            117,  # the config file is also in the metadata folder
            len(list((self.working_dir / "metadata").rglob("*.ttl")))
        )

        RDFFileStore._expected_file_extensions = {".ttl", }
        metadata_store = RDFFileStore(
            data_dir=self.working_dir / "metadata",
            recursive_exploration=True,
            formats="ttl"
        )

        raw_store = HDF5SqlDB(data_dir=self.working_dir)

        db_interface = opencefadb.OpenCeFaDB(
            metadata_store=metadata_store,
            hdf_store=raw_store
        )
        res = SELECT_FAN_PROPERTIES.execute(db_interface.metadata_store)
        self.assertEqual(88, len(res.data))

        query_d1_value = """PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
        SELECT ?value
        WHERE {
            <https://doi.org/10.5281/zenodo.17871736#D1> m4i:hasNumericalValue ?value  .
        }
        """
        new_sparql = SparqlQuery(
            query_d1_value,
            description="Selects all properties of a specific dataset"
        )
        res = new_sparql.execute(db_interface.metadata_store)
        self.assertTrue(138.0, res.data["value"][0])

    def test_config_validation(self):
        config_filename = OpenCeFaDB.pull(
            version="latest",
            target_directory=self.working_dir,
            sandbox=True
        )
        # validate the configuration file
        config_graph = rdflib.Graph()
        config_graph.parse(source=config_filename, format="ttl")

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
        database_interface = opencefadb.OpenCeFaDB(
            metadata_store=RDFFileStore(
                data_dir=self.working_dir / "metadata",
                formats="ttl",
                recursive_exploration=True
            ),
            hdf_store=HDF5SqlDB(data_dir=self.working_dir)
        )
        metadata_dir = database_interface.metadata_store.data_dir
        metadata_ttl_filenames = list(metadata_dir.rglob("*.ttl"))
        self.assertEqual(116, len(metadata_ttl_filenames))

        # also the second time should work (exist_ok=True):
        database_interface = opencefadb.OpenCeFaDB(
            metadata_store=RDFFileStore(
                data_dir=self.working_dir / "metadata",
                formats="ttl",
                recursive_exploration=True
            ),
            hdf_store=HDF5SqlDB(data_dir=self.working_dir)
        )
        graph = database_interface.metadata_store.graph

        # Compute RDFS closure (adds entailed triples to the graph)
        DeductiveClosure(RDFS_Semantics).expand(graph)

        q = """
        PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
        SELECT * WHERE {
          ?s m4i:identifier ?o .
        } LIMIT 10
        """

        bindings = graph.query(q)
        for row in bindings:
            print(row.s, row.o)

        # assert with a SPARQL query that one person is called Matthias:
        find_first_names = """
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?firstName
        WHERE {
            ?s a prov:Person .
            ?s foaf:firstName ?firstName .
        }
        """
        bindings = database_interface.metadata_store.graph.query(find_first_names).bindings
        names = [str(row[rdflib.Variable("firstName")]) for row in bindings]
        self.assertIn("Matthias", names)
        self.assertIn("Balazs", names)

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
        bindings = database_interface.metadata_store.graph.query(find_snt_title).bindings
        titles = [str(row[rdflib.Variable("title")]) for row in bindings]
        self.assertEqual(
            titles[0],
            "Standard Name Table for the Property Descriptions of Centrifugal Fans"
        )

    def test_graphdb(self):
        try:
            gdb = GraphDB(
                endpoint="http://localhost:7201",
                repository="OpenCeFaDB-Sandbox",
                username="admin",
                password="admin"
            )
            gdb.get_repository_info("OpenCeFaDB-Sandbox")
        except requests.exceptions.ConnectionError as e:
            self.skipTest(f"GraphDB not available: {e}")
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
        count = len(res.data)
        tripels = gdb.count_triples(key="total")
        self.assertEqual(count, tripels)

        res = gdb.delete_repository("OpenCeFaDB-Sandbox")
        self.assertTrue(res)

    def test_db_with_graphdb(self):
        try:
            gdb = GraphDB(
                endpoint="http://localhost:7201",
                repository="OpenCeFaDB-Sandbox",
                username="admin",
                password="admin"
            )
            gdb.get_repository_info("OpenCeFaDB-Sandbox")
        except requests.exceptions.ConnectionError as e:
            self.skipTest(f"GraphDB not available: {e}")

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

        for filename in (self.working_dir / "metadata").rglob("*.ttl"):

            target_graph = rdflib.Graph()
            target_graph.parse(source=filename, format="ttl")

            shacl_graph = rdflib.Graph()
            shacl_graph.parse(data=PERSON_SHACL, format="ttl")
            results = pyshacl.validate(
                data_graph=target_graph,
                shacl_graph=shacl_graph,
                inference='rdfs',
                abort_on_first=False,
                meta_shacl=False,
                advanced=True,
            )
            conforms, results_graph, results_text = results  # self.assertTrue(conforms)
            if not conforms:
                print("SHACL validation results:")
                print(results_text)

            gdb.upload_file(filename)

        database_interface = opencefadb.OpenCeFaDB(
            metadata_store=gdb,
            hdf_store=HDF5SqlDB(data_dir=self.working_dir)
        )
        res = RemoteSparqlQuery(
            "SELECT * WHERE { ?s ?p ?o }",
            description="Selects all triples in the RDF database"
        ).execute(database_interface.metadata_store)

        count = len(res.data)
        tripels = gdb.count_triples(key="total")
        self.assertEqual(count, tripels)

        _ = RemoteSparqlQuery(
            SELECT_FAN_PROPERTIES.query,
            description="Selects all properties of the fan"
        ).execute(database_interface.metadata_store)

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
        ).execute(database_interface.metadata_store)

        q = """PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
SELECT * WHERE {
  ?s m4i:hasEmployedTool ?o .
} LIMIT 100
"""
        res = RemoteSparqlQuery(
            q,
            description="Selects identifiers"
        ).execute(database_interface.metadata_store)

        self.assertEqual(100, len(res.data))

        # find all names of persons in the database
        res = RemoteSparqlQuery(
            """
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX prov: <http://www.w3.org/ns/prov#>
            
            SELECT ?s
            WHERE {
                ?s a prov:Person .
            }
            """,
            description="Selects names of all persons in the database"
        ).execute(database_interface.metadata_store)
        self.assertEqual(2, len(res.data))
