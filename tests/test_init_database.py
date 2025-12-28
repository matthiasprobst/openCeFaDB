import pathlib
import unittest

import dotenv
import rdflib
import requests.exceptions
from h5rdmtoolbox import catalog as h5cat
from h5rdmtoolbox.catalog import HDF5SqlDB, GraphDB
from h5rdmtoolbox.catalog.profiles import IS_VALID_CATALOG_SHACL

from opencefadb import OpenCeFaDB
from opencefadb.query_templates.sparql import SELECT_FAN_PROPERTIES
from opencefadb.stores import RDFFileStore

__this_dir__ = pathlib.Path(__file__).parent


class TestInitDatabase(unittest.TestCase):

    def setUp(self):
        dotenv.load_dotenv(__this_dir__ / ".env", override=True)
        self.working_dir = pathlib.Path(__this_dir__ / "local-db")

    # def tearDown(self):
    #     if self.working_dir.exists():
    #         shutil.rmtree(self.working_dir)

    def test_database_with_graphdb(self):
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

        db = OpenCeFaDB(working_directory=self.working_dir, version="latest", sandbox=True)
        self.assertEqual(db.catalog.version, "1.5.0")

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

        # gdb.register_shacl_shape("PersonShape", shacl_data=PERSON_SHACL)

        for filename in db.rdf_directory.rglob("*.ttl"):
            gdb.upload_file(filename)

        db.add_main_rdf_store(gdb)

        db.add_wikidata_store(augment_knowledge=True)

        res = h5cat.RemoteSparqlQuery(
            "SELECT * WHERE { ?s ?p ?o }",
            description="Selects all triples in the RDF database"
        ).execute(db.main_rdf_store)

        count = len(res.data)
        tripels = gdb.count_triples(key="total")
        self.assertEqual(count, tripels)

        res = h5cat.RemoteSparqlQuery(
            SELECT_FAN_PROPERTIES.query,
            description="Selects all properties of the fan"
        ).execute(db.main_rdf_store)
        self.assertEqual(88, len(res.data))

        # find all names of persons in the database
        res = h5cat.RemoteSparqlQuery(
            """
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX prov: <http://www.w3.org/ns/prov#>

            SELECT ?s
            WHERE {
                ?s a prov:Person .
            }
            """,
            description="Selects names of all persons in the database"
        ).execute(db.main_rdf_store)
        self.assertEqual(2, len(res.data))

    def test_database_with_rdflib_store(self):
        db = OpenCeFaDB(working_directory=self.working_dir, version="latest", sandbox=True)
        self.assertEqual(db.catalog.version, "1.5.0")

        metadata_store = RDFFileStore(
            data_dir=self.working_dir / "metadata",
            recursive_exploration=True,
            formats="ttl"
        )

        db.add_main_rdf_store(metadata_store)
        db.add_wikidata_store(augment_knowledge=True)
        res = SELECT_FAN_PROPERTIES.execute(db.main_rdf_store)
        self.assertEqual(88, len(res.data))

        query_d1_value = """PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
        SELECT ?value
        WHERE {
            <https://doi.org/10.5281/zenodo.17871736#D1> m4i:hasNumericalValue ?value  .
        }
        """
        new_sparql = h5cat.SparqlQuery(
            query_d1_value,
            description="Selects all properties of a specific dataset"
        )
        res = new_sparql.execute(db.main_rdf_store)
        self.assertTrue(138.0, res.data["value"][0])

        find_first_names = """
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                SELECT ?firstName
                WHERE {
                    ?s a prov:Person .
                    ?s foaf:firstName ?firstName .
                }
                """
        bindings = db.main_rdf_store.graph.query(find_first_names).bindings
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
        bindings = db.main_rdf_store.graph.query(find_snt_title).bindings
        titles = [str(row[rdflib.Variable("title")]) for row in bindings]
        self.assertEqual(
            titles[0],
            "Standard Name Table for the Property Descriptions of Centrifugal Fans"
        )

    def test_config_validation(self):
        catalog = OpenCeFaDB.download(
            version="latest",
            target_directory=self.working_dir,
            sandbox=True,
            validate=False
        )
        validation_result = catalog.validate(shacl_data=IS_VALID_CATALOG_SHACL)
        if not validation_result.conforms:
            print("SHACL validation results:")
            print(validation_result.results_text)
        self.assertTrue(validation_result)
