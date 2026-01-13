import pathlib
import unittest

import dotenv
import rdflib
from h5rdmtoolbox import catalog as h5cat
from h5rdmtoolbox.catalog import HDF5SqlDB, GraphDB
from h5rdmtoolbox.catalog.profiles import IS_VALID_CATALOG_SHACL

from opencefadb import OpenCeFaDB
from opencefadb.sparql_templates.fan import SELECT_FAN_PROPERTIES
from opencefadb.stores import RDFFileStore

__this_dir__ = pathlib.Path(__file__).parent


class TestInitDatabase(unittest.TestCase):

    def setUp(self):
        dotenv.load_dotenv(__this_dir__ / ".env", override=True)
        self.working_dir = pathlib.Path(__this_dir__ / "local-db")

    def test_database_with_graphdb(self):
        db = OpenCeFaDB.from_graphdb_setup(
            working_directory=self.working_dir,
            version="latest",
            sandbox=True,
            endpoint="http://localhost:7200",
            repository="OpenCeFaDB-Sandbox",
            username="admin",
            password="admin",
            add_wikidata_store=True
        )
        db.download_metadata()
        self.assertEqual(db.catalog.version, "1.7.0")
        res = h5cat.RemoteSparqlQuery(
            "SELECT * WHERE { ?s ?p ?o }",
            description="Selects all triples in the RDF database"
        ).execute(db.main_rdf_store)

        count = len(res.data)
        tripels = db.main_rdf_store.count_triples(key="total")
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

        # find a hdf5 dataset based on a hdf attribute standard_name="volume_flow_rate":
        res = h5cat.SparqlQuery(
            """
            PREFIX h5py: <http://purl.org/nfdi4ing/h5py#>
            PREFIX dcterms: <http://purl.org/dc/terms/>
            
            SELECT ?dataset ?title
            WHERE {
                ?dataset a h5py:Dataset .
                ?dataset dcterms:title ?title .
                ?dataset h5py:standard_name "volume_flow_rate" .
            }
            """,
            description="Finds a hdf5 dataset based on a hdf attribute standard_name='volume_flow_rate'"
        ).execute(db.hdf5_store)

    def test_database_with_rdflib_store(self):
        db = OpenCeFaDB(working_directory=self.working_dir, version="latest", sandbox=True)
        self.assertEqual(db.catalog.version, "1.6.0")

        metadata_store = RDFFileStore(
            data_dir=self.working_dir / "metadata",
            recursive_exploration=True,
            formats="ttl"
        )

        db.add_main_rdf_store(metadata_store)
        db.add_wikidata_store(augment_main_rdf_store=True)
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
        catalog = OpenCeFaDB.download_catalog(
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
