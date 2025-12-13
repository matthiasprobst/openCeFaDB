# import pathlib
# import shutil
# import unittest
#
# import rdflib
# from gldb.stores import InMemoryRDFStore
#
# from opencefadb import set_logging_level
# from opencefadb.core import OpenCeFaDB
# from opencefadb.stores.filedb.hdf5sqldb import HDF5SqlDB
# from opencefadb.utils import download_file
#
# set_logging_level('DEBUG')
#
# __this_dir__ = pathlib.Path(__file__).parent
#
#
# class TestDatabase(unittest.TestCase):
#
#     def setUp(self):
#         self._test_download_dir = pathlib.Path(__this_dir__ / "test_download")
#         self._test_download_dir.mkdir(exist_ok=True)
#         # self._cfg = get_config()
#         # self._current_profile = self._cfg.profile
#         # self._cfg.select_profile("test")
#         # self.profile = "local_graphdb.test"
#         #
#         # GraphDBStore.create(
#         #     config_filename=__this_dir__ / "test-repo-config.ttl",
#         #     host="localhost",
#         #     port=7201
#         # )
#
#     def tearDown(self):
#         shutil.rmtree(__this_dir__ / "test_download")
#         # self._cfg.select_profile(self._current_profile)
#
#     def test_download_with_metadata(self):
#         download_url = "https://zenodo.org/records/14551649/files/metadata.jsonld"
#         r = download_file(download_url, self._test_download_dir / "metadata.jsonld")
#         self.assertTrue(r.exists())
#         self.assertEqual(r.name, "metadata.jsonld")
#
#     def test_download_hdf_ontology(self):
#         url = "https://purl.allotrope.org/voc/adf/REC/2024/12/hdf.ttl"
#         filename = download_file(url, self._test_download_dir / "ontology-hdf.ttl")
#         self.assertTrue(filename.exists())
#         graph = rdflib.Graph()
#         graph.parse(filename, format="ttl")
#         self.assertGreater(len(graph), 0)
#
#         # query for hdf5:File:
#         res = graph.query("""
#         PREFIX hdf: <http://purl.allotrope.org/ontologies/hdf5/1.8#>
#         PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#         PREFIX owl: <http://www.w3.org/2002/07/owl#>
#
#         SELECT ?p ?o WHERE {
#             hdf:File ?p ?o .
#         }
#         """)
#         bindings = res.bindings
#         self.assertEqual(7, len(bindings))
#         property_value_dict = {b[rdflib.Variable("p")]: b[rdflib.Variable("o")] for b in bindings}
#         self.assertEqual(
#             property_value_dict[rdflib.URIRef("http://www.w3.org/2000/01/rdf-schema#label")],
#             rdflib.Literal("file")
#         )
#         self.assertEqual(
#             property_value_dict[rdflib.URIRef("http://www.w3.org/2004/02/skos/core#altLabel")],
#             rdflib.Literal("HDF 5 file")
#         )
#
#     def test_database(self):
#         in_memory_rdf_store = InMemoryRDFStore(data_dir=__this_dir__)
#         db = OpenCeFaDB(
#             metadata_store=in_memory_rdf_store,
#         )
#         db.download_metadata()
#         print(db)
#
#     def test_download_cad_file(self):
#         test_data_dir = __this_dir__ / "test-db-data"
#         test_data_dir.mkdir(exist_ok=True)
#         in_memory_rdf_store = InMemoryRDFStore(data_dir=__this_dir__)
#         db = OpenCeFaDB(
#             metadata_store=in_memory_rdf_store,
#             hdf_store=HDF5SqlDB(),
#             data_directory=test_data_dir
#         )
#         db.initialize(config_filename=__this_dir__ / "../opencefadb/db-dataset-config.jsonld", exist_ok=True)
#         # db.download_metadata()
#
#         filename = db.download_cad_file(target_dir=self._test_download_dir)
#         self.assertTrue(filename.exists())
#         self.assertTrue(filename.suffix == ".igs")
#
#     def test_upload_hdf_file(self):
#         test_data_dir = __this_dir__ / "test-db-data"
#         test_data_dir.mkdir(exist_ok=True)
#         in_memory_rdf_store = InMemoryRDFStore(data_dir=__this_dir__)
#         db = OpenCeFaDB(
#             metadata_store=in_memory_rdf_store,
#             hdf_store=HDF5SqlDB(),
#             data_directory=test_data_dir
#         )
#
#         id = db.upload_hdf(
#             r"C:\Users\matth\Documents\PHD\data\measurements\processed\opm\main_cases\2023-10-11\fan_curve\run1\2023-10-11-14-55-47_run.hdf"
#         )
#         print(id)
#         # db.store_manager["hdf_db"].upload_file(
#         #     r"C:\Users\matth\Documents\PHD\opencefadb-admin\data\measurements\processed\opm\main_cases\2023-10-11\fan_curve\run1\2023-10-11-14-55-47_run.hdf"
#         # )
