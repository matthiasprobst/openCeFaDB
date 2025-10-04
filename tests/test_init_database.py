import pathlib
import shutil
import unittest

import opencefadb
import dotenv
__this_dir__ = pathlib.Path(__file__).parent


class TestInitDatabase(unittest.TestCase):

    def setUp(self):
        self.working_dir = pathlib.Path(__this_dir__ / "local-db")

    def tearDown(self):
        if self.working_dir. exists():
            shutil.rmtree(self.working_dir)

    def test_init_local_default(self):
        dotenv.load_dotenv(__this_dir__ / ".env", override=True)
        local_db = opencefadb.OpenCeFaDB.setup_local_default(
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
        local_db = opencefadb.OpenCeFaDB.setup_local_default(
            working_directory=self.working_dir,
            config_filename=__this_dir__ / "../opencefadb" / "db-dataset-config-sandbox.ttl"
        )
        graph = local_db.stores.rdf.graph
        self.assertEqual(len(graph), 7752)

    def test_download_zenodo_metadata(self):
        pass
