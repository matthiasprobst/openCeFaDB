import pathlib
import shutil
import unittest

import opencefadb
import dotenv
__this_dir__ = pathlib.Path(__file__).parent


class TestInitDatabase(unittest.TestCase):

    def setUp(self):
        self.working_dir = pathlib.Path(__this_dir__ / "local-db")

    # def tearDown(self):
    #     if self.working_dir. exists():
    #         shutil.rmtree(self.working_dir)

    def test_init_local_default(self):
        dotenv.load_dotenv(__this_dir__ / ".env", override=True)
        local_db = opencefadb.OpenCeFaDB.setup_local_default(
            working_directory=self.working_dir,
            config_filename=__this_dir__ / "../opencefadb" / "db-dataset-config-sandbox.ttl"
        )

    def test_download_zenodo_metadata(self):
        pass
