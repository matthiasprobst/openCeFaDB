import pathlib
import shutil
import unittest

import rdflib

from opencefadb import connect_to_database
from opencefadb import set_logging_level
from opencefadb.configuration import get_config
from opencefadb.database import dbinit
from opencefadb.database.dbinit import initialize_database
from opencefadb.database.stores.rdf_stores.graphdb import GraphDBStore

set_logging_level('DEBUG')

__this_dir__ = pathlib.Path(__file__).parent

class TestDatabase(unittest.TestCase):

    def setUp(self):
        pathlib.Path("./test_download").mkdir(exist_ok=True)
        self._cfg = get_config()
        self._current_profile = self._cfg.profile
        self._cfg.select_profile("test")
        self.profile = "local_graphdb.test"

        GraphDBStore.create(
            config_filename=__this_dir__ / "test-repo-config.ttl",
            host="localhost",
            port=7201
        )

        db = connect_to_database(self.profile)
        initialize_database(self._cfg.metadata_directory)

    def tearDown(self):
        shutil.rmtree("./test_download")
        self._cfg.select_profile(self._current_profile)

    def test_singleton(self):
        db1 = connect_to_database(self.profile)
        db2 = connect_to_database(self.profile)
        self.assertIs(db1, db2)

    def test_read_dataset_files(self):
        dataset = dbinit._get_metadata_datasets()
        self.assertIsInstance(dataset, rdflib.Graph)

    def test_init_database(self):
        filenames = dbinit.initialize_database(metadata_directory="./test_download")
        for filename in filenames:
            self.assertTrue(filename.exists())
        self.assertEqual(len(filenames), 4, f"Expected 4 files, got {len(filenames)}. filenames are: {filenames}")

    def test_download_cad_file(self):
        db = connect_to_database(self.profile)
        filename = db.download_cad_file(target_dir="./test_download")
        self.assertTrue(filename.exists())
        self.assertTrue(filename.suffix == ".igs")

    def test_config_singleton(self):
        cfg1 = get_config()
        cfg2 = get_config()
        self.assertIs(cfg1, cfg2)
        cfg1.select_profile("test")
        cfg3 = get_config()
        self.assertIs(cfg1, cfg3)
        self.assertEqual(cfg1.profile, "test")
        cfg3.select_profile("local_graphdb.test")
        self.assertEqual(cfg3.profile, "local_graphdb.test")
        cfg4 = get_config()
        self.assertIs(cfg3, cfg4)

    def test_upload_hdf_file(self):
        self._cfg.select_profile("local_sql.test")
        db = connect_to_database(self.profile)
        id = db.upload_hdf(
            r"C:\Users\matth\Documents\PHD\data\measurements\processed\opm\main_cases\2023-10-11\fan_curve\run1\2023-10-11-14-55-47_run.hdf"
        )
        print(id)
        # db.store_manager["hdf_db"].upload_file(
        #     r"C:\Users\matth\Documents\PHD\opencefadb-admin\data\measurements\processed\opm\main_cases\2023-10-11\fan_curve\run1\2023-10-11-14-55-47_run.hdf"
        # )
