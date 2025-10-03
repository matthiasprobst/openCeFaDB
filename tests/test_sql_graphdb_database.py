import logging
import pathlib
import shutil
import unittest

from opencefadb import connect_to_database
from opencefadb.configuration import get_config
from opencefadb.database.query_templates.sparql import SELECT_ALL
from opencefadb.database.stores.rdf_stores.graphdb import GraphDBStore

__this_dir__ = pathlib.Path(__file__).parent


class TestSqlGraphDBDatabase(unittest.TestCase):
    """Testing the combination of using a SQL database and a graph database."""

    def setUp(self):
        logger = logging.getLogger("opencefadb")
        self._level = logger.level
        logger.setLevel(logging.INFO)
        for handler in logger.handlers:
            handler.setLevel(logging.INFO)

        self.profile = "local_graphdb.test"
        pathlib.Path("./test_download").mkdir(exist_ok=True)
        # self._cfg = get_config()
        # self._current_profile = self._cfg.profile
        # self._cfg.select_profile(self.profile)

        # # create or get existing:
        # GraphDBStore.create(
        #     config_filename=__this_dir__ / "test-repo-config.ttl",
        #     host="localhost",
        #     port=7201
        # )

    def tearDown(self):
        shutil.rmtree("./test_download")
        self._cfg.select_profile(self._current_profile)

        logger = logging.getLogger("opencefadb")
        logger.setLevel(self._level)
        for handler in logger.handlers:
            handler.setLevel(self._level)

    def test_init_rdf_database(self):
        db = connect_to_database(
            profile="local_graphdb.test"
        )
        db.rdf.reset(__this_dir__ / "test-repo-config.ttl")
        results = db.rdf.execute_query(SELECT_ALL)
        self.assertEqual(70, len(results.result.bindings))

        # We need to download the metadata again and full the rdf:
        from opencefadb.database.dbinit import initialize_database
        filenames = initialize_database(self._cfg.metadata_directory)
        self.assertEqual(4, len(filenames))

        # search again
        results = db.rdf.execute_query(SELECT_ALL)
        self.assertTrue(len(results.result.bindings) > 5093)

        # db.rdf.upload_file("tests/data/ontology.ttl")
