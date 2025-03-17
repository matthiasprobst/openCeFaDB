import logging
import pathlib
from typing import Union, Optional

import rdflib
from gldb.query import QueryResult
from gldb.query.rdfstorequery import SparqlQuery
from gldb.stores import RDFStore

from .connection import GraphDB

logger = logging.getLogger("opencefadb")


class GraphDBStore(RDFStore):

    def __init__(
            self,
            host,
            port,
            user,
            password,
            repository):
        self._graphdb_url = f"http://{host}:{port}"
        self._auth = (user, password)
        _graphdb = GraphDB(
            url=self._graphdb_url,
            auth=self._auth
        )
        self._host = host
        self._port = port
        self._repoID = repository
        self._repo = _graphdb[repository]
        self._expected_file_extensions = {".ttl", ".rdf", ".jsonld"}

    def __repr__(self):
        return f"<{self.__class__.__name__} (GraphDB-Repo={self._repo})>"

    def reset(self, config_filename: Optional[Union[str, pathlib.Path]] = None):
        logger.debug("Resetting the GraphDB store.")
        logger.debug("Deleting the GraphDB repo...")
        self.__class__.delete(
            self._repoID,
            self._host,
            self._port,
            self._auth
        )
        logger.debug("Creating a new GraphDB repo...")
        self.__class__.create(
            config_filename,
            self._host,
            self._port
        )
        logger.debug("...done")

    @classmethod
    def create(cls,
               config_filename: Optional[Union[str, pathlib.Path]] = None,
               host="http://localhost",
               port=7200):
        from . import administration
        if config_filename is None:
            from opencefadb import GRAPH_DB_CONFIG_FILENAME
            config_filename = GRAPH_DB_CONFIG_FILENAME
        repoID = administration.create_repository(
            config_filename,
            host,
            port
        )
        return repoID

    @classmethod
    def delete(cls, repository_id: str, host="http://localhost", port=7200, auth=None):
        from . import administration
        if host == "localhost":
            host = "http://localhost"
        return administration.delete_repository(
            repository_id=repository_id,
            host=host,
            port=port,
            auth=auth
        )

    @property
    def graph(self) -> rdflib.Graph:
        return self._repo.graph

    def upload_file(self, filename: Union[str, pathlib.Path]) -> bool:
        return self._repo.upload_file(filename) == 204

    def execute_query(self, query: SparqlQuery) -> QueryResult:
        return QueryResult(query=query, result=query.execute(self.graph))


def _parse_url(url):
    if url.endswith("/"):
        return url[:-1]
    return url
