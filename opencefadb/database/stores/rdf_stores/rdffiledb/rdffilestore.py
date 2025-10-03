import logging
import pathlib

import rdflib
from gldb.query.query import QueryResult
from gldb.query import SparqlQuery

from ..abstracts import OpenCeFaDBRDFStore

logger = logging.getLogger("opencefadb")


class RDFFileStore(OpenCeFaDBRDFStore):

    def __init__(self):
        self._filenames = []
        self._graphs = {}
        self._expected_file_extensions = {".ttl", ".rdf", ".jsonld"}

    def __repr__(self):
        return f"<{self.__class__.__name__} (n_files={len(self._filenames)})>"

    @property
    def expected_file_extensions(self):
        return self._expected_file_extensions

    def execute_query(self, query: SparqlQuery) -> QueryResult:
        return QueryResult(query=query, result=query.execute(self.graph))

    def upload_file(self, filename) -> bool:
        filename = pathlib.Path(filename)
        if not filename.exists():
            raise FileNotFoundError(f"File {filename} not found.")
        if filename.suffix not in self._expected_file_extensions:
            raise ValueError(f"File type {filename.suffix} not supported.")
        self._filenames.append(filename.resolve().absolute())
        return True

    @property
    def graph(self) -> rdflib.Graph:
        combined_graph = rdflib.Graph()
        for filename in self._filenames:
            assert filename.exists(), f"File {filename} does not exist."
            g = self._graphs.get(filename, None)
            if not g:
                g = rdflib.Graph()
                try:
                    g.parse(filename)
                except Exception as e:
                    logger.critical(f"Could not parse file '{filename}'. Skipping the file! Orig. error message: '{e}'")
                    continue
                for s, p, o in g:
                    if isinstance(s, rdflib.BNode):
                        new_s = rdflib.URIRef(f"https://example.org/{s}")
                    else:
                        new_s = s
                    if isinstance(o, rdflib.BNode):
                        new_o = rdflib.URIRef(f"https://example.org/{o}")
                    else:
                        new_o = o
                    g.remove((s, p, o))
                    g.add((new_s, p, new_o))
                self._graphs[filename] = g
            combined_graph += g
        return combined_graph

    def reset(self, *args, **kwargs):
        self._filenames = []
        self._graphs = {}
        return self
