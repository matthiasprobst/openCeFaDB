import logging
import pathlib

import rdflib
from h5rdmtoolbox.catalog import InMemoryRDFStore, RDFStore, RDFStore
from rdflib.plugins.stores.sparqlstore import SPARQLStore

# from .._abstracts import OpenCeFaDBRDFStore

logger = logging.getLogger("opencefadb")


class RDFFileStore(InMemoryRDFStore):

    def reset(self, *args, **kwargs):
        self._filenames = []
        self._graphs = rdflib.Graph()
        self._combined_graph = rdflib.Graph()
        return self


class RdflibSPARQLStore(RDFStore):
    """A SPARQL endpoint RDF store using rdflib's SPARQLStore."""

    _expected_file_extensions = {".ttl", ".nt", ".rdf", ".xml", ".n3"}

    def __init__(self, endpoint_url: str):
        super().__init__()
        self._store = SPARQLStore(endpoint_url)
        self._graph = rdflib.Graph(store=self._store)

    @property
    def graph(self):
        return self._graph

    def upload_file(self, filename) -> bool:
        filename = pathlib.Path(filename).resolve().absolute()
        if not filename.exists():
            raise FileNotFoundError(f"File {filename} not found.")
        if filename.suffix not in self._expected_file_extensions:
            raise ValueError(f"File type {filename.suffix} not supported.")
        g = rdflib.Graph()
        g.parse(location=str(filename))
        self._graph += g
        return True
