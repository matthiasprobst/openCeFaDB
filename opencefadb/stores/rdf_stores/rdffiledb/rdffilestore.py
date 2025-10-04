import logging

import rdflib
from gldb.stores import InMemoryRDFStore

from .._abstracts import OpenCeFaDBRDFStore

logger = logging.getLogger("opencefadb")


class RDFFileStore(InMemoryRDFStore, OpenCeFaDBRDFStore):

    def reset(self, *args, **kwargs):
        self._filenames = []
        self._graphs = rdflib.Graph()
        self._combined_graph = rdflib.Graph()
        return self
