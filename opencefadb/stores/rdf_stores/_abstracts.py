from abc import abstractmethod

from gldb.stores import RDFStore


class OpenCeFaDBRDFStore(RDFStore):

    @abstractmethod
    def reset(self, *args, **kwargs):
        """Resets the store/database."""
