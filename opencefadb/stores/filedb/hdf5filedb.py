import hashlib
import logging
import pathlib
from abc import ABC, abstractmethod
from typing import List

import h5py
from gldb import DataStore
from gldb.query.query import AbstractQuery
from h5rdmtoolbox.database.hdfdb import FilesDB

logger = logging.getLogger("opencefadb")


class HDF5FileQuery(AbstractQuery, ABC):

    def __init__(self, filter, objfilter=None, recursive=True, **kwargs):
        self._filter = filter
        self._objfilter = objfilter
        self._recursive = recursive
        self._kwargs = kwargs

    @abstractmethod
    def execute(self, filenames: List[pathlib.Path], *args, **kwargs):
        pass


class FindOneLazyHDFObject(HDF5FileQuery):
    def execute(self, filenames: List[pathlib.Path], *args, **kwargs):
        return FilesDB(filenames).find_one(flt=self._filter,
                                           objfilter=self._objfilter,
                                           recursive=self._recursive,
                                           **kwargs)


class FindManyLazyHDFObject(HDF5FileQuery):
    def execute(self, filenames: List[pathlib.Path], *args, **kwargs):
        return FilesDB(filenames).find(flt=self._filter,
                                       objfilter=self._objfilter,
                                       recursive=self._recursive, **kwargs)


class HDF5FileDB(DataStore):

    def __init__(self):
        self._filenames = {}
        self._expected_file_extensions = {".hdf", ".hdf5", ".h5"}

    def __repr__(self):
        return f"<{self.__class__.__name__} (n_files={len(self.filenames)})>"

    @property
    def filenames(self):
        return self._filenames

    def upload_file(self, filename) -> str:
        filename = pathlib.Path(filename)
        import hashlib
        hash_obj = hashlib.sha256()
        hash_obj.update(str(filename.resolve().absolute()).encode())

        assert filename.exists(), f"File {filename} does not exist."
        assert filename.suffix in self._expected_file_extensions, f"File type {filename.suffix} not supported"

        h5_hash = h5hash(str(filename))

        if h5_hash in self._filenames:
            return h5_hash

        logger.debug(f"Uploading file {filename} to {self}")
        self._filenames[h5_hash] = filename.resolve().absolute()
        return h5_hash

    def execute_query(self, query: HDF5FileQuery):
        return query.execute(self.filenames)


def h5hash(filename: str) -> str:
    """Generate a deterministic ID based on metadata and content."""
    hash_obj = hashlib.sha256()

    with h5py.File(filename, 'r') as f:
        # Include metadata in the hash
        for k, v in f.attrs.items():
            hash_obj.update(k.encode())
            hash_obj.update(v.encode())

        # Include content in the hash
        def hash_group(group):
            """Recursively hash the datasets in a group."""
            for key in group:
                item = group[key]
                if isinstance(item, h5py.Group):
                    hash_group(item)
                elif isinstance(item, h5py.Dataset):
                    hash_obj.update(item[()].tobytes())  # Update hash with dataset content

        hash_group(f)

    return hash_obj.hexdigest()
