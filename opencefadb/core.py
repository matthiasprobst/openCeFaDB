import logging
import pathlib
from typing import Any, Optional, Union

import pandas as pd
import rdflib
from gldb import GenericLinkedDatabase
from gldb.query import Query
from gldb.stores import RDFStore, DataStore

from opencefadb.dbinit import download_metadata_datasets, _get_metadata_datasets, MediaType
from opencefadb.query_templates.sparql import (
    SELECT_FAN_PROPERTIES,
    SELECT_ALL,
    SELECT_FAN_CAD_FILE,
    SELECT_ALL_OPERATION_POINTS
)
from opencefadb.stores.rdf_stores.rdffiledb.rdffilestore import RDFFileStore
from opencefadb.utils import download_file

logger = logging.getLogger("opencefadb")
__this_dir__ = pathlib.Path(__file__).parent
CONFIG_DIR = __this_dir__

_db_instance = None


def _parse_to_qname(uri: rdflib.URIRef):
    uri_str = str(uri)
    for prefix, namespace in RDFFileStore.namespaces.items():
        if namespace in uri_str:
            return f"{prefix}:{uri_str.replace(namespace, '')}"
    return uri_str


class QueryResult:

    def __init__(self, query: Query, result: Any):
        self.query = query
        self.result = result


class OpenCeFaDB(GenericLinkedDatabase):

    def __init__(
            self,
            metadata_store: RDFStore,
            hdf_store: DataStore,
            working_directory: Union[str, pathlib.Path],
            config_filename: Union[str, pathlib.Path]
    ):
        super().__init__(
            stores={
                "rdf": metadata_store,
                "hdf": hdf_store,
            }
        )
        self.working_directory = pathlib.Path(working_directory)
        self.working_directory.mkdir(parents=True, exist_ok=True)
        self.cache_directory = self.working_directory / ".opencefadb"
        # self.metadata_directory = self.working_directory / "metadata"
        # self.rawdata_directory = self.working_directory / "rawdata"
        # self.working_directory.mkdir(parents=True, exist_ok=True)
        # self.metadata_directory.mkdir(parents=True, exist_ok=True)
        self._initialize(config_filename)

    def _initialize(self, config_filename: Union[str, pathlib.Path], exist_ok=False):
        download_dir = self.cache_directory
        download_dir.mkdir(parents=True, exist_ok=True)
        filenames = download_metadata_datasets(
            _get_metadata_datasets(config_filename, self.working_directory),
            download_dir=download_dir,
            exist_ok=exist_ok,
            allowed_media_types=[MediaType.JSON_LD, MediaType.TURTLE]
        )
        for filename in filenames:
            print("Uploading", filename)
            self.stores.rdf.upload_file(filename)

    @classmethod
    def setup_local_default(
            cls,
            working_directory: Optional[Union[str, pathlib.Path]] = None,
            config_filename: Optional[Union[str, pathlib.Path]] = None):
        from opencefadb.stores import RDFFileStore
        from opencefadb.stores import HDF5SqlDB
        if working_directory is not None:
            working_directory = pathlib.Path(working_directory)
        else:
            working_directory = pathlib.Path.cwd()
        if config_filename is None:
            config_filename = __this_dir__ / "db-dataset-config.ttl"
        else:
            config_filename = pathlib.Path(config_filename)
            if not config_filename.exists():
                raise FileNotFoundError(f"Config file {config_filename.resolve()} does not exist.")
        working_directory.mkdir(parents=True, exist_ok=True)
        return cls(
            metadata_store=RDFFileStore(data_dir=working_directory / "metadata"),
            hdf_store=HDF5SqlDB(),
            working_directory=working_directory,
            config_filename=config_filename
        )

    # def download_metadata(self):
    #     """Downloads metadata files from the metadata directory."""
    #     metadata_store: InMemoryRDFStore = self.stores.rdf
    #     for file in metadata_store.data_dir.glob("*.ttl"):
    #         print(f"> Downloading metadata file: {file.name} ...")

    def upload_hdf(self, filename: pathlib.Path):
        """Uploads a file to all stores in the store manager. Not all stores may support this operation.
        This is then skipped."""
        filename = pathlib.Path(filename)
        if not filename.exists():
            raise FileNotFoundError(f"File {filename} does not exist.")
        hdf_db_id = self.stores.hdf.upload_file(filename)
        # get the metadata:
        import h5rdmtoolbox as h5tbx
        meta_filename = self.metadata_directory / f"{filename.stem}.ttl"
        try:
            with open(meta_filename, "w", encoding="utf-8") as f:
                f.write(h5tbx.serialize(filename, fmt="turtle", indent=2, file_uri="https://local.org/"))
        except Exception as e:
            logger.error(f"Error while generating metadata for {filename}: {e}")
            meta_filename.unlink(missing_ok=True)
            raise e
        self.stores.rdf.upload_file(meta_filename)

        # # now link both items:
        # g = rdflib.Graph()
        # g.parse(meta_filename, format="turtle")
        # sparql = f"""
        # PREFIX hdf5: <http://purl.allotrope.org/ontologies/hdf5/1.8#>
        #
        # SELECT ?h5id
        # WHERE {{
        #     ?h5id a hdf5:File .
        # }}
        # LIMIT 1
        # """
        # result = g.query(sparql)
        # # print(str(result.bindings[0].get(rdflib.Variable("h5id"))))
        # # # TODO: link the resources using what? owl: sameAs?
        # # self.store_manager["rdf_db"].link_resources(hdf_db_id, str(result.bindings[0].get(rdflib.Variable("h5id"))))
        return hdf_db_id

    def linked_upload(self, filename: Union[str, pathlib.Path]):
        raise NotImplemented("Linked upload not yet implemented")

    def select_fan_properties(self):
        def _parse_term(term):
            if isinstance(term, rdflib.URIRef):
                return _parse_to_qname(term)
            if isinstance(term, rdflib.Literal):
                return term.value
            return term

        result = self.execute_query("rdf_db", SELECT_FAN_PROPERTIES)
        result_data = [{str(k): _parse_term(v) for k, v in binding.items()} for binding in result.result.bindings]
        variables = {}
        for data in result_data:
            if data["parameter"] not in variables:
                variables[data["parameter"]] = {}

            _data = data.copy()
            if data["property"] in ("m4i:hasStringValue", "m4i:hasNumericalValue"):
                key = "value"
            else:
                key = data["property"]

            if isinstance(data["value"], str) and "/standard_names/" in data["value"]:
                value = data["value"].split("/standard_names/")[-1]
            else:
                value = data["value"]
            variables[data["parameter"]][key] = value
        for var in variables.values():
            var.pop("rdf:type")
        return pd.DataFrame(variables.values())

    def download_cad_file(self, target_dir: Union[str, pathlib.Path], exist_ok=False):
        """Queries the RDF database for the iges cad file"""
        query_result = SELECT_FAN_CAD_FILE.execute(self.stores.rdf)
        bindings = query_result.data
        assert len(bindings) == 1, f"Expected one CAD file, got {len(bindings)}"
        download_url = bindings["downloadURL"][0]
        _guess_filenames = download_url.rsplit("/", 1)[-1]
        target_dir = pathlib.Path(target_dir)
        target_filename = target_dir / _guess_filenames
        if target_filename.exists() and exist_ok:
            return target_filename
        return download_file(download_url, target_dir / _guess_filenames)

    def select_all(self) -> QueryResult:
        return self.execute_query("rdf_db", SELECT_ALL)

    def select_all_operation_points(self):
        result = self.execute_query("rdf_db", SELECT_ALL_OPERATION_POINTS)
