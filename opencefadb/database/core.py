import logging
import pathlib
from typing import Any
from typing import Union

import pandas as pd
import rdflib
from gldb import GenericLinkedDatabase
from gldb.query import Query
from gldb.stores import DataStoreManager

from opencefadb.configuration import get_config
from opencefadb.database.query_templates.sparql import (
    SELECT_FAN_PROPERTIES,
    SELECT_ALL,
    SELECT_FAN_CAD_FILE,
    SELECT_ALL_OPERATION_POINTS
)
from opencefadb.database.stores.rdf_stores.rdffiledb.rdffilestore import RDFFileStore
from opencefadb.utils import download_file

logger = logging.getLogger("opencefadb")

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

    def __init__(self, store_manager: DataStoreManager):
        self._store_manager = store_manager

    def __repr__(self):
        sotre_names = ", ".join([store_name for store_name in self.store_manager.stores.keys()])
        return f"OpenCeFaDB(store_manager={sotre_names})"

    @property
    def store_manager(self) -> DataStoreManager:
        return self._store_manager

    @property
    def rdf(self) -> RDFFileStore:
        return self.store_manager["rdf_db"]

    @property
    def hdf(self) -> Any:
        return self.store_manager["hdf_db"]

    def upload_hdf(self, filename: pathlib.Path):
        """Uploads a file to all stores in the store manager. Not all stores may support this operation.
        This is then skipped."""
        filename = pathlib.Path(filename)
        hdf_db_id = self.store_manager.stores.get("hdf_db").upload_file(filename)
        # get the metadata:
        cfg = get_config()
        import h5rdmtoolbox as h5tbx
        meta_filename = cfg.metadata_directory / f"{filename.stem}.jsonld"
        with open(meta_filename, "w") as f:
            f.write(h5tbx.dump_jsonld(filename, indent=2, blank_node_iri_base="https://local.org/"))
        self.store_manager.stores.get("rdf_db").upload_file(meta_filename)

        # now link both items:
        g = rdflib.Graph()
        g.parse(meta_filename, format="json-ld")
        sparql = f"""
        PREFIX hdf5: <http://purl.allotrope.org/ontologies/hdf5/1.8#>
        
        SELECT ?h5id
        WHERE {{
            ?h5id a hdf5:File .
        }}
        LIMIT 1
        """
        result = g.query(sparql)
        # print(str(result.bindings[0].get(rdflib.Variable("h5id"))))
        # TODO: link the resources using what? owl: sameAs?
        self.store_manager["rdf_db"].link_resources(hdf_db_id, str(result.bindings[0].get(rdflib.Variable("h5id"))))
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

    def download_cad_file(self, target_dir: Union[str, pathlib.Path]):
        """Queries the RDF database for the iges cad file"""
        query_result = self.execute_query("rdf_db", SELECT_FAN_CAD_FILE)
        bindings = query_result.result.bindings
        assert len(bindings) == 1, f"Expected one CAD file, got {len(bindings)}"
        download_url = bindings[0][rdflib.Variable("downloadURL")]
        _guess_filenames = download_url.rsplit("/", 1)[-1]
        target_dir = pathlib.Path(target_dir)
        return download_file(download_url, target_dir / _guess_filenames)

    def select_all(self) -> QueryResult:
        return self.execute_query("rdf_db", SELECT_ALL)

    def select_all_operation_points(self):
        result = self.execute_query("rdf_db", SELECT_ALL_OPERATION_POINTS)


def connect_to_database(profile) -> OpenCeFaDB:
    """Connects to the database according to the configuration."""
    global _db_instance
    cfg = get_config()
    if cfg.profile == profile:
        if _db_instance:
            return _db_instance
    cfg.select_profile(profile)
    store_manager = DataStoreManager()
    if cfg.rawdata_store == "hdf5_file_db":
        from opencefadb.database.stores.filedb.hdf5filedb import HDF5FileDB
        store_manager.add_store("hdf_db", HDF5FileDB())
    elif cfg.rawdata_store == "hdf5_sql_db":
        from opencefadb.database.stores.filedb.hdf5sqldb import HDF5SqlDB
        store_manager.add_store("hdf_db", HDF5SqlDB())
    else:
        raise TypeError(f"Raw data store '{cfg.rawdata_store}' not (yet) supported. Please check your configuration "
                        f"filename: {cfg.filename}.")
    if cfg.metadata_datastore == "rdf_file_db":
        rdf_file_store = RDFFileStore()
        for filename in cfg.metadata_directory.glob("*.jsonld"):
            rdf_file_store.upload_file(filename)
        for filename in cfg.metadata_directory.glob("*.ttl"):
            rdf_file_store.upload_file(filename)
        store_manager.add_store("rdf_db", rdf_file_store)
    elif cfg.metadata_datastore.lower() == "local_graphdb":
        from opencefadb.database.stores.rdf_stores.graphdb import GraphDBStore
        graphdb_store = GraphDBStore(
            host=cfg["graphdb.host"],
            port=cfg["graphdb.port"],
            user=cfg["graphdb.user"],
            password=cfg["graphdb.password"],
            repository=cfg["graphdb.repository"]
        )
        store_manager.add_store("rdf_db", graphdb_store)
    else:
        raise TypeError(f"Metadata store '{cfg.metadata_datastore}' not supported. Please check your configuration "
                        f"filename: {cfg.filename}.")
    _db_instance = OpenCeFaDB(store_manager)
    cfg.select_profile(profile)
    return _db_instance
