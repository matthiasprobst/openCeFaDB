import enum
import os
import pathlib
import shutil
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Union, Optional, Type

import dotenv
import rdflib
import requests
import requests.exceptions
import tqdm
from h5rdmtoolbox import catalog as h5cat
from h5rdmtoolbox.catalog import InMemoryRDFStore, GraphDB, HDF5FileStore
from h5rdmtoolbox.catalog.profiles import IS_VALID_CATALOG_SHACL
from h5rdmtoolbox.repository.zenodo import ZenodoRecord
from ontolutils import Thing
from ontolutils.ex import dcat, hdf5, qudt, sosa, ssn
from ontolutils.ex.sis import StandardMU
from ontolutils.ex.sosa import Observation
from rdflib.namespace import split_uri
from ssnolib import StandardName
from ssnolib.m4i import NumericalVariable

from opencefadb import logger
from opencefadb import sparql_templates
from opencefadb._core._database_initialization import DownloadStatus, database_initialization
# from opencefadb.sparql_templates.sparql import get_properties, find_dataset_value_in_same_group_by_other_standard_name, \
#     find_datasets_by_standard_name_and_value_range
from opencefadb.utils import download_file, remove_none
from .models.wikidata import FAN_OPERATING_POINT
from .utils import opencefa_print

__this_dir__ = pathlib.Path(__file__).parent

_db_instance = None

unit_entities = {
    "http://qudt.org/vocab/unit/PA": qudt.Unit(
        id="http://qudt.org/vocab/unit/PA",
        name="Pascal",
        symbol="Pa",
        conversionMultiplier=1.0,
    )
}


def parse_to_entity(df, uri, entity: Type[Thing]):
    # check if columns are "property" and "value":
    if "property" not in df.columns or "value" not in df.columns:
        raise ValueError("DataFrame must contain 'property' and 'value' columns.")
    _grouped_dict = df.groupby("property")["value"].apply(list).to_dict()
    # extract the value from the uri, which is in the key
    _data_dict = {
        "id": uri
    }
    for k, v in _grouped_dict.items():
        _, key = split_uri(k)
        if len(v) == 1:
            _data_dict[key] = v[0]
        else:
            _data_dict[key] = v
    return entity.model_validate(_data_dict)


def get_and_unpack_property_value_query(uri: str, entity: Type[Thing], metadata_store: h5cat.RDFStore) -> Thing:
    """Evaluates a SPARQL query against the metadata store to retrieve the unit entity for the given unit URI."""
    sparql_query = sparql_templates.get_properties(uri)
    res = sparql_query.execute(metadata_store)
    if not res:
        return entity(id=uri)
    df = res.data
    return parse_to_entity(df, uri, entity)


def get_unit_entity(unit_uri: str, metadata_store: h5cat.RDFStore) -> Optional[qudt.Unit]:
    """Evaluates a SPARQL query against the metadata store to retrieve the unit entity for the given unit URI."""
    return get_and_unpack_property_value_query(unit_uri, qudt.Unit, metadata_store)


def get_standard_name_entity(standard_name_uri: str, metadata_store: h5cat.RDFStore) -> Thing:
    return get_and_unpack_property_value_query(standard_name_uri, StandardName, metadata_store)


@dataclass
class DistributionMetadata:
    download_url: str
    media_type: Optional[str] = None
    size: Optional[str] = None
    checksum: Optional[str] = None
    checksum_algorithm: Optional[str] = None


class MediaType(enum.Enum):
    JSON_LD = "application/ld+json"
    TURTLE = "text/turtle"
    IGES = "model/iges"
    IGS = "igs"
    CSV = "text/csv"
    TXT = "text/plain"
    XML = "application/rdf+xml"
    XML2 = "application/xml"
    HDF5 = "application/x-hdf5"

    @classmethod
    def parse(cls, media_type: str):
        media_type = str(media_type)
        if media_type is None:
            return None
        if media_type.startswith("https://"):
            media_type = str(media_type).rsplit('media-types/', 1)[-1]
        elif media_type.startswith("http://"):
            media_type = str(media_type).rsplit('media-types/', 1)[-1]
        try:
            return cls(media_type)
        except ValueError:
            return None

    def get_suffix(self):
        if self == MediaType.JSON_LD:
            return ".jsonld"
        elif self == MediaType.HDF5:
            return ".hdf5"
        elif self == MediaType.TURTLE:
            return ".ttl"
        elif self in (MediaType.IGES, MediaType.IGS):
            return ".igs"
        elif self == MediaType.CSV:
            return ".csv"
        elif self == MediaType.TXT:
            return ".txt"
        elif self == MediaType.XML:
            return ".xml"
        elif self == MediaType.XML2:
            return ".xml"
        else:
            return ""


def _get_download_urls_of_metadata_distributions_of_publisher(
        publisher: str,
        doi: str
):
    publisher = str(publisher)
    if publisher.lower() != "zenodo":
        raise ValueError(f"Unsupported publisher: {publisher}")
    return _get_download_urls_of_metadata_distributions_of_zenodo_record(doi)


def _get_download_urls_of_metadata_distributions_of_zenodo_record(doi: str) -> List[DistributionMetadata]:
    _doi = str(doi)
    sandbox = "10.5072/zenodo" in _doi
    record_id = _doi.rsplit('zenodo.', 1)[-1]
    z = ZenodoRecord(source=int(record_id), sandbox=sandbox)
    return [DistributionMetadata(
        download_url=file.download_url,
        size=file.size,
        media_type=file.media_type,
        checksum=file.checksum,
        checksum_algorithm=file.checksum_algorithm or "md5"
    ) for filename, file in
        z.files.items() if
        filename.endswith('.ttl') or filename.endswith('.jsonld')]


def _get_metadata_datasets(
        config_filename: Union[str, pathlib.Path],
        config_dir: Union[str, pathlib.Path]
) -> rdflib.Graph:
    """Parses the configuration file and returns the graph with metadata datasets.
    The configuration file should contain DCAT descriptions of datasets.

    Parameters
    ----------
    config_filename : str or pathlib.Path
        The path to the configuration file (e.g., config.ttl or config.jsonld).
    config_dir : str or pathlib.Path
        The directory where the configuration file will be copied to and parsed from.
    """
    config_filename = pathlib.Path(config_filename)
    if not config_filename.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_filename}")
    config_dir = pathlib.Path(config_dir)
    if not config_dir.exists():
        raise FileNotFoundError(f"Configuration directory not found: {config_dir}")
    config_suffix = config_filename.suffix
    db_dataset_config_filename = config_dir / f"db-dataset-config.{config_suffix}"
    shutil.copy(config_filename, db_dataset_config_filename)

    logger.debug(f"Parsing database dataset config '{db_dataset_config_filename.resolve().absolute()}'...")

    if config_suffix == '.ttl':
        fmt = "ttl"
    elif config_suffix in ('.json', '.jsonld', '.json-ld'):
        fmt = "json-ld"
    else:
        raise ValueError(f"Unsupported config file suffix: {config_suffix}")
    g = rdflib.Graph()
    g.parse(source=db_dataset_config_filename, format=fmt)
    logger.debug("Successfully parsed database dataset config.")
    return g


class OpenCeFaDB(h5cat.CatalogManager):

    def __init__(
            self,
            working_directory: Union[str, pathlib.Path],
            version: Union[str, pathlib.Path] = "latest",
            sandbox: bool = False
    ):
        """Initializes the OpenCeFaDB database.

        Parameters
        ----------
        working_directory : Union[str, pathlib.Path]
            The working directory where the database files will be stored.
        version : str, optional
            The version of the catalog to use. This can be a version string like "1.0.0", "latest", or a path to a local
            catalog file. Default is "latest".
        sandbox : bool, optional
            Whether to use the sandbox version of the catalog. Default is False.
        """
        if pathlib.Path(version).exists():
            opencefa_print("Using local catalog file as version.")
            catalog = dcat.Catalog.from_file(source=str(version))[0]
        else:
            catalog = self.__class__.download_catalog(
                version,
                sandbox=sandbox,
                target_directory=working_directory
            )
        super().__init__(
            catalog=catalog,
            working_directory=working_directory
        )

    @classmethod
    def from_graphdb_setup(
            cls,
            *,
            working_directory: Union[str, pathlib.Path],
            version: Union[str, pathlib.Path] = "latest",
            sandbox: bool = False,
            endpoint: str = "http://localhost:7200",
            repository: str = "OpenCeFaDB",
            username: str = None,
            password: str = None,
            add_wikidata_store: bool = False
    ) -> "OpenCeFaDB":
        try:
            gdb = GraphDB(
                endpoint=endpoint,
                repository=repository,
                username=username,
                password=password
            )
            gdb.get_repository_info(repository)
        except requests.exceptions.ConnectionError as e:
            raise RuntimeError(f"GraphDB not available: {e}")
        gdb_exists = gdb.get_repository_info(repository)
        if not gdb_exists:
            raise RuntimeError(f"GraphDB repository '{repository}' does not exist.")
        db = cls(working_directory=working_directory, version=version, sandbox=sandbox)
        db.add_main_rdf_store(gdb)
        if add_wikidata_store:
            db.add_wikidata_store(augment_main_rdf_store=True)
        db.add_hdf_infile_index()
        return db

    @classmethod
    def from_rdflib_setup(
            cls,
            *,
            working_directory: Union[str, pathlib.Path],
            version: Union[str, pathlib.Path] = "latest",
            sandbox: bool = False,
            add_wikidata_store: bool = False
    ) -> "OpenCeFaDB":
        working_directory = pathlib.Path(working_directory)
        working_directory.mkdir(parents=True, exist_ok=True)
        db = cls(working_directory=working_directory, version=version, sandbox=sandbox)
        InMemoryRDFStore.__populate_on_init__ = False
        local_rdf_store = InMemoryRDFStore(
            data_dir=working_directory / "rdf",
            recursive_exploration=True,
            formats=["ttl"]
        )
        db.add_main_rdf_store(local_rdf_store)
        db.download_metadata()
        _local_graph_file = working_directory / ".graph.nt"
        if _local_graph_file.exists():
            g = rdflib.Graph().parse(_local_graph_file)
            local_rdf_store.graph += g
        hdf_store = HDF5FileStore(data_directory=working_directory / "hdf")
        db.add_hdf_store(hdf_store)
        if add_wikidata_store:
            db.add_wikidata_store(augment_main_rdf_store=True)
        db.add_hdf_infile_index()
        return db

    @classmethod
    def download_catalog(cls,
                         version: Optional[str] = None,
                         target_directory: Optional[Union[str, pathlib.Path]] = None,
                         sandbox: bool = False,
                         validate=True) -> dcat.Catalog:
        """Download the catalog (dcat:Catalog)"""
        catalog_filename = _download_catalog(version, target_directory, sandbox)
        catalog = dcat.Catalog.from_file(source=catalog_filename)[0]
        if validate:
            opencefa_print("Validating downloaded catalog against SHACL shapes...")
            catalog.validate(shacl_data=IS_VALID_CATALOG_SHACL, raise_on_error=True)
            opencefa_print("Catalog is valid.")
        return catalog

    @classmethod
    def initialize(
            cls,
            config_filename: Union[str, pathlib.Path],
            working_directory: Union[str, pathlib.Path] = None
    ) -> List[DownloadStatus]:
        if working_directory is None:
            working_directory = pathlib.Path.cwd()
        download_directory = pathlib.Path(working_directory) / "metadata"
        download_directory.mkdir(parents=True, exist_ok=True)
        opencefa_print(
            f"Copying the config file {config_filename} to the target directory {download_directory.resolve()} ..."
        )
        shutil.copy(
            config_filename,
            download_directory / pathlib.Path(config_filename).name
        )
        return database_initialization(
            config_filename=config_filename,
            download_directory=download_directory
        )

    @classmethod
    def get_config(cls, sandbox=False) -> pathlib.Path:
        if sandbox:
            return __this_dir__ / "data/opencefadb-config-sandbox.ttl"
        return __this_dir__ / "data/opencefadb-config.ttl"

    @property
    def rdf_store(self):
        """The database's main RDF store."""
        return self.main_rdf_store

    def add_hdf_infile_index(self):
        if hasattr(self.main_rdf_store, 'graph'):
            from .utils import build_infile_index_via_parents_for_graph
            idx_g = build_infile_index_via_parents_for_graph(self.main_rdf_store.graph, include_rootgroup=True)
            for s, p, o in tqdm.tqdm(idx_g, desc="Uploading HDF5 infile index triples", unit=" triples"):
                self.main_rdf_store.upload_triple((s, p, o))
        else:
            # is graphdb?!
            from .utils import build_infile_index_via_parents_for_graphdb
            build_infile_index_via_parents_for_graphdb(self.main_rdf_store)

    def download_cad_file(self, target_directory: Union[str, pathlib.Path], exist_ok=False):
        """Queries the RDF database for the iges cad file"""
        query_result = sparql_templates.fan.SELECT_FAN_CAD_FILE.execute(self.main_rdf_store)
        bindings = query_result.data
        n_bindings = len(bindings)
        if n_bindings != 1:
            raise ValueError(f"Expected one CAD file, got {n_bindings}")
        download_url = bindings["downloadURL"][0]
        _guess_filenames = download_url.rsplit("/", 1)[-1]
        target_directory = pathlib.Path(target_directory)
        target_filename = target_directory / _guess_filenames
        if target_filename.exists() and exist_ok:
            return target_filename
        return download_file(download_url, target_directory / _guess_filenames)

    # database specific methods:
    def get_fan_properties(self):
        from .sparql_templates.fan import SELECT_FAN_PROPERTIES
        res = SELECT_FAN_PROPERTIES.execute(self.main_rdf_store)
        return res.data  # {str(it["parameter"]): it for it in res.data.to_dict("records")}

    def get_fan_property(
            self,
            property_standard_name_uri: str
    ) -> NumericalVariable:
        """Gets a specific fan property from the fan curve data for the given rotational speed (in rpm) with the given tolerance."""
        q = sparql_templates.fan.get_fan_property(property_standard_name_uri)
        res = q.execute(self.main_rdf_store)
        return parse_to_entity(
            res.data,
            property_standard_name_uri,
            NumericalVariable
        )

    def get_operating_point_observations(
            self,
            operating_point_standard_names,
            standard_name_of_rotational_speed: str,
            n_rot_speed_rpm: float,
            n_rot_tolerance: float = 0.05
    ) -> List[Observation]:
        """Gets the fan curve data for the given rotational speed (in rpm) with the given tolerance
        and infer the OperatingPoint (SemanticOperatingPoint) objects from it

        ."""
        return get_operating_point_observations(
            self.main_rdf_store,
            operating_point_standard_names,
            standard_name_of_rotational_speed,
            n_rot_speed_rpm=n_rot_speed_rpm,
            n_rot_tolerance=n_rot_tolerance
        )


def get_operating_point_observations(
        rdf_store: h5cat.RDFStore,
        operating_point_standard_names,
        standard_name_of_rotational_speed,
        n_rot_speed_rpm: float,
        n_rot_tolerance: float = 0.05
):
    n_rot_range = tuple(
        [((1 - n_rot_tolerance) * n_rot_speed_rpm) / 60.,
         ((1 + n_rot_tolerance) * n_rot_speed_rpm) / 60.]
    )
    q_n_rot = sparql_templates.hdf.find_dataset_for_standard_name(standard_name_of_rotational_speed, n_rot_range)
    standard_names_of_interest = operating_point_standard_names

    mean_data = defaultdict(dict)

    res_n_rot = q_n_rot.execute(rdf_store)

    if len(res_n_rot.data) > 0:
        for dataset_uri in tqdm.tqdm(res_n_rot.data["dataset"], desc="Processing operating point datasets",
                                     unit=" datasets"):
            q = sparql_templates.hdf.find_hdf5_file_for_dataset(dataset_uri)
            res_hdf_file = q.execute(rdf_store)
            hdf_file_uri = res_hdf_file.data.iloc[0]["hdfFile"]

            for target_standard_name in standard_names_of_interest:
                q = sparql_templates.hdf.find_dataset_in_file_by_standard_name(hdf_file_uri, target_standard_name)
                res = q.execute(rdf_store)
                if len(res) > 1:
                    raise ValueError(f"Expected one dataset for standard name {target_standard_name}, got {len(res)}")
                if len(res) == 0:
                    continue

                target_data = res.data.iloc[0]
                units_term = target_data.get("units")
                if units_term:
                    try:
                        units = units_term.toPython() if units_term is not None else None
                    except Exception:
                        units = str(units_term) if units_term is not None else None
                    unit_entity = get_unit_entity(units, rdf_store)
                else:
                    unit_entity = None
                standard_name_entity = get_standard_name_entity(target_standard_name, rdf_store)
                _data = {
                    "hasNumericalValue": target_data.get("value"),
                    "hasUnit": unit_entity,
                    "hasStandardName": standard_name_entity,
                    "hasSymbol": target_data.get("hasSymbol"),
                    "label": target_data.get("label"),
                    "altLabel": target_data.get("altLabel"),
                    "hasMinimumValue": target_data.get("hasMinimumValue"),
                    "hasMaximumValue": target_data.get("hasMaximumValue")
                }
                # find standard deviation counterpart:
                std_standard_name = target_standard_name.replace("arithmetic_mean_of", "standard_deviation_of")
                q = sparql_templates.hdf.find_dataset_in_file_by_standard_name(
                    dataset_uri,
                    std_standard_name,
                )
                res = q.execute(rdf_store)
                if len(res) == 1:
                    value = float(res.data["value"][0])
                    _data["hasUncertaintyDeclaration"] = StandardMU(has_standard_uncertainty=value)

                sn = NumericalVariable.model_validate(remove_none(_data))
                mean_data[dataset_uri][target_standard_name] = sn
                mean_data[dataset_uri]["hdf_uri"] = hdf_file_uri

    observations = []
    for k, v in mean_data.items():
        _hdf_uri = v.pop("hdf_uri")
        results = [ssn.Result(hasNumericalVariable=nv) for nv in v.values()]
        hdf_dist = dcat.Distribution(
            id=_hdf_uri,
            media_type="application/x-hdf5",
            download_URL=_hdf_uri
        )
        observation = sosa.Observation(
            hasFeatureOfInterest=FAN_OPERATING_POINT,
            hasResult=results,
            hadPrimarySource=hdf_dist
        )
        observations.append(observation)

    return observations


def _download_catalog(
        version: Optional[str] = None,
        target_directory: Optional[str] = None,
        sandbox: bool = False) -> Optional[pathlib.Path]:
    """download initial config"""
    if target_directory is None:
        target_directory = pathlib.Path.cwd()

    if sandbox:
        base_url = "https://sandbox.zenodo.org/api/records/419419"
        dotenv.load_dotenv(pathlib.Path.cwd() / ".env", override=True)
        access_token = os.getenv("ZENODO_SANDBOX_API_TOKEN", None)
        config_filename = "opencefadb-config-sandbox.ttl"
    else:
        access_token = None
        base_url = "https://zenodo.org/api/records/14551649"
        config_filename = "opencefadb-config.ttl"
    pathlib.Path(target_directory).mkdir(parents=True, exist_ok=True)

    if version is not None and version.lower().strip() == "latest":
        version = None

    # if version is None, get the latest version of the zenodo record and download the file test.ttl, else get the specific version of the record:
    res = requests.get(base_url, params={'access_token': access_token} if sandbox else {})
    if res.status_code != 200:
        opencefa_print(f"Error: could not retrieve Zenodo record: {res.status_code}")
        raise ValueError(f"Error: could not retrieve Zenodo record: {res.status_code}")

    if version is None:
        opencefa_print("Searching for the latest version...")
        links = res.json().get("links", {})
        latest_version_url = links.get("latest", None)
        if latest_version_url is None:
            opencefa_print("Error: could not retrieve latest version URL from Zenodo record.")
            raise ValueError("Error: could not retrieve latest version URL from Zenodo record.")
        res = requests.get(latest_version_url, params={'access_token': access_token} if sandbox else {})
        if res.status_code != 200:
            opencefa_print(f"Error: could not retrieve latest version record from Zenodo: {res.status_code}")
            raise ValueError(f"Error: could not retrieve latest version record from Zenodo: {res.status_code}")
        detected_version = res.json()["metadata"]["version"]

        if sandbox:
            target_filename = pathlib.Path(
                target_directory) / f"opencefadb-config-sandbox-{detected_version.replace('.', '-')}.ttl"
        else:
            target_filename = pathlib.Path(
                target_directory) / f"opencefadb-config-{detected_version.replace('.', '-')}.ttl"

        for file in res.json().get("files", []):
            if file["key"] == config_filename:
                opencefa_print(f"downloading version {detected_version}...")
                file_res = requests.get(file["links"]["self"], params={'access_token': access_token} if sandbox else {})
                with open(target_filename, "wb") as f:
                    f.write(file_res.content)
                opencefa_print(f"Downloaded latest OpenCeFaDB config file to '{target_filename}'.")
                return target_filename
        opencefa_print(
            f"Error: Could not find config the file {config_filename} in the latest version of the Zenodo record.")
        raise ValueError(
            f"Error: Could not find config the file {config_filename} in the latest version of the Zenodo record.")

    opencefa_print(f"Searching for version {version}...")
    # a specific version is given:
    found_hit = None
    version_hits = requests.get(
        res.json()["links"]["versions"], params={'access_token': access_token} if sandbox else {}
    ).json()["hits"]["hits"]
    for hit in version_hits:
        if hit["metadata"]["version"] == version:
            found_hit = hit
            break
    if not found_hit:
        opencefa_print(f"Error: could not find version {version} in Zenodo record.")
        raise ValueError(f"Error: could not find version {version} in Zenodo record.")
    res_version = requests.get(found_hit["links"]["self"], params={'access_token': access_token} if sandbox else {})
    for file in res_version.json()["files"]:
        if file["key"] == config_filename:
            opencefa_print(f"Downloading version {version}...")
            file_res = requests.get(file["links"]["self"], params={'access_token': access_token} if sandbox else {})

            if sandbox:
                target_filename = pathlib.Path(
                    target_directory) / f"opencefadb-config-sandbox-{version.replace('.', '-')}.ttl"
            else:
                target_filename = pathlib.Path(
                    target_directory) / f"opencefadb-config-{version.replace('.', '-')}.ttl"

            with open(target_filename, "wb") as f:
                f.write(file_res.content)
            opencefa_print(f"Downloaded OpenCeFaDB config file version {version} to '{target_filename}'.")
            return target_filename
    opencefa_print(f"Error: could not find {config_filename} in the specified version.")
    return None
