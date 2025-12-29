import enum
import os
import pathlib
import shutil
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Union, Optional, Type

import dotenv
import rdflib
import requests
from h5rdmtoolbox import catalog as h5cat
from h5rdmtoolbox.catalog.profiles import IS_VALID_CATALOG_SHACL
from h5rdmtoolbox.repository.zenodo import ZenodoRecord
from ontolutils import Thing
from ontolutils.ex import dcat, hdf5, qudt, sosa, ssn
from ontolutils.ex.sis import StandardMU
from rdflib.namespace import split_uri
from ssnolib import StandardName
from ssnolib.m4i import NumericalVariable

from opencefadb import logger
from opencefadb._core._database_initialization import DownloadStatus, database_initialization
from opencefadb.query_templates import sparql
from opencefadb.query_templates.sparql import get_properties, find_dataset_value_in_same_group_by_other_standard_name, \
    find_datasets_by_standard_name_and_value_range
from opencefadb.utils import download_file, compute_sha256
from opencefadb.utils import download_multiple_files
from opencefadb.utils import remove_none
from .models import Observation
from .models.wikidata import FAN_OPERATING_POINT
from .utils import opencefadb_print

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


def get_and_unpack_property_value_query(uri: str, entity: Type[Thing], metadata_store: h5cat.RDFStore):
    """Evaluates a SPARQL query against the metadata store to retrieve the unit entity for the given unit URI."""
    sparql_query = get_properties(uri)
    res = sparql_query.execute(metadata_store)
    if not res:
        return entity(id=uri)
    df = res.data
    return parse_to_entity(df, uri, entity)


def get_unit_entity(unit_uri: str, metadata_store: h5cat.RDFStore) -> Optional[qudt.Unit]:
    """Evaluates a SPARQL query against the metadata store to retrieve the unit entity for the given unit URI."""
    return get_and_unpack_property_value_query(unit_uri, qudt.Unit, metadata_store)


def get_standard_name_entity(standard_name_uri: str, metadata_store: h5cat.RDFStore) -> Optional[StandardName]:
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


def _download_metadata_datasets(
        graph: rdflib.Graph,
        allowed_media_types=None,
        download_dir=None,
        n_threads=4,
        exist_ok: bool = False) -> List[pathlib.Path]:
    """Downloads all metadata datasets from the given RDF graph.

    If a dataset is a zenodo record, all ttl and jsonld distributions will be downloaded."""
    if allowed_media_types is None:
        allowed_media_types = [None, ]
    if download_dir is None:
        download_dir = pathlib.Path.cwd()
    else:
        download_dir = pathlib.Path(download_dir)

    logger.debug(f"Downloading metadata datasets to '{download_dir.resolve().absolute()}' ...")

    res = graph.query(f"""
    PREFIX dcat: <http://www.w3.org/ns/dcat#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX spdx: <http://spdx.org/rdf/terms#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?dataset ?downloadURL ?checksumValue ?checksumAlgorithm ?publisherName ?mediaType
    WHERE {{
      ?dataset a dcat:Dataset .
      OPTIONAL {{
        ?dataset dcat:distribution ?distribution .
        ?distribution dcat:downloadURL ?downloadURL .
        ?distribution dcat:mediaType ?mediaType .
        OPTIONAL {{
          ?distribution spdx:checksum ?checksum .
          ?checksum spdx:checksumValue ?checksumValue .
          ?checksum spdx:algorithm ?checksumAlgorithm .
        }}
      }}
      OPTIONAL {{
        ?dataset dcterms:publisher ?publisher .
        ?publisher foaf:name ?publisherName .
      }}
    }}
    """)
    checksums = []
    target_filenames = []
    return_filenames = []
    download_urls = []
    download_flags = []

    for r in res.bindings:
        media_type = MediaType.parse(r.get(rdflib.Variable("mediaType"), None))
        if media_type is not None and media_type not in allowed_media_types:
            opencefadb_print(f"Skipping dataset with media type '{media_type}' ...")
            continue
        else:
            has_distributions = rdflib.Variable("downloadURL") in r
            logger.debug(f"Processing dataset '{r[rdflib.Variable('dataset')]}' ...")
            if not has_distributions and rdflib.Variable("publisherName") in r:
                publisher = str(r[rdflib.Variable("publisherName")])
                resource = str(r[rdflib.Variable("ds")])
                logger.debug(
                    f"Getting all distributions related to resource '{resource}' and publisher '{publisher}' ...")

                logger.debug(
                    f"Getting all distributions related to resource '{resource}' and publisher '{publisher}' ...")
                distributions = _get_download_urls_of_metadata_distributions_of_publisher(
                    publisher,
                    resource
                )
            elif has_distributions:
                logger.debug(f"Downloading '{r[rdflib.Variable('downloadURL')]}' ...")
                _checksum = r.get(rdflib.Variable("checksumValue"), None)
                _checksum_algorithm = r.get(rdflib.Variable("checksumAlgorithm"), None)
                if _checksum is None or _checksum_algorithm is None:
                    _checksum = None
                    _checksum_algorithm = None
                    opencefadb_print(f"No checksum information found for dataset '{r[rdflib.Variable('dataset')]}'")
                distributions = [
                    DistributionMetadata(
                        download_url=str(r[rdflib.Variable("downloadURL")]),
                        media_type=media_type,
                        checksum=_checksum,
                        checksum_algorithm=_checksum_algorithm
                    )
                ]
            opencefadb_print(f"Found {len(distributions)} distributions.")

            for d in distributions:
                _filename = pathlib.Path(d.download_url.rsplit('/', 1)[-1])
                _suffix = _filename.suffix
                _url_hash = compute_sha256(d.download_url)
                target_directory = download_dir / _url_hash
                target_directory.mkdir(parents=True, exist_ok=True)
                target_filename = download_dir / _url_hash / _filename.name
                if target_filename.suffix == "":
                    opencefadb_print(d.download_url)
                    opencefadb_print(d.media_type)
                    if d.media_type is not None:
                        target_filename = target_filename.with_suffix(d.media_type.get_suffix())
                if target_filename in target_filenames:
                    logger.debug(f"File {target_filename.name} already in download list, skipping duplicate.")
                    download_flags.append(False)
                elif target_filename.exists() and not exist_ok:
                    download_flags.append(False)
                    logger.debug(f"File {target_filename.name} already exists, skipping download.")
                else:
                    download_flags.append(True)
                target_filenames.append(target_filename)
                download_urls.append(d.download_url)
                checksums.append({"checksum": d.checksum, "checksum_algorithm": d.checksum_algorithm})

    if n_threads == 1:
        for download_url, target_filename, checksum_data, download_flag in zip(download_urls, target_filenames,
                                                                               checksums, download_flags):
            if download_flag:
                checksum = checksum_data.get("checksum", None)
                checksum_algorithm = checksum_data.get("checksum_algorithm", None)
                if target_filename.exists() and not exist_ok:
                    logger.debug(f"File {target_filename.name} already exists, skipping download.")
                    continue
                opencefadb_print(f"Downloading file {target_filename.name} ...")
                return_filenames.append(download_file(
                    download_url,
                    target_filename.resolve(),
                    checksum=checksum,
                    checksum_algorithm=checksum_algorithm
                )
                )
    else:
        download_multiple_files(
            urls=[download_url for download_url, flag in zip(download_urls, download_flags) if flag],
            target_filenames=[target_filename for target_filename, flag in zip(target_filenames, download_flags) if
                              flag],
            max_workers=n_threads,
            checksums=[checksum for checksum, flag in zip(checksums, download_flags) if flag],
        )
    return target_filenames


class OpenCeFaDB(h5cat.CatalogManager):

    def __init__(
            self,
            working_directory: Union[str, pathlib.Path],
            version: str = "latest",
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
            catalog = dcat.Catalog.from_file(source=str(version))[0]
        else:
            catalog = self.__class__.download(
                version,
                sandbox=sandbox,
                target_directory=working_directory
            )
        super().__init__(
            catalog=catalog,
            working_directory=working_directory
        )

    # def __init__(
    #         self,
    #         metadata_store: MetadataStore = None,
    #         hdf_store: DataStore = None
    # ):
    #     wikidata_store = RemoteSparqlStore(endpoint_url="https://query.wikidata.org/sparql", return_format="json")
    #     stores = {
    #         "wikidata": wikidata_store
    #     }
    #     if metadata_store is not None:
    #         stores["rdf"] = metadata_store
    #     if hdf_store is not None:
    #         stores["hdf"] = hdf_store
    #     super().__init__(
    #         stores=stores
    #     )

    # def __repr__(self):
    #     return f"{self.__class__.__name__}(stores={self.stores})"

    # @property
    # def metadata_store(self):
    #     return self.stores.rdf

    # @property
    # def hdf_store(self):
    #     return self.stores.hdf

    # @classmethod
    # def validate_config(cls, config_filename: Union[str, pathlib.Path]) -> bool:
    #     config_graph = rdflib.Graph()
    #     config_graph.parse(source=config_filename, format="ttl")
    #     shacl_graph = rdflib.Graph()
    #     shacl_graph.parse(data=MINIMUM_DATASET_SHACL, format="ttl")
    #     results = pyshacl.validate(
    #         data_graph=config_graph,
    #         shacl_graph=shacl_graph,
    #         inference='rdfs',
    #         abort_on_first=False,
    #         meta_shacl=False,
    #         advanced=True,
    #     )
    #     conforms, results_graph, results_text = results
    #     if not conforms:
    #         warnings.warn("Configuration file does not conform to SHACL shapes.")
    #         opencefadb_print("SHACL validation results:")
    #         opencefadb_print(results_text)

    @classmethod
    def download(cls,
                 version: Optional[str] = None,
                 target_directory: Optional[Union[str, pathlib.Path]] = None,
                 sandbox: bool = False,
                 validate=True) -> dcat.Catalog:
        """Download the catalog (dcat:Catalog)"""
        catalog_filename = _download_catalog(version, target_directory, sandbox)
        catalog = dcat.Catalog.from_file(source=catalog_filename)[0]
        if validate:
            catalog.validate(shacl_data=IS_VALID_CATALOG_SHACL, raise_on_error=True)
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
        opencefadb_print(
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

    # @classmethod
    # def setup_local_default(
    #         cls,
    #         working_directory: Optional[Union[str, pathlib.Path]] = None,
    #         config_filename: Optional[Union[str, pathlib.Path]] = None):
    #     from opencefadb.stores import RDFFileStore
    #     from opencefadb.stores import HDF5SqlDB
    #     if working_directory is not None:
    #         working_directory = pathlib.Path(working_directory)
    #     else:
    #         working_directory = pathlib.Path.cwd()
    #     if config_filename is None:
    #         config_filename = __this_dir__ / "db-dataset-config.ttl"
    #     else:
    #         config_filename = pathlib.Path(config_filename)
    #         if not config_filename.exists():
    #             raise FileNotFoundError(f"Config file {config_filename.resolve()} does not exist.")
    #     working_directory.mkdir(parents=True, exist_ok=True)
    #     return cls(
    #         metadata_store=RDFFileStore(data_dir=working_directory / "metadata"),
    #         hdf_store=HDF5SqlDB(),
    #         working_directory=working_directory,
    #         config_filename=config_filename
    #     )

    # def download_metadata(self):
    #     """Downloads metadata files from the metadata directory."""
    #     metadata_store: InMemoryRDFStore = self.stores.rdf
    #     for file in metadata_store.data_dir.glob("*.ttl"):
    #         opencefadb_print(f"> Downloading metadata file: {file.name} ...")

    # def upload_hdf(self, filename: pathlib.Path):
    #     """Uploads a file to all stores in the store manager. Not all stores may support this operation.
    #     This is then skipped."""
    #     filename = pathlib.Path(filename)
    #     if not filename.exists():
    #         raise FileNotFoundError(f"File {filename} does not exist.")
    #     hdf_db_id = self.stores.hdf.upload_file(filename)
    #     # get the metadata:
    #     meta_filename = self.metadata_directory / f"{filename.stem}.ttl"
    #     try:
    #         with open(meta_filename, "w", encoding="utf-8") as f:
    #             f.write(h5tbx.serialize(filename, fmt="turtle", indent=2, file_uri="https://local.org/"))
    #     except Exception as e:
    #         logger.error(f"Error while generating metadata for {filename}: {e}")
    #         meta_filename.unlink(missing_ok=True)
    #         raise e
    #     self.stores.rdf.upload_file(meta_filename)
    #
    #     # # now link both items:
    #     # g = rdflib.Graph()
    #     # g.parse(meta_filename, format="turtle")
    #     # sparql = f"""
    #     # PREFIX hdf5: <http://purl.allotrope.org/ontologies/hdf5/1.8#>
    #     #
    #     # SELECT ?h5id
    #     # WHERE {{
    #     #     ?h5id a hdf5:File .
    #     # }}
    #     # LIMIT 1
    #     # """
    #     # result = g.query(sparql)
    #     # # opencefadb_print(str(result.bindings[0].get(rdflib.Variable("h5id"))))
    #     # # # TODO: link the resources using what? owl: sameAs?
    #     # # self.store_manager["rdf_db"].link_resources(hdf_db_id, str(result.bindings[0].get(rdflib.Variable("h5id"))))
    #     return hdf_db_id

    # def linked_upload(self, filename: Union[str, pathlib.Path]):
    #     raise NotImplemented("Linked upload not yet implemented")

    def download_cad_file(self, target_directory: Union[str, pathlib.Path], exist_ok=False):
        """Queries the RDF database for the iges cad file"""
        query_result = sparql.SELECT_FAN_CAD_FILE.execute(self.stores.rdf)
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
) -> List[Observation]:
    """This function will make a SPARQL query to get the fan curve data for the given rotational speed (in rpm) considering
    a certain tolerance.

    The SPARQL query identifies alls HDF5 Files (hdf:File) that contain datasets with standard names for:
    - arithmetic_mean_of_fan_rotational_speed
    - arithmetic_mean_of_fan_volume_flow_rate
    - arithmetic_mean_of_difference_of_static_pressure_between_fan_outlet_and_fan_inlet

    and where the value of the arithmetic_mean_of_fan_rotational_speed is within the given tolerance of the
    specified n_rot_speed_rpm.

    the query returns the hdf file, standard name, value, and units for each of the datasets found.

    The function `get_fan_curve_dataseries` transforms this into a list of Observation objects, defined in
    opencefadb.models.observation, where each Observation corresponds to a dataset found in the query.
    """

    n_rot_range = tuple(
        [((1 - n_rot_tolerance) * n_rot_speed_rpm) / 60, ((1 + n_rot_tolerance) * n_rot_speed_rpm) / 60])

    standard_names_of_interest = operating_point_standard_names

    opencefadb_print("> Executing queries (may take a few moments)...")
    st = time.time()
    # find all rotational speed datasets:
    n_rot_query = find_datasets_by_standard_name_and_value_range(
        standard_name_of_rotational_speed,
        n_rot_range
    )

    mean_data = defaultdict(dict)
    # now iterate over all found rotational speed datasets and find the dataset within the same group for the other given standard names:

    for index, row in n_rot_query.execute(rdf_store).data.iterrows():
        for target_standard_name in standard_names_of_interest:
            q_sn = find_dataset_value_in_same_group_by_other_standard_name(
                row["dataset"],
                target_standard_name
            )
            res = q_sn.execute(rdf_store)
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
            q = find_dataset_value_in_same_group_by_other_standard_name(
                row.get("dataset"),
                std_standard_name,
            )
            res = q.execute(rdf_store)
            if len(res) == 1:
                value = float(res.data["value"][0])
                _data["hasUncertaintyDeclaration"] = StandardMU(has_standard_uncertainty=value)

            sn = NumericalVariable.model_validate(remove_none(_data))
            mean_data[row["dataset"]][target_standard_name] = sn
    et = time.time()
    opencefadb_print(f"> ... finished in {et - st:.2f} seconds.")

    observations = []
    for k, v in mean_data.items():
        results = [ssn.Result(hasNumericalVariable=nv) for nv in v.values()]
        hdf_file = hdf5.File(
            id=k,
        )
        observation = sosa.Observation(
            hasFeatureOfInterest=FAN_OPERATING_POINT,
            hasResult=results,
            hadPrimarySource=hdf_file
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
        opencefadb_print(f"Error: could not retrieve Zenodo record: {res.status_code}")
        sys.exit(1)

    if version is None:
        opencefadb_print("Searching for the latest version...")
        links = res.json().get("links", {})
        latest_version_url = links.get("latest", None)
        if latest_version_url is None:
            opencefadb_print("Error: could not retrieve latest version URL from Zenodo record.")
            sys.exit(1)
        res = requests.get(latest_version_url, params={'access_token': access_token} if sandbox else {})
        if res.status_code != 200:
            opencefadb_print(f"Error: could not retrieve latest version record from Zenodo: {res.status_code}")
            sys.exit(1)
        detected_version = res.json()["metadata"]["version"]

        if sandbox:
            target_filename = pathlib.Path(
                target_directory) / f"opencefadb-config-sandbox-{detected_version.replace('.', '-')}.ttl"
        else:
            target_filename = pathlib.Path(
                target_directory) / f"opencefadb-config-{detected_version.replace('.', '-')}.ttl"

        for file in res.json().get("files", []):
            if file["key"] == config_filename:
                opencefadb_print(f"downloading version {detected_version}...")
                file_res = requests.get(file["links"]["self"], params={'access_token': access_token} if sandbox else {})
                with open(target_filename, "wb") as f:
                    f.write(file_res.content)
                opencefadb_print(f"Downloaded latest OpenCeFaDB config file to '{target_filename}'.")
                return target_filename
        opencefadb_print(
            f"Error: Could not find config the file {config_filename} in the latest version of the Zenodo record.")
        sys.exit(1)

    opencefadb_print(f"Searching for version {version}...")
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
        opencefadb_print(f"Error: could not find version {version} in Zenodo record.")
        sys.exit(1)
    res_version = requests.get(found_hit["links"]["self"], params={'access_token': access_token} if sandbox else {})
    for file in res_version.json()["files"]:
        if file["key"] == config_filename:
            opencefadb_print(f"Downloading version {version}...")
            file_res = requests.get(file["links"]["self"], params={'access_token': access_token} if sandbox else {})

            if sandbox:
                target_filename = pathlib.Path(
                    target_directory) / f"opencefadb-config-sandbox-{version.replace('.', '-')}.ttl"
            else:
                target_filename = pathlib.Path(
                    target_directory) / f"opencefadb-config-{version.replace('.', '-')}.ttl"

            with open(target_filename, "wb") as f:
                f.write(file_res.content)
            opencefadb_print(f"Downloaded OpenCeFaDB config file version {version} to '{target_filename}'.")
            return target_filename
    opencefadb_print(f"Error: could not find {config_filename} in the specified version.")
    return None


def get_fan_property(
        rdf_store: h5cat.RDFStore,
        property_standard_name_uri: str
) -> NumericalVariable:
    """Gets a specific fan property from the fan curve data for the given rotational speed (in rpm) with the given tolerance."""
    q = sparql.get_fan_property(property_standard_name_uri)
    res = q.execute(rdf_store)
    return parse_to_entity(
        res.data,
        property_standard_name_uri,
        NumericalVariable
    )
