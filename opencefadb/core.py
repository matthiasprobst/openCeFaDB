import enum
import os
import pathlib
import shutil
import sys
import warnings
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Union, Optional, Type, Dict

import dotenv
import pyshacl
import rdflib
import requests
from gldb import GenericLinkedDatabase
from gldb.stores import RDFStore, DataStore, RemoteSparqlStore, MetadataStore
from h5rdmtoolbox.repository.zenodo import ZenodoRecord
from ontolutils import Thing
from ontolutils.ex.qudt import Unit
from rdflib.namespace import split_uri
from ssnolib import StandardName
from ssnolib.m4i import NumericalVariable

from opencefadb import logger
from opencefadb.models import DataSeries
from opencefadb.query_templates.sparql import (
    SELECT_FAN_CAD_FILE
)
from opencefadb.query_templates.sparql import construct_data_based_on_standard_name_based_search_and_range_condition, \
    get_properties
from opencefadb.utils import download_file, compute_sha256
from opencefadb.utils import download_multiple_files
from opencefadb.utils import remove_none
from opencefadb.validation.shacl.templates.dcat import MINIMUM_DATASET_SHACL

__this_dir__ = pathlib.Path(__file__).parent

_db_instance = None

unit_entities = {
    "http://qudt.org/vocab/unit/PA": Unit(
        id="http://qudt.org/vocab/unit/PA",
        name="Pascal",
        symbol="Pa",
        conversionMultiplier=1.0,
    )
}


def get_and_unpack_property_value_query(uri: str, entity: Type[Thing], metadata_store: RDFStore):
    """Evaluates a SPARQL query against the metadata store to retrieve the unit entity for the given unit URI."""
    sparql_query = get_properties(uri)
    res = sparql_query.execute(metadata_store)
    if not res:
        return entity(id=uri)
    df = res.data
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


def get_unit_entity(unit_uri: str, metadata_store: RDFStore) -> Optional[Unit]:
    """Evaluates a SPARQL query against the metadata store to retrieve the unit entity for the given unit URI."""
    return get_and_unpack_property_value_query(unit_uri, Unit, metadata_store)


def get_standard_name_entity(standard_name_uri: str, metadata_store: RDFStore) -> Optional[StandardName]:
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
            logger.info(f"Skipping dataset with media type '{media_type}' ...")
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
                    logger.info(f"No checksum information found for dataset '{r[rdflib.Variable('dataset')]}'")
                distributions = [
                    DistributionMetadata(
                        download_url=str(r[rdflib.Variable("downloadURL")]),
                        media_type=media_type,
                        checksum=_checksum,
                        checksum_algorithm=_checksum_algorithm
                    )
                ]
            logger.debug(f"Found {len(distributions)} distributions.")

            for d in distributions:
                _filename = pathlib.Path(d.download_url.rsplit('/', 1)[-1])
                _suffix = _filename.suffix
                _url_hash = compute_sha256(d.download_url)
                target_directory = download_dir / _url_hash
                target_directory.mkdir(parents=True, exist_ok=True)
                target_filename = download_dir / _url_hash / _filename.name
                if target_filename.suffix == "":
                    print(d.download_url)
                    print(d.media_type)
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
                logger.info(f"Downloading file {target_filename.name} ...")
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


class OpenCeFaDB(GenericLinkedDatabase):

    def __init__(
            self,
            metadata_store: MetadataStore = None,
            hdf_store: DataStore = None
    ):
        wikidata_store = RemoteSparqlStore(endpoint_url="https://query.wikidata.org/sparql", return_format="json")
        stores = {
            "wikidata": wikidata_store
        }
        if metadata_store is not None:
            stores["rdf"] = metadata_store
        if hdf_store is not None:
            stores["hdf"] = hdf_store
        super().__init__(
            stores=stores
        )

    @property
    def metadata_store(self):
        return self.stores.rdf

    @property
    def hdf_store(self):
        return self.stores.hdf

    @classmethod
    def validate_config(cls, config_filename: Union[str, pathlib.Path]) -> bool:
        config_graph = rdflib.Graph()
        config_graph.parse(source=config_filename, format="ttl")
        shacl_graph = rdflib.Graph()
        shacl_graph.parse(data=MINIMUM_DATASET_SHACL, format="ttl")
        results = pyshacl.validate(
            data_graph=config_graph,
            shacl_graph=shacl_graph,
            inference='rdfs',
            abort_on_first=False,
            meta_shacl=False,
            advanced=True,
        )
        conforms, results_graph, results_text = results
        if not conforms:
            warnings.warn("Configuration file does not conform to SHACL shapes.")
            print("SHACL validation results:")
            print(results_text)

    @classmethod
    def pull(cls, version: str | None = None, target_directory: Optional[Union[str, pathlib.Path]] = None,
             sandbox: bool = False):
        return _pull(version, target_directory, sandbox)

    @classmethod
    def initialize(
            cls,
            config_filename: Union[str, pathlib.Path],
            working_directory: Union[str, pathlib.Path] = None
    ):
        from opencefadb._core._database_initialization import database_initialization
        if working_directory is None:
            working_directory = pathlib.Path.cwd()
        download_directory = pathlib.Path(working_directory) / "metadata"
        download_directory.mkdir(parents=True, exist_ok=True)
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
    #         print(f"> Downloading metadata file: {file.name} ...")

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
    #     # # print(str(result.bindings[0].get(rdflib.Variable("h5id"))))
    #     # # # TODO: link the resources using what? owl: sameAs?
    #     # # self.store_manager["rdf_db"].link_resources(hdf_db_id, str(result.bindings[0].get(rdflib.Variable("h5id"))))
    #     return hdf_db_id

    def linked_upload(self, filename: Union[str, pathlib.Path]):
        raise NotImplemented("Linked upload not yet implemented")

    def download_cad_file(self, target_directory: Union[str, pathlib.Path], exist_ok=False):
        """Queries the RDF database for the iges cad file"""
        query_result = SELECT_FAN_CAD_FILE.execute(self.stores.rdf)
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

    def get_fan_curve(
            self,
            n_rot_speed_rpm: float,
            n_rot_tolerance: float = 0.05
    ) -> Dict:
        """Gets the fan curve data for the given rotational speed (in rpm) with the given tolerance."""
        return get_fan_curve_dataseries(
            self.metadata_store,
            n_rot_speed_rpm=n_rot_speed_rpm,
            n_rot_tolerance=n_rot_tolerance
        )


def get_fan_curve_dataseries(
        metadata_store: RDFStore,
        n_rot_speed_rpm: float,
        n_rot_tolerance: float = 0.05
) -> Dict:
    zenodo_record_ns = rdflib.namespace.Namespace(
        "https://doi.org/10.5281/zenodo.17572275#")  # TODO dont hardcode this!
    sn_mean_dp_stat = zenodo_record_ns[
        'standard_name_table/derived_standard_name/arithmetic_mean_of_difference_of_static_pressure_between_fan_outlet_and_fan_inlet']
    sn_mean_vfr = zenodo_record_ns[
        'standard_name_table/derived_standard_name/arithmetic_mean_of_fan_volume_flow_rate']
    sn_mean_nrot = zenodo_record_ns['standard_name_table/derived_standard_name/arithmetic_mean_of_fan_rotational_speed']

    n_rot_range = tuple(
        [((1 - n_rot_tolerance) * n_rot_speed_rpm) / 60, ((1 + n_rot_tolerance) * n_rot_speed_rpm) / 60])

    query = construct_data_based_on_standard_name_based_search_and_range_condition(
        target_standard_name_uris=[sn_mean_vfr, sn_mean_dp_stat, sn_mean_nrot],
        conditional_standard_name_uri=sn_mean_nrot,
        condition_range=n_rot_range
    )

    print(" > Executing query (may take a few moments)...")
    result = query.execute(metadata_store)
    print(f"   - Found {len(result.data)} results.")

    data = defaultdict(dict)

    df = result.data
    for index, row in df.iterrows():
        hdf_file = str(row["hdfFile"])

        standard_term = row.get("standardName") or row.get("dataset")
        standard_name = str(standard_term) if standard_term is not None else None

        val_term = row.get("value")
        units_term = row.get("units")

        try:
            value = val_term.toPython() if val_term is not None else None
        except Exception:
            value = str(val_term) if val_term is not None else None

        try:
            units = units_term.toPython() if units_term is not None else None
        except Exception:
            units = str(units_term) if units_term is not None else None

        if standard_name is not None:
            unit_entity = get_unit_entity(units, metadata_store)
            standard_name_entity = get_standard_name_entity(standard_name, metadata_store)
            _data = {
                "hasNumericalValue": value,
                "hasUnit": unit_entity,
                "hasStandardName": standard_name_entity,
                "hasSymbol": None
            }
            data[hdf_file][standard_name] = NumericalVariable.model_validate(remove_none(_data))
    return dict(data)
    # def safe(v):
    #     if isinstance(v, (str, int, float, bool)) or v is None:
    #         return v
    #     return str(v)
    #
    # serializable = {
    #     hf: {sn.rsplit("/", 1)[-1]: {"hasStandardName": sn,
    #                                  "hasNumericalValue": safe(item["value"]),
    #                                  "hasUnit": safe(item["units"])} for
    #          sn, item in sn_map.items()}
    #     for hf, sn_map in data.items()
    # }
    #
    # _data = []
    # for k, v in serializable.items():
    #     _dataset = []
    #     for sn, value in v.items():
    #         _dataset.append(NumericalVariable.model_validate(remove_none(value)))
    #     _data.append(
    #         NumericalVariable.model_validate(dict(data=_dataset))
    #     )
    # dc = DataSeries.model_validate(dict(datasets=_data))
    # return dc


def _pull(
        version: str | None = None,
        target_directory: str | None = None,
        sandbox: bool = False) -> Optional[pathlib.Path]:
    """download initial config"""
    if target_directory is None:
        target_directory = pathlib.Path.cwd()

    if sandbox:
        base_url = "https://sandbox.zenodo.org/api/records/414371"
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
    if version:
        print(f"Downloading OpenCeFaDB config file version {version} from Zenodo...")
    else:
        print("Downloading the latest OpenCeFaDB config file from Zenodo...")

    # if version is None, get the latest version of the zenodo record and download the file test.ttl, else get the specific version of the record:
    res = requests.get(base_url, params={'access_token': access_token} if sandbox else {})
    if res.status_code != 200:
        print(f"Error: could not retrieve Zenodo record: {res.status_code}")
        sys.exit(1)

    if version is None:
        links = res.json().get("links", {})
        latest_version_url = links.get("latest", None)
        if latest_version_url is None:
            print("Error: could not retrieve latest version URL from Zenodo record.")
            sys.exit(1)
        res = requests.get(latest_version_url, params={'access_token': access_token} if sandbox else {})
        if res.status_code != 200:
            print(f"Error: could not retrieve latest version record from Zenodo: {res.status_code}")
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
                print(f"downloading version {detected_version}...")
                file_res = requests.get(file["links"]["self"], params={'access_token': access_token} if sandbox else {})
                with open(target_filename, "wb") as f:
                    f.write(file_res.content)
                print(f"Downloaded latest OpenCeFaDB config file to '{target_filename}'.")
                return target_filename
        print("Error: could not find config file in the latest version of the Zenodo record.")
        sys.exit(1)

    # a specific version is given:
    found_hit = None
    version_hits = \
        requests.get(res.json()["links"]["versions"], params={'access_token': access_token} if sandbox else {}).json()[
            "hits"]["hits"]
    for hit in version_hits:
        if hit["metadata"]["version"] == version:
            found_hit = hit
            break
    if not found_hit:
        print(f"Error: could not find version {version} in Zenodo record.")
        sys.exit(1)
    res_version = requests.get(found_hit["links"]["self"], params={'access_token': access_token} if sandbox else {})
    for file in res_version.json()["files"]:
        if file["key"] == config_filename:
            print(f"downloading version {version}...")
            file_res = requests.get(file["links"]["self"], params={'access_token': access_token} if sandbox else {})

            if sandbox:
                target_filename = pathlib.Path(
                    target_directory) / f"opencefadb-config-sandbox-{version.replace('.', '-')}.ttl"
            else:
                target_filename = pathlib.Path(
                    target_directory) / f"opencefadb-config-{version.replace('.', '-')}.ttl"

            with open(target_filename, "wb") as f:
                f.write(file_res.content)
            print(f"Downloaded OpenCeFaDB config file version {version} to '{target_filename}'.")
            return target_filename
    print(f"Error: could not find {config_filename} in the specified version.")
    return None
