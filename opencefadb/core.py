import enum
import hashlib
import pathlib
import shutil
from dataclasses import dataclass
from typing import List, Union, Optional, Dict

import rdflib
from gldb import GenericLinkedDatabase
from gldb.stores import RDFStore, DataStore
from h5rdmtoolbox.repository.zenodo import ZenodoRecord

from opencefadb import logger
from opencefadb.query_templates.sparql import (
    SELECT_FAN_CAD_FILE
)
from opencefadb.stores.rdf_stores.rdffiledb.rdffilestore import RDFFileStore
from opencefadb.utils import download_file
from opencefadb.utils import download_multiple_files

__this_dir__ = pathlib.Path(__file__).parent
CONFIG_DIR = __this_dir__

_db_instance = None


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

#
# def _parse_to_qualified_name(uri: rdflib.URIRef):
#     """Converts a URI to a qualified name using the namespaces defined in RDFFileStore."""
#     uri_str = str(uri)
#     for prefix, namespace in RDFFileStore.namespaces.items():
#         if namespace in uri_str:
#             return f"{prefix}:{uri_str.replace(namespace, '')}"
#     return uri_str


def _get_url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


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
                _url_hash = _get_url_hash(d.download_url)
                target_dir = download_dir / _url_hash
                target_dir.mkdir(parents=True, exist_ok=True)
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
                    checksum_algorithm=checksum_algorithm)
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


from ontolutils.ex import dcat


@dataclass
class ZenodoRecordInfo:
    record_id: int
    sandbox: bool = False


def parse_local_filenames(filenames: List[Union[str, pathlib.Path]]) -> dcat.Dataset:
    def parse_download_url(download_url: Union[str, pathlib.Path]) -> str:
        return f"file:///{pathlib.Path(download_url).resolve().absolute()}"
    dist = [dcat.Distribution(id=f"https://example.org{du.name}", downloadURL=parse_download_url(du), ) for du in filenames]
    return dcat.Dataset(distribution=dist)


def generate_config(
        zenodo_records: List[Union[ZenodoRecordInfo, Dict]] = None,
        local_filenames: List[Union[str, pathlib.Path]] = None,
        output_config_filename: Union[str, pathlib.Path] = None,
        root_config_filename: Union[str, pathlib.Path] = None
):
    """Generates a database configuration file based on the provided zenodo records."""
    root_config_filename = root_config_filename or (__this_dir__ / "db-dataset-config-sandbox-base.ttl")
    output_config_filename = output_config_filename or (__this_dir__ / "db-dataset-config-sandbox-3.ttl")
    g = rdflib.Graph()
    g.parse(source=root_config_filename, format="ttl")

    for zenodo_record_info in zenodo_records:
        record = ZenodoRecord(
            source=zenodo_record_info["record_id"],
            sandbox=zenodo_record_info["sandbox"]
        )
        g2 = rdflib.Graph()
        g2.parse(data=record.as_dcat_dataset().serialize("ttl"))
        g += g2

    if local_filenames is not None:
        lokal_dataset = parse_local_filenames(local_filenames)
        g3 = rdflib.Graph()
        g3.parse(data=lokal_dataset.serialize("ttl"))
        g += g3

    with open(output_config_filename, "w", encoding="utf-8") as f:
        f.write(g.serialize(format="ttl"))

    return output_config_filename


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
        config_filename = pathlib.Path(config_filename)
        if not config_filename.exists():
            raise FileNotFoundError(f"Config file {config_filename.resolve()} does not exist.")
        self.working_directory = pathlib.Path(working_directory)
        self.working_directory.mkdir(parents=True, exist_ok=True)
        self.cache_directory = self.working_directory / ".opencefadb"
        # self.metadata_directory = self.working_directory / "metadata"
        # self.rawdata_directory = self.working_directory / "rawdata"
        # self.working_directory.mkdir(parents=True, exist_ok=True)
        # self.metadata_directory.mkdir(parents=True, exist_ok=True)
        self._initialize(config_filename)

    def _initialize(self, config_filename: Union[str, pathlib.Path], exist_ok=False):
        """Initializes the database by downloading and uploading metadata files."""
        download_dir = self.cache_directory
        download_dir.mkdir(parents=True, exist_ok=True)
        ttl_filenames = download_dir.glob("*.ttl")
        jsonld_filenames = download_dir.glob("*.jsonld")
        downloaded_filenames = _download_metadata_datasets(
            _get_metadata_datasets(config_filename, self.working_directory),
            download_dir=download_dir,
            exist_ok=exist_ok,
            allowed_media_types=[MediaType.JSON_LD, MediaType.TURTLE]
        )
        filenames = set(list(ttl_filenames) + list(jsonld_filenames) + downloaded_filenames)
        self.stores.rdf.upload_file(config_filename)
        for filename in filenames:
            logger.debug(f"Uploading {filename}")
            self.stores.rdf.upload_file(filename)

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
