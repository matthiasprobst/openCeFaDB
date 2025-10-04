import dataclasses
import enum
import hashlib
import pathlib
import shutil
from typing import List, Union, Optional

import rdflib
from h5rdmtoolbox.repository.zenodo import ZenodoRecord

from opencefadb import logger
from opencefadb.utils import download_file, download_multiple_files

__this_dir__ = pathlib.Path(__file__).parent


class MediaType(enum.Enum):
    JSON_LD = "application/ld+json"
    TURTLE = "text/turtle"
    IGES = "model/iges"


@dataclasses.dataclass
class DistributionMetadata:
    download_url: str
    size: Optional[str] = None
    checksum: Optional[str] = None
    checksum_algorithm: Optional[str] = None


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


# def initialize_database_depr(metadata_directory):
#     """Downloads all metadata (jsonld-files) from the known zenodo repositories"""
#
#     download_dir = pathlib.Path(metadata_directory)
#
#     logger.info("Downloading metadata datasets...")
#
#     # use sparql to get all distributions that are of type application/ld+json
#
#     filenames = download_metadata_datasets(
#         _get_metadata_datasets(),
#         download_dir=download_dir
#     )
#     cfg = get_config()
#     logger.debug("Init the opencefadb...")
#     db = connect_to_database(cfg.profile)
#
#     logger.debug("Uploading datasets...")
#     cfg = get_config()
#     for filename in cfg.metadata_directory.glob("*.jsonld"):
#         logger.debug(f"Uploading {filename.stem}...")
#         db.rdf.upload_file(filename)
#         logger.debug("...done")
#     for filename in cfg.metadata_directory.glob("*.ttl"):
#         logger.debug(f"Uploading {filename.stem}...")
#         db.rdf.upload_file(filename)
#         logger.debug("...done")
#     logger.debug("...initialization done")
#     return filenames


def initialize_database(metadata_directory):
    download_dir = pathlib.Path(metadata_directory)
    download_dir.mkdir(parents=True, exist_ok=True)
    dcat_dataset_graph = _get_metadata_datasets()
    return download_metadata_datasets(
        dcat_dataset_graph,
        download_dir=download_dir,
        allowed_media_types=[MediaType.JSON_LD, MediaType.TURTLE]
    )


def _url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def get_download_urls_of_metadata_distributions_of_zenodo_record(identifier: str) -> List[DistributionMetadata]:
    _identifier = str(identifier)
    sandbox = "10.5072/zenodo" in _identifier
    record_id = _identifier.rsplit('zenodo.', 1)[-1]
    z = ZenodoRecord(source=int(record_id), sandbox=sandbox)
    return [DistributionMetadata(download_url=file.download_url, size=file.size, checksum=file.checksum,
                                 checksum_algorithm=file.checksum_algorithm) for filename, file in z.files.items() if
            filename.endswith('.ttl') or filename.endswith('.jsonld')]


def _parse_media_type(media_type: Optional[str]) -> Optional[MediaType]:
    media_type = str(media_type)
    if media_type is None:
        return None
    if media_type.startswith("https://"):
        media_type = str(media_type).rsplit('media-types/', 1)[-1]
    elif media_type.startswith("http://"):
        media_type = str(media_type).rsplit('media-types/', 1)[-1]
    try:
        return MediaType(media_type)
    except ValueError:
        return None


def download_metadata_datasets(
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
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX spdx: <http://spdx.org/rdf/terms#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?identifier ?downloadURL ?checksumValue ?checksumAlgorithm ?publisherName ?mediaType
    WHERE {{
      ?dataset a dcat:Dataset .
      ?dataset dct:identifier ?identifier .
      OPTIONAL {{
        ?dataset dcat:distribution ?distribution .
        ?distribution dcat:downloadURL ?downloadURL .
        ?distribution dcat:mediaType ?mediaType .
        OPTIONAL {{
          ?distribution dcat:checksum ?checksum .
          ?checksum spdx:checksumValue ?checksumValue .
          ?checksum spdx:algorithm ?checksumAlgorithm .
        }}
      }}
      OPTIONAL {{
        ?dataset dct:publisher ?publisher .
        ?publisher foaf:Agent ?agent .
        ?agent foaf:name ?publisherName .
      }}
    }}
    """)
    checksums = []
    target_filenames = []
    return_filenames = []
    download_urls = []
    download_flags = []
    for r in res.bindings:
        if _parse_media_type(r.get(rdflib.Variable("mediaType"), None)) not in allowed_media_types:
            logger.info(f"Skipping dataset with media type '{r.get(rdflib.Variable('mediaType'), None)}' ...")
            continue
        else:
            has_distributions = rdflib.Variable("downloadURL") in r
            if not has_distributions and rdflib.Variable("publisherName") in r:
                distributions = get_download_urls_of_metadata_distributions_of_zenodo_record(
                    r[rdflib.Variable("identifier")])
            else:
                _checksum = r.get(rdflib.Variable("checksumValue"), None)
                _checksum_algorithm = r.get(rdflib.Variable("checksumAlgorithm"), None)
                if _checksum is None or _checksum_algorithm is None:
                    _checksum = None
                    _checksum_algorithm = None
                    logger.info(f"No checksum information found for dataset '{r[rdflib.Variable('identifier')]}'")
                distributions = [
                    DistributionMetadata(
                        download_url=str(r[rdflib.Variable("downloadURL")]),
                        checksum=_checksum,
                        checksum_algorithm=_checksum_algorithm
                    )
                ]
            logger.debug(f"Found {len(distributions)} distributions.")

            for d in distributions:
                _filename = pathlib.Path(d.download_url.rsplit('/', 1)[-1])
                _suffix = _filename.suffix
                _name = _url_hash(d.download_url) + _suffix
                target_filename = download_dir / _name

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
        for download_url, target_filename, checksum_data, download_flag in zip(download_urls, target_filenames, checksums, download_flags):
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
            target_filenames=[target_filename for target_filename, flag in zip(target_filenames, download_flags) if flag],
            max_workers=n_threads,
            checksums=[checksum for checksum, flag in zip(checksums, download_flags) if flag],
        )
    return target_filenames
