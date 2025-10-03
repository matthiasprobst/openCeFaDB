import enum
import hashlib
import pathlib
import shutil
from typing import List, Union

import rdflib

from opencefadb import logger
from opencefadb.utils import download_file, download_multiple_files

__this_dir__ = pathlib.Path(__file__).parent


class ApplicationType(enum.Enum):
    JSON_LD = "application/ld+json"
    TURTLE = "text/turtle"
    IGES = "model/iges"


def _get_metadata_datasets(
        config_filename: Union[str, pathlib.Path],
        config_dir: Union[str, pathlib.Path]
) -> rdflib.Graph:
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
    return download_metadata_datasets(
        _get_metadata_datasets(),
        download_dir=download_dir,
    )


def url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def download_metadata_datasets(
        graph: rdflib.Graph,
        application_types=None,
        download_dir=None,
        n_threads=4,
        exist_ok: bool = False) -> List[pathlib.Path]:
    if application_types is None:
        application_types = [ApplicationType.JSON_LD, ApplicationType.TURTLE]
    if download_dir is None:
        download_dir = pathlib.Path.cwd()
    else:
        download_dir = pathlib.Path(download_dir)

    print(f"Downloading metadata datasets to '{download_dir.resolve().absolute()}' ...")
    logger.debug(f"Downloading metadata datasets to '{download_dir.resolve().absolute()}' ...")

    all_files = []
    for application_type in application_types:
        res = graph.query(f"""
        PREFIX dcat: <http://www.w3.org/ns/dcat#>
        PREFIX dct: <http://purl.org/dc/terms/>
        
        SELECT ?identifier ?downloadURL
        WHERE {{
          ?dataset a dcat:Dataset .
          ?dataset dcat:distribution ?distribution .
          ?dataset dct:identifier ?identifier .
          ?distribution dcat:downloadURL ?downloadURL .
          ?distribution dcat:mediaType ?mediaType .
          FILTER (
            ?mediaType = "{application_type.value}" ||
            ?mediaType = <https://www.iana.org/assignments/media-types/{application_type.value}>
          )
        }}
        """)
        download_urls = [str(r[rdflib.Variable("downloadURL")]) for r in res.bindings]
        logger.debug(f"Found {len(download_urls)} datasets of type '{application_type.value}'.")
        target_filenames = []

        for r in res.bindings:
            download_url = str(r[rdflib.Variable("downloadURL")])
            # identifier = str(r[rdflib.Variable("identifier")])

            _filename = pathlib.Path(download_url.rsplit('/', 1)[-1])
            _suffix = _filename.suffix
            _name = url_hash(download_url) + _suffix
            target_filename = download_dir / _name

            # if identifier.startswith("http"):
            #     identifier = identifier.rsplit('.', 1)[-1]
            # target_filename = download_dir / f"{filename.stem}_{identifier}{filename.suffix}"
            target_filenames.append(target_filename)

            if target_filename.exists() and not exist_ok:
                continue
            if n_threads == 1:
                all_files.append(download_file(download_url, target_filename.resolve()))

        if not exist_ok:
            _download_urls = []
            _target_filenames = []
            for u, t in zip(download_urls, target_filenames):
                if t.exists():
                    logger.debug(f"File {t.name} already exists, skipping download.")
                else:
                    logger.info(f"Downloading file {t.name} ...")
                    _download_urls.append(u)
                    _target_filenames.append(t)
            if len(_download_urls) > 0:
                all_files.extend(download_multiple_files(_download_urls, _target_filenames))
        else:
            all_files.extend(download_multiple_files(download_urls, target_filenames))

    return all_files
