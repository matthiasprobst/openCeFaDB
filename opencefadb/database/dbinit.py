import enum
import logging
import pathlib
import shutil
from typing import List

import rdflib

from opencefadb import paths
from opencefadb.configuration import get_config
from opencefadb.database import connect_to_database
from opencefadb.utils import download_file, download_multiple_files

__this_dir__ = pathlib.Path(__file__).parent

logger = logging.getLogger("opencefadb")


class ApplicationType(enum.Enum):
    JSON_LD = "application/ld+json"
    TURTLE = "text/turtle"
    IGES = "model/iges"


def _get_metadata_datasets() -> rdflib.Graph:
    src_config_filename = __this_dir__ / "../db-dataset-config.jsonld"

    config_dir = paths["config"].parent
    db_dataset_config = config_dir / f"db-dataset-config.jsonld"
    shutil.copy(src_config_filename, db_dataset_config)

    logger.debug(f"Parsing database dataset config '{db_dataset_config.resolve().absolute()}'...")

    assert db_dataset_config.exists(), f"Database dataset config file not found: {db_dataset_config}"
    g = rdflib.Graph()
    g.parse(source=db_dataset_config, format="json-ld")
    return g


def initialize_database(metadata_directory):
    """Downloads all metadata (jsonld-files) from the known zenodo repositories"""

    download_dir = pathlib.Path(metadata_directory)

    logger.info("Downloading metadata datasets...")

    # use sparql to get all distributions that are of type application/ld+json

    filenames = download_metadata_datasets(
        _get_metadata_datasets(),
        download_dir=download_dir
    )
    cfg = get_config()
    logger.debug("Init the opencefadb...")
    db = connect_to_database(cfg.profile)

    logger.debug("Uploading datasets...")
    cfg = get_config()
    for filename in cfg.metadata_directory.glob("*.jsonld"):
        logger.debug(f"Uploading {filename.stem}...")
        db.rdf.upload_file(filename)
        logger.debug("...done")
    for filename in cfg.metadata_directory.glob("*.ttl"):
        logger.debug(f"Uploading {filename.stem}...")
        db.rdf.upload_file(filename)
        logger.debug("...done")
    logger.debug("...initialization done")
    return filenames


def download_metadata_datasets(
        graph: rdflib.Graph,
        application_types: List[ApplicationType] = [ApplicationType.JSON_LD, ApplicationType.TURTLE],
        download_dir=None,
        n_threads=4) -> List[pathlib.Path]:
    if download_dir is None:
        download_dir = pathlib.Path.cwd()
    else:
        download_dir = pathlib.Path(download_dir)

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
          ?distribution dcat:mediaType "{application_type.value}" .
        }}
        """)
        filenames = []
        download_urls = [str(r[rdflib.Variable("downloadURL")]) for r in res.bindings]
        target_filenames = []

        for r in res.bindings:
            download_url = str(r[rdflib.Variable("downloadURL")])
            identifier = str(r[rdflib.Variable("identifier")])
            if identifier.startswith("http"):
                identifier = identifier.rsplit('.', 1)[-1]
            filename = pathlib.Path(download_url.rsplit('/', 1)[-1])
            target_filename = download_dir / f"{filename.stem}_{identifier}{filename.suffix}"
            target_filenames.append(target_filename)
            filenames.append(target_filename)
            if n_threads == 1:
                download_file(download_url, target_filename.resolve())

        # download_file(download_url, target_filename.resolve())
        all_files.extend(download_multiple_files(download_urls, target_filenames))
    return all_files
