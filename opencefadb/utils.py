import enum
import hashlib
import logging
import pathlib
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union, Optional, Dict

import rdflib
import requests
from tqdm import tqdm

logger = logging.getLogger("opencefadb")


class ExportFormat(enum.Enum):
    JSON_LD = "json-ld"
    DCAT = "dcat-ap"
    JSON = "json"
    CODEMETA = "Codemeta"
    CFF = "cff"


def _parse_checksum_algorithm(algorithm: str) -> str:
    algorithm = str(algorithm)
    if algorithm in ("sha256", "sha-256", "sha_256"):
        return "sha256"
    elif algorithm in ("md5", "md-5", "md_5"):
        return "md5"
    elif algorithm in ("sha1", "sha-1", "sha_1"):
        return "sha1"
    if "spdx.org/rdf/terms#" in algorithm:
        return algorithm.rsplit("_", 1)[-1]
    elif algorithm.startswith("http:"):
        return algorithm.rsplit('/', 1)[-1].lower()
    raise ValueError(f"Unsupported checksum algorithm: {algorithm}")


def download_file(download_url,
                  target_filename,
                  checksum: str = None,
                  checksum_algorithm: str = None) -> pathlib.Path:
    """Downloads from a URL"""
    if checksum is not None:
        if checksum_algorithm is None:
            raise ValueError("If checksum is provided, checksum_algorithm must also be provided.")
        checksum_algorithm = _parse_checksum_algorithm(checksum_algorithm)
        checksum = {'value': checksum, 'algorithm': checksum_algorithm}
    target_filename = pathlib.Path(target_filename)
    logger.debug(f"Downloading metadata file from URL {download_url}...")
    response = requests.get(download_url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get('content-length', 0))  # Get total file size
    chunk_size = 1024  # Define chunk size for download

    hasher = None
    if checksum is not None:
        try:
            hasher = hashlib.new(checksum_algorithm)
        except ValueError:
            raise ValueError(f"Unsupported checksum algorithm: {checksum_algorithm}")

    with tqdm(total=total_size, unit='B', unit_scale=True, desc=target_filename.name, initial=0) as progress:
        with open(target_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)
                    progress.update(len(chunk))
                    if hasher:
                        hasher.update(chunk)

    assert target_filename.exists(), f"File {target_filename} does not exist."
    logger.debug(f"Download successful.")

    if hasher:
        file_checksum = hasher.hexdigest()
        if file_checksum != checksum['value']:
            raise ValueError(
                f"Checksum mismatch for {target_filename}: expected {checksum['value']}, got {file_checksum}")
        logger.debug(f"Checksum verification successful.")

    return target_filename


def download_multiple_files(
        urls,
        target_filenames,
        max_workers=4,
        checksums: Optional[List[Dict]] = None) -> List[pathlib.Path]:
    """Download multiple files concurrently."""
    if max_workers == 1:
        return [download_file(url, target_filename, **checksum) for url, target_filename, checksum in
                zip(urls, target_filenames, checksums)]

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_file, url, target_filename, checksum["checksum"], checksum["checksum_algorithm"])
            for url, target_filename, checksum in
            zip(urls, target_filenames, checksums)]
        for future in futures:
            # Wait for each future to complete (can handle exceptions if needed)
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Failed to download a file: {e}")
    return results


def download_zenodo_metadata(record_id: int,
                             export_format: Union[str, ExportFormat],
                             convert_to_format: str = None) -> pathlib.Path:
    """Download metadata from a Zenodo record.

    Parameters
    ----------
    record_id : int
        The Zenodo record ID.
    export_format : str or ExportFormat
        The format to export the metadata to. Selections are defined by Zenodo.
    convert_to_format : str, optional
        The format to convert the metadata to. Default is None, which is the
        same as the export format. However, you may convert it to a different
        format. Use case: Download metadata in DCAT format and convert it to JSON-LD.
    """
    export_format = str(ExportFormat[str(export_format)].value)
    url = f"https://zenodo.org/records/{record_id}/export/{export_format}"
    try:
        # Send GET request to fetch metadata
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        cd = response.headers["content-disposition"]
        output_file = cd.split("filename=")[-1]
        if not output_file or pathlib.Path(output_file).suffix == "":
            output_file = f"{record_id}_metadata.{export_format}"

        # Write the metadata to a file
        with open(output_file, 'w', encoding='utf-8') as file:
            file.write(response.text)

        return pathlib.Path(output_file)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error fetching metadata: {e}")


def download_zenodo_dcat_metadata(record_id: int,
                                  format="json-ld",
                                  target_filename: Optional[pathlib.Path] = None):
    """Download metadata from a Zenodo record in DCAT format and convert it to the requested format.
    Default format is JSON-LD.

    Parameters
    ----------
    record_id : int
        The Zenodo record ID.
    format : str
        The format to convert the metadata to. Default is JSON-LD.
    target_filename : pathlib.Path, optional
        The target filename to save the converted metadata to. Default is None.
    """
    download_filename = download_zenodo_metadata(record_id, export_format="DCAT")

    g = rdflib.Graph()
    g.parse(download_filename)
    serialized_filename = download_filename.with_suffix(f".{format}")
    g.serialize(destination=serialized_filename,
                format=format,
                context={
                    "dcat": "http://www.w3.org/ns/dcat#",
                    "dct:": "http://purl.org/dc/terms/",
                    "doi:": "https://doi.org/",
                    "adms": "http://www.w3.org/ns/adms#",
                    "foaf": "http://xmlns.com/foaf/0.1/",
                    "owl": "http://www.w3.org/2002/07/owl#",
                    "prov": "http://www.w3.org/ns/prov#",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                    "skos": "http://www.w3.org/2004/02/skos/core#"
                },
                indent=2)
    if target_filename:
        return serialized_filename.rename(pathlib.Path(target_filename))
    return serialized_filename


def compute_md5(filename: Union[str, pathlib.Path]) -> str:
    """Compute the MD5 checksum of a file."""
    import hashlib
    filename = pathlib.Path(filename)
    if not filename.exists():
        raise FileNotFoundError(f"File {filename} does not exist.")
    hasher = hashlib.md5()
    with open(filename, 'rb') as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            hasher.update(byte_block)
    return hasher.hexdigest()
