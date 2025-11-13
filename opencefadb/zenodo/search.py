import pathlib

import requests
from ontolutils.ex import prov
from ontolutils.ex.dcat import Dataset, Distribution
from ontolutils.ex.foaf import Agent
from h5rdmtoolbox.repository.zenodo import ZenodoRecord
ZENODO_API = "https://zenodo.org/api/records"

_EXT_MAP = {
    "csv": "text/csv",
    "tsv": "text/tab-separated-values",
    "json": "application/json",
    "jsonld": "application/ld+json",
    "ttl": "text/turtle",
    "hdf5": "application/x-hdf5",
    "h5": "application/x-hdf5",
    "nc": "application/x-netcdf",
    "zip": "application/zip",
    "iges": "model/iges",
    "igs": "model/iges",
    "md": "text/markdown",
    "txt": "text/plain",
    "xml": "application/xml",
    "rdf": "application/rdf+xml",
}


def _spdx_checksum_from_zenodo(checksum_str: str):
    """
    Convert Zenodo's 'md5:abcdef...' string to an SPDX checksum object.
    """
    if not checksum_str:
        return None
    if ":" in checksum_str:
        algo, value = checksum_str.split(":", 1)
    else:
        algo, value = "md5", checksum_str  # fallback

    algo_map = {
        "md5": "https://spdx.org/rdf/terms#checksumAlgorithm_md5",
        "sha1": "https://spdx.org/rdf/terms#checksumAlgorithm_sha1",
        "sha256": "https://spdx.org/rdf/terms#checksumAlgorithm_sha256",
        "sha512": "https://spdx.org/rdf/terms#checksumAlgorithm_sha512",
    }
    algo_iri = algo_map.get(algo.lower())
    return {
        "algorithm": algo_iri if algo_iri else algo,
        "checksumValue": value,
    }


def _zenodo_files(record):
    """
    Return a list of file dicts from a Zenodo record, handling common layouts.
    """
    files = record.get("files")
    if isinstance(files, list):  # modern layout
        return files
    # Older layouts sometimes keep files in metadata (rare for /records)
    md_files = record.get("metadata", {}).get("files", {})
    if isinstance(md_files, dict) and "files" in md_files:
        return md_files["files"]
    return []


def zenodo_record_to_dcat(record: dict) -> dict:
    """
    Translate a Zenodo record (as returned by /api/records) into a DCAT Dataset JSON-LD object.
    Includes dcat:Distribution entries with SPDX checksums for each file.
    """
    md = record.get("metadata", {})
    license_ = md.get("license").get("id") or None
    creators = md.get("creators") or []
    keywords = md.get("keywords") or []
    doi = md.get("doi")
    landing = record.get("links", {}).get("self")

    dcat_creators = []
    for c in creators:
        name = c.get("name") or c.get("affiliation")
        orcid = c.get("orcid") or ""
        person = {
            "id": f"https://orcid.org/{orcid.replace('https://orcid.org/', '')}",
            "name": name,
            "orcid": orcid,
            "affiliation": {
                "name": c.get("affiliation")
            }
        }
        dcat_creators.append(prov.Person.model_validate(person))

    def _parse_media_type(filename_suffix: str) -> str:
        ext = filename_suffix.rsplit('.', 1)[-1].lower()
        return _EXT_MAP.get(ext, "application/octet-stream")

    # Build distributions from files
    distributions = []
    for f in _zenodo_files(record):
        key = f.get("key") or f.get("filename") or "file"
        doi = doi or str(record.get("doi"))
        title = key
        media_type = _parse_media_type(pathlib.Path(f.get("key")).suffix)
        size = f.get("size")
        links = f.get("links", {})
        access_url = links.get("self") or record.get("links", {}).get("files")
        download_url = links.get("self")
        checksum_obj = _spdx_checksum_from_zenodo(f.get("checksum"))

        dist = {
            "id": links["self"],
            "title": title,
            "accessURL": access_url,
            "downloadURL": download_url,
            "mediaType": media_type,
            "byteSize": size,
            "checksum": checksum_obj
        }
        dcat_dist = Distribution.model_validate(dist)
        distributions.append(dcat_dist)
    publisher = Agent(
        id="https://zenodo.org/",
        name="Zenodo",
    )
    # Build dataset
    dataset = {
        "id": record["links"]["doi"],
        "title": md.get("title"),
        "description": md.get("description"),
        "identifier": doi or str(record.get("id")),
        "language": md.get("language"),
        "issued": md.get("publication_date"),
        "modified": record.get("updated") or record.get("revision"),
        "publisher": publisher,
        "creator": dcat_creators,
        "keyword": keywords,
        "landingPage": landing,
        "license": license_,
        "accessRights": md.get("access_right"),
        "version": md.get("version"),
        "distribution": distributions,
    }
    dcat_dataset = Dataset.model_validate(
        dataset
    )
    return dcat_dataset


def get_latest_opencefadb_datasets(max_pages=10):
    """
    Fetch all *latest* Zenodo records tagged 'opencefadb' that are of type 'Dataset'.
    """
    params = {
        "q": "keywords:opencefadb AND resource_type.type:dataset",
        "size": 100,
        "page": 1,
        "all_versions": "false",
        "sort": "mostrecent",
    }

    all_records = []
    for page in range(1, max_pages + 1):
        params["page"] = page
        resp = requests.get(ZENODO_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        all_records.extend(hits)
    return all_records


if __name__ == "__main__":
    # 1) Fetch latest opencefadb datasets
    records = get_latest_opencefadb_datasets(max_pages=20)

    # 2) Convert each to DCAT
    dcat_datasets = [zenodo_record_to_dcat(r) for r in records]

    # 3) Print a tiny summary and checksums
    print(f"Converted {len(dcat_datasets)} datasets to DCAT.\n")
    for ds in dcat_datasets:
        print("Dataset:")
        print(ds.serialize("ttl"))

        # for dist in ds.distribution:
        #     dist.download(dest_filename=dist.title)
