import pathlib
from typing import Union

import rdflib
import requests

from .url import _parse_url


def delete_repository(*,
                      repository_id: str,
                      host="http://localhost",
                      port=7200,
                      auth=(None, None)):
    """Delete a repository from a GraphDB instance.

    Equivalent to "curl -X DELETE localhost:7200/rest/repositories/<repository_id>"
    """
    response = requests.delete(f"{_parse_url(host)}:{port}/rest/repositories/{repository_id}", auth=auth)
    response.raise_for_status()
    return response


def create_repository(
        config_filename: Union[str, pathlib.Path],
        host="localhost",
        port=7200,
        exist_ok=True) -> str:
    """Create a repository in a GraphDB instance.

    Equivalent to `curl -X POST localhost:7201/rest/repositories -H "Content-Type: multipart/form-data" -F "config=@test-repo-config.ttl"`
    """
    config_filename = pathlib.Path(config_filename)
    assert config_filename.exists(), f"Configuration file '{config_filename}' does not exist"
    repoID = None
    g = rdflib.Graph()
    g.parse(config_filename, format="ttl")
    for res in g.query("""
    PREFIX rep: <http://www.openrdf.org/config/repository#>

    SELECT ?repoID
    WHERE {
        ?repo rep:repositoryID ?repoID .
    }
    """):
        repoID = str(res[0])

    if not config_filename.exists():
        raise FileNotFoundError(f"Configuration file '{config_filename}' does not exist")
    if not config_filename.suffix == '.ttl':
        raise ValueError("Configuration file must be a Turtle file (.ttl)")

    uri = f"{_parse_url(host)}:{port}"
    existing_repos = requests.get(f"{uri}/rest/repositories").json()
    if repoID in [e["id"] for e in existing_repos] and exist_ok:
        return repoID

    with open(config_filename, 'rb') as config_file:
        files = {'config': config_file}
        response = requests.post(f"{uri}/rest/repositories", files=files)
        response.raise_for_status()
    return repoID
