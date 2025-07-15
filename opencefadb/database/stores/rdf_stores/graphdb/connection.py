import logging

import requests

from .repository import GraphDBRepository

logger = logging.getLogger("pygraphdb")

CONTENT_TYPE = {
    ".jsonld": "application/ld+json",
    ".ttl": "text/turtle",
}

FALLBACK_CONFIG = "DEFAULT"


def _get_request(*args, **kwargs):
    try:
        return requests.get(*args, **kwargs)
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {e}")
        raise e


def _post_request(*args, **kwargs):
    try:
        return requests.post(*args, **kwargs)
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error: {e}")
        raise e


class GraphDB:

    def __init__(self, url: str, auth=(None, None)):
        self._auth = auth
        self._url = url
        self._repositories = None

    def __getitem__(self, item) -> GraphDBRepository:
        selected_repo = self.repositories.get(item, None)
        if selected_repo is None:
            raise KeyError(f"Repository '{item}' not found in GraphDB at {self._url}.")
        selected_repo.update({"auth": self._auth})
        repo = GraphDBRepository(selected_repo)
        return repo

    @property
    def repositories(self):
        if self._repositories is not None:
            return self._repositories
        response = _get_request(f"{self._url}/rest/repositories", auth=(None, None))
        response.raise_for_status()
        self._repositories = {repo["id"]: repo for repo in response.json()}
        return self._repositories