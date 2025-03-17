import logging
import pathlib

import rdflib
from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from .request_utils import _post_request

logger = logging.getLogger("pygraphdb")

CONTENT_TYPE = {
    ".jsonld": "application/ld+json",
    ".ttl": "text/turtle",
}


class GraphDBRepository:

    def __init__(self, params):
        self._params = params
        store = SPARQLStore(auth=self._params["auth"])
        store.open(self._params["uri"])
        self.graph = Graph(store=store, identifier=None, bind_namespaces="none")

    def __getitem__(self, item):
        return self._params[item]

    def upload_file(
            self,
            filename: pathlib.Path,
            check_for_blank_nodes=True,
            use_base_iri="https://local.org/"
    ):
        logger.debug(f"Uploading file {filename} to {self['id']} ...")
        filename = pathlib.Path(filename).resolve().absolute()
        if not filename.exists():
            raise FileNotFoundError(f"File '{filename}' does not exist")

        if check_for_blank_nodes:
            has_blank_nodes, (s, p, o) = _check_for_blank_nodes(filename)
            if has_blank_nodes and use_base_iri:
                with open(str(filename), 'r', encoding='utf-8') as f:
                    content = f.read()
                content = content.replace("_:b", f"{use_base_iri}b")
                with open(str(filename), 'w', encoding='utf-8') as f:
                    f.write(content)
            elif has_blank_nodes and not use_base_iri:
                raise ValueError(f"File '{filename}' contains blank nodes. Blank nodes are not supported: "
                                 f"{s}, {p}, {o}")

        headers = {"Content-Type": CONTENT_TYPE[filename.suffix]}

        repo_id = self['id']

        with open(filename, 'rb') as f:
            response = _post_request(self["uri"] + "/statements", headers=headers, data=f, auth=self["auth"])
        if response.status_code == 204:
            logger.info(f"File '{filename}' successfully uploaded to GraphDB repository '{repo_id}'")
        else:
            logger.error(f"Could not upload file {filename}: {response.status_code} - {response.text}")
        logger.debug(f"Done uploading file {filename} to {repo_id} ...")
        return response


def _check_for_blank_nodes(filename):
    logger.debug(f"Checking file {filename} for blank nodes ...")
    g = rdflib.Graph()
    g.parse(source=filename)
    for s, p, o in g.triples((None, None, None)):
        if isinstance(s, rdflib.BNode) or isinstance(p, rdflib.BNode) or isinstance(o, rdflib.BNode):
            return True, (s, p, o)
    return False, (None, None, None)
