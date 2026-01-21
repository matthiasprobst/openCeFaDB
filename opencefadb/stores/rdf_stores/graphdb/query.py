from rdflib import Graph
from rdflib.plugins.stores.sparqlstore import SPARQLStore


def select_query(repo, sparql_query: str):
    store = SPARQLStore(auth=repo["auth"])
    store.open(repo["uri"])
    g = Graph(store=store, identifier=None, bind_namespaces="none")
    return g.query(sparql_query)


def ask_query():
    pass
