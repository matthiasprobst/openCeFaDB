from typing import Union

import rdflib
from h5rdmtoolbox import catalog as h5cat
from pydantic import HttpUrl

from . import hdf, fan

__all__ = [
    "hdf",
    "fan",
]

SELECT_ALL = h5cat.SparqlQuery("SELECT * WHERE {?s ?p ?o}", description="Selects all triples in the RDF database")


def get_properties(
        subject_uri: Union[str, HttpUrl, rdflib.URIRef],
        cls_uri: Union[str, HttpUrl, rdflib.URIRef] = None,
        limit: int = None
):
    """Returns a SPARQL query that selects all properties of the given subject URI. If cls_uri is provided,
    only properties for which the subject is of the given class are returned."""
    subject_uri = str(HttpUrl(subject_uri))
    if cls_uri is None:
        query_str = f"""
    PREFIX ssno: <https://matthiasprobst.github.io/ssno#>

    SELECT ?property ?value
    WHERE {{
        <{subject_uri}> ?property ?value .
    }}
    ORDER BY ?property
    """
    else:
        query_str = f"""
    SELECT ?property ?value
    WHERE {{
        <{subject_uri}> a {cls_uri} .
        <{subject_uri}> ?property ?value .
    }}
    ORDER BY ?property
    """
    if limit is not None:
        query_str += f"LIMIT {limit}\n"

    return h5cat.SparqlQuery(
        query=query_str,
        description=f"Selects all properties of the target URI {subject_uri}"
    )
