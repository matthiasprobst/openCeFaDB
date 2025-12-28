from typing import Union

import rdflib
from h5rdmtoolbox import catalog as h5cat
from pydantic import HttpUrl

SELECT_FAN_PROPERTIES = h5cat.SparqlQuery(
    query="""PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>

SELECT ?parameter ?property ?value
WHERE {
  <https://www.wikidata.org/wiki/Q131549102> m4i:hasParameter ?parameter .
  ?parameter a ?type .
  ?parameter ?property ?value .
}

ORDER BY ?parameter ?property
""",
    description="Selects all properties of the fan")


def get_fan_property(standard_name_uri: Union[str, rdflib.URIRef]):
    """Returns a SPARQL query that selects all properties of the fan parameter with the given standard name URI."""
    if isinstance(standard_name_uri, rdflib.URIRef):
        standard_name_uri = str(standard_name_uri)
    query_str = f"""PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
PREFIX ssno: <https://matthiasprobst.github.io/ssno#>

SELECT ?property ?value
WHERE {{
  <https://www.wikidata.org/wiki/Q131549102> m4i:hasParameter ?parameter .
  ?parameter ssno:hasStandardName <{standard_name_uri}> .
  ?parameter ?property ?value .
}}
ORDER BY ?property
"""
    return h5cat.SparqlQuery(
        query=query_str,
        description=f"Selects all properties of the fan parameter with standard name {standard_name_uri}"
    )


SELECT_FAN_CAD_FILE = h5cat.SparqlQuery(
    query="""
PREFIX schema: <http://schema.org/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>

SELECT DISTINCT ?downloadURL
WHERE {
  <https://www.wikidata.org/wiki/Q131549102> dcterms:hasPart ?part .
  ?part (dcat:distribution|schema:distribution|dcterms:hasPart) ?distribution .
  ?distribution (dcat:downloadURL|schema:downloadUrl|dcterms:identifier) ?downloadURL .
  FILTER(BOUND(?downloadURL))
}

LIMIT 10
""",
    description="Selects the CAD file for the fan")

SELECT_ALL = h5cat.SparqlQuery("SELECT * WHERE {?s ?p ?o}", description="Selects all triples in the RDF database")


def get_properties(
        subject_uri: Union[str, HttpUrl],
        cls_uri: Union[str, HttpUrl] = None,
        limit: int = None
):
    """Returns all properties of the given subject URI."""
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


def construct_data_based_on_standard_name_based_search_and_range_condition(
        target_standard_name_uris: list[str],
        conditional_standard_name_uri: str,
        condition_range: tuple[float, float]
) -> h5cat.SparqlQuery:
    """
    Finds data for multiple target standard name URIs per HDF file where the file contains a
    dataset with standard name `conditional_standard_name_uri` whose value is within the given range.

    :param target_standard_name_uris: list of standard name URIs to query for
    :param conditional_standard_name_uri: the standard name URI that must be present in the same HDF file
    :param condition_range: tuple with (min, max) values for the condition
    :return:
    """
    values_block = " ".join(f"<{uri}>" for uri in target_standard_name_uris)
    query_str = f"""
PREFIX hdf: <http://purl.allotrope.org/ontologies/hdf5/1.8#>
PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?hdfFile ?dataset ?value ?units ?standardName ?datasetName ?hasSymbol ?altLabel ?label
WHERE {{
  VALUES ?standardName {{ {values_block} }}
  ?dataset a hdf:Dataset ;
           ssno:hasStandardName ?standardName .

  OPTIONAL {{ ?dataset hdf:value ?value }}
  OPTIONAL {{ ?dataset hdf:name ?datasetName }}
  OPTIONAL {{ ?dataset m4i:hasSymbol ?hasSymbol }}

  OPTIONAL {{ ?dataset ssno:unit ?unit1 }}
  OPTIONAL {{ ?dataset m4i:hasUnit ?unit2 }}
  OPTIONAL {{ ?dataset rdfs:label ?label }}
  OPTIONAL {{ ?dataset skos:altLabel ?altLabel }}
  BIND(COALESCE(?unit1, ?unit2) AS ?units)

  ?hdfFile (hdf:rootGroup/hdf:member*)* ?dataset .
  ?hdfFile a hdf:File .

  # Optional: finde im selben File ein Dataset mit Standardname 'conditional_standard_name_uri' und seinen Wert
  OPTIONAL {{
    ?hdfFile (hdf:rootGroup/hdf:member*)* ?rotDataset .
    ?rotDataset ssno:hasStandardName <{conditional_standard_name_uri}> .
    OPTIONAL {{ ?rotDataset hdf:value ?conditionValue }}
  }}

  # Bedingung: ?conditionValue muss vorhanden sein und im Bereich liegen
  FILTER(
    BOUND(?conditionValue) && xsd:double(?conditionValue) >= {condition_range[0]} && xsd:double(?conditionValue) <= {condition_range[1]}
  )
}}
ORDER BY ?hdfFile ?dataset
"""
    description = (f"Selects datasets with standard names {target_standard_name_uris} within range "
                   f"{condition_range} of datasets with standard name {conditional_standard_name_uri}")
    return h5cat.SparqlQuery(
        query=query_str,
        description=description
    )


def construct_wikidata_property_search(wikidata_entity: str) -> h5cat.RemoteSparqlQuery:
    """e.g. wd:Q131549102"""
    if str(wikidata_entity).startswith("http"):
        wikidata_entity = f"<{wikidata_entity}>"
    else:
        wikidata_entity = f"wd:{wikidata_entity}"
    query = f"""
SELECT * WHERE {{
   {wikidata_entity} ?property ?value .

  OPTIONAL {{
    ?value rdfs:label ?valueLabel .
    FILTER(LANG(?valueLabel) IN ("de", "en"))
  }}

  # literal values: keep only english
  FILTER(
    !isLiteral(?value) ||
    (isLiteral(?value) && LANG(?value) IN ("en"))
  )

  # remove rows where valueLabel is None
  FILTER(BOUND(?valueLabel))
}}
ORDER BY ?property
"""
    return h5cat.RemoteSparqlQuery(query=query, description=f"Searches all properties of Wikidata entity {wikidata_entity}")
