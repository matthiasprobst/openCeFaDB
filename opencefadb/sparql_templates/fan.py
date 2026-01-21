from typing import Union

import rdflib
from h5rdmtoolbox import catalog as h5cat

SELECT_FAN_PROPERTIES = h5cat.SparqlQuery(
    query="""PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>

SELECT ?parameter ?property ?value
WHERE {
  <https://www.wikidata.org/wiki/Q131549102> m4i:hasParameter ?parameter .
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


# def construct_data_based_on_standard_name_based_search(
#         target_standard_name_uris: list[str]
# ) -> h5cat.SparqlQuery:
#     """
#     Finds data for multiple target standard name URIs per HDF file where the file contains a
#     dataset with standard name `conditional_standard_name_uri` whose value is within the given range.
#
#     :param target_standard_name_uris: list of standard name URIs to query for
#     :return:
#     """
#     values_block = " ".join(f"<{uri}>" for uri in target_standard_name_uris)
#     query_str = f"""
# PREFIX hdf: <http://purl.allotrope.org/ontologies/hdf5/1.8#>
# PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
# PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
#
# SELECT ?hdfFile ?dataset ?value ?units ?standardName ?datasetName ?hasSymbol ?altLabel ?label
# WHERE {{
#   VALUES ?standardName {{ {values_block} }}
#   ?dataset a hdf:Dataset ;
#            ssno:hasStandardName ?standardName .
#
#   OPTIONAL {{ ?dataset hdf:value ?value }}
#   OPTIONAL {{ ?dataset hdf:name ?datasetName }}
#   OPTIONAL {{ ?dataset m4i:hasSymbol ?hasSymbol }}
#
#   OPTIONAL {{ ?dataset ssno:unit ?unit1 }}
#   OPTIONAL {{ ?dataset m4i:hasUnit ?unit2 }}
#   OPTIONAL {{ ?dataset rdfs:label ?label }}
#   OPTIONAL {{ ?dataset skos:altLabel ?altLabel }}
#   BIND(COALESCE(?unit1, ?unit2) AS ?units)
#
#   ?hdfFile (hdf:rootGroup/hdf:member*)* ?dataset .
#   ?hdfFile a hdf:File .
# }}
# ORDER BY ?hdfFile ?dataset
# """
#     description = f"Selects datasets with standard names {target_standard_name_uris}"
#     return h5cat.SparqlQuery(
#         query=query_str,
#         description=description
#     )


# def construct_data_based_on_standard_name_based_search_and_range_condition(
#         target_standard_name_uris: list[str],
#         conditional_standard_name_uri: str,
#         condition_range: tuple[float, float]
# ) -> h5cat.SparqlQuery:
#     values_block = " ".join(f"<{uri}>" for uri in target_standard_name_uris)
#
#     query_str = f"""
# PREFIX hdf: <http://purl.allotrope.org/ontologies/hdf5/1.8#>
# PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
# PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
# PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
# PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#
# SELECT ?hdfFile ?dataset ?value ?units ?standardName ?datasetName ?hasSymbol ?altLabel ?label
# WHERE {{
#   VALUES ?standardName {{ {values_block} }}
#
#   ?hdfFile a hdf:File ;
#            hdf:rootGroup ?root .
#
#   # Traverse file structure via groups (matches your model)
#   ?root hdf:member* ?group .
#   ?group a hdf:Group ;
#          hdf:member ?dataset .
#
#   ?dataset a hdf:Dataset ;
#            ssno:hasStandardName ?standardName .
#
#   ?dataset hdf:value ?value .
#   ?dataset hdf:name ?datasetName .
#   OPTIONAL {{ ?dataset m4i:hasSymbol ?hasSymbol }}
#   OPTIONAL {{ ?dataset ssno:unit ?unit1 }}
#   OPTIONAL {{ ?dataset m4i:hasUnit ?unit2 }}
#   OPTIONAL {{ ?dataset rdfs:label ?label }}
#   OPTIONAL {{ ?dataset skos:altLabel ?altLabel }}
#   BIND(COALESCE(?unit1, ?unit2) AS ?units)
#
#   # Condition: file must contain a dataset with the conditional standard name in the numeric range
#   FILTER EXISTS {{
#     ?root hdf:member* ?g2 .
#     ?g2 hdf:member ?rotDataset .
#     ?rotDataset a hdf:Dataset ;
#                 ssno:hasStandardName <{conditional_standard_name_uri}> ;
#                 hdf:value ?conditionValue .
#     FILTER(
#       xsd:double(?conditionValue) >= {condition_range[0]}
#       && xsd:double(?conditionValue) <= {condition_range[1]}
#     )
#   }}
# }}
# ORDER BY ?hdfFile ?dataset
# """
#     description = (f"Selects datasets with standard names {target_standard_name_uris} within range "
#                    f"{condition_range} of datasets with standard name {conditional_standard_name_uri}")
#     return h5cat.SparqlQuery(query=query_str, description=description)

#


# def find_dataset_value_in_same_group_by_other_standard_name(
#         dataset_a: str,
#         standard_name_b: str,
# ) -> h5cat.SparqlQuery:
#     query_str = f"""
# PREFIX hdf:  <http://purl.allotrope.org/ontologies/hdf5/1.8#>
# PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
# PREFIX m4i:  <http://w3id.org/nfdi4ing/metadata4ing#>
# PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
# PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#
# SELECT DISTINCT
#   ?hdfFile ?group ?dsA ?dsB ?value ?units ?label ?altLabel ?hasSymbol ?hasMinimumValue ?hasMaximumValue
# WHERE {{
#   BIND(<{dataset_a}> AS ?dsA)
#   BIND(<{standard_name_b}> AS ?stdB)
#
#   # 1) Find the group(s) that directly contain dataset A (no property paths)
#   ?group a hdf:Group ;
#          hdf:member ?dsA ;
#          hdf:member ?dsB .
#
#   # 2) Constrain dataset B early
#   ?dsB a hdf:Dataset ;
#        ssno:hasStandardName ?stdB ;
#        hdf:value ?value .
#
#   FILTER(?dsB != ?dsA)
#
#   # 3) Only now, resolve which file this group belongs to
#   ?hdfFile a hdf:File ;
#            hdf:rootGroup ?root .
#
#   {{
#     # group directly under root
#     ?root hdf:member ?group .
#   }}
#   UNION
#   {{
#     # group under subgroups
#     ?root hdf:member+ ?g1 .
#     ?g1  hdf:member ?group .
#   }}
#
#   OPTIONAL {{ ?dsB m4i:hasMinimumValue ?hasMinimumValue }}
#   OPTIONAL {{ ?dsB m4i:hasMaximumValue ?hasMaximumValue }}
#   OPTIONAL {{ ?dsB m4i:hasSymbol ?hasSymbol }}
#   OPTIONAL {{ ?dsB m4i:hasUnit ?units }}
#   OPTIONAL {{ ?dsB rdfs:label ?label }}
#   OPTIONAL {{ ?dsB skos:altLabel ?altLabel }}
# }}
# """
#     return h5cat.SparqlQuery(
#         query=query_str,
#         description=f"Find dataset with standard name {standard_name_b} in same group as {dataset_a}"
#     )


# def find_dataset_value_in_same_group_by_other_standard_name2(
#         dataset_a: str,
#         standard_name_b: str,
# ) -> h5cat.SparqlQuery:
#     query_str = f"""
# PREFIX hdf: <http://purl.allotrope.org/ontologies/hdf5/1.8#>
# PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
# PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
# PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
#
# SELECT ?hdfFile ?group ?dsA ?dsB ?value ?units ?label ?altLabel ?hasSymbol ?hasMinimumValue ?hasMaximumValue
# WHERE {{
#   VALUES ?dsA  {{ <{dataset_a}> }}
#   VALUES ?stdB {{ <{standard_name_b}> }}
#
#   # file + group hierarchy
#   ?hdfFile a hdf:File ;
#            hdf:rootGroup ?root .
#
#   # find the group (under this file) that directly contains dataset A
#   ?root hdf:member* ?group .
#   ?group a hdf:Group ;
#          hdf:member ?dsA .
#
#   # find dataset B in the same group with the requested standard name
#   ?group hdf:member ?dsB .
#   ?dsB a hdf:Dataset ;
#        ssno:hasStandardName ?stdB .
#   ?dsB hdf:value ?value
#
#   OPTIONAL {{ ?dsB m4i:hasMinimumValue ?hasMinimumValue }}
#   OPTIONAL {{ ?dsB m4i:hasMaximumValue ?hasMaximumValue }}
#   OPTIONAL {{ ?dsB m4i:hasSymbol ?hasSymbol }}
#   OPTIONAL {{ ?dsB m4i:hasUnit ?units }}
#   OPTIONAL {{ ?dsB rdfs:label ?label }}
#   OPTIONAL {{ ?dsB skos:altLabel ?altLabel }}
#
#   FILTER(?dsB != ?dsA)
# }}
# """
#     return h5cat.SparqlQuery(
#         query=query_str,
#         description=f"Find dataset with standard name {standard_name_b} in same group as {dataset_a}"
#     )
#
#

