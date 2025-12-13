from gldb.query import SparqlQuery, RemoteSparqlQuery

SELECT_FAN_PROPERTIES = SparqlQuery(
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

SELECT_FAN_CAD_FILE = SparqlQuery(
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

SELECT_ALL = SparqlQuery("SELECT * WHERE {?s ?p ?o}", description="Selects all triples in the RDF database")

SELECT_ALL_OPERATION_POINTS = SparqlQuery(
    query=""""
PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
PREFIX qudt: <http://qudt.org/vocab/unit#>
    
""",
    description="Selects all operation points by searching for HDF5 files that contain standard names for operation points"
)


# def get_data_based_on_standard_name_based_search_and_range_condition(
#         target_standard_name_uri: str,
#         conditional_standard_name_uri: str,
#         condition_range: tuple[float, float],
# ) -> SparqlQuery:
#     """
#     Finds data based on a target standard name URI per HDF file a dataset with standard name
#     `conditional_standard_name_uri` whose value is within the given n_rot_range.
#     https://doi.org/10.5281/zenodo.17572275#standard_name_table/derived_standard_name/arithmetic_mean_of_fan_rotational_speed
#
#     :param target_standard_name_uri: the standard name URI to query for
#     :param conditional_standard_name_uri: the standard name URI that must be present in the same HDF file
#     :param condition_range: tuple with (min, max) values for the condition
#     :return:
#     """
#     query_str = f"""
# PREFIX hdf: <http://purl.allotrope.org/ontologies/hdf5/1.8#>
# PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
# PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
#
# SELECT ?hdfFile ?dataset ?value ?units ?standardName
# WHERE {{
#   BIND(<{target_standard_name_uri}> AS ?standardName)
#   ?dataset a hdf:Dataset ;
#            ssno:hasStandardName ?standardName .
#
#   OPTIONAL {{ ?dataset hdf:value ?value }}
#
#   OPTIONAL {{ ?dataset ssno:unit ?unit1 }}
#   OPTIONAL {{ ?dataset m4i:hasUnit ?unit2 }}
#   BIND(COALESCE(?unit1, ?unit2) AS ?units)
#
#   ?hdfFile (hdf:rootGroup/hdf:member*)* ?dataset .
#   ?hdfFile a hdf:File .
#
#   # Optional: finde im selben File ein Dataset mit Standardname 'conditional_standard_name_uri' und seinen Wert
#   OPTIONAL {{
#     ?hdfFile (hdf:rootGroup/hdf:member*)* ?rotDataset .
#     ?rotDataset ssno:hasStandardName <{conditional_standard_name_uri}> .
#     OPTIONAL {{ ?rotDataset hdf:value ?conditionValue }}
#   }}
#
#   # Wenn der angefragte Standardname auf 'mean_rot_speed' endet, muss ?conditionValue im Bereich liegen
#   FILTER(
#     (BOUND(?conditionValue) && xsd:double(?conditionValue) >= {condition_range[0]} && xsd:double(?conditionValue) <= {condition_range[1]})
#   )
# }}
# ORDER BY ?hdfFile ?dataset
# """
#     return SparqlQuery(
#         query=query_str,
#         description=f"Selects datasets with standard name {target_standard_name_uri} within range "
#                     f"{condition_range} of datasets with standard name {conditional_standard_name_uri}"
#     )

def construct_data_based_on_standard_name_based_search_and_range_condition(
        target_standard_name_uris: list[str],
        conditional_standard_name_uri: str,
        condition_range: tuple[float, float]
) -> SparqlQuery:
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

SELECT ?hdfFile ?dataset ?value ?units ?standardName
WHERE {{
  VALUES ?standardName {{ {values_block} }}
  ?dataset a hdf:Dataset ;
           ssno:hasStandardName ?standardName .

  OPTIONAL {{ ?dataset hdf:value ?value }}

  OPTIONAL {{ ?dataset ssno:unit ?unit1 }}
  OPTIONAL {{ ?dataset m4i:hasUnit ?unit2 }}
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
    return SparqlQuery(
        query=query_str,
        description=description
    )


def construct_wikidata_property_search(wikidata_entity: str) -> RemoteSparqlQuery:
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
    return RemoteSparqlQuery(query=query, description=f"Searches all properties of Wikidata entity {wikidata_entity}")

# def construct_operation_point_query(
#         revolution_speed_Hz: float,
# ):
#     return SparqlQuery(f"""
#     PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>
#     PREFIX qudt: <http://qudt.org/vocab/unit#>
#     PREFIX qudt-unit: <http://qudt.org/vocab/unit/>
#     PREFIX qudt-kind: <http://qudt.org/vocab/quantitykind/>
#     PREFIX qudt-unit-1-8: <http://qudt.org/vocab/unit/1-8#>
#     PREFIX qudt-kind-1-8: <http://qudt.org/vocab/quantitykind/1-8#>
#     PREFIX ex: <https://example.org/>
#
#     SELECT ?operationPoint ?speed ?torque ?power ?efficiency ?vibration
#     WHERE {{
#       ?operationPoint a m4i:OperationPoint .
#       ?operationPoint m4i:hasSpeed ?speed .
#       ?operationPoint m4i:hasTorque ?torque .
#       ?operationPoint m4i:hasPower ?power .
#       ?operationPoint m4i:hasEfficiency ?efficiency .
#       ?operationPoint m4i:hasVibration ?vibration .
#       ?speed qudt:unit qudt-unit:HZ .
#       ?speed qudt:kind qudt-kind:Frequency .
#       ?speed qudt:numericValue {revolution_speed_Hz} .
#     }}
#     """)
