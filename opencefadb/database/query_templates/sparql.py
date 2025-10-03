from gldb.query import SparqlQuery

SELECT_FAN_PROPERTIES = SparqlQuery(
    query="""PREFIX m4i: <http://w3id.org/nfdi4ing/metadata4ing#>

SELECT ?parameter ?property ?value
WHERE {
  <https://www.wikidata.org/wiki/Q131549102> m4i:hasParameter ?parameter .
  ?parameter a ?type .
  ?parameter ?property ?value .
}""",
    description="Selects all properties of the fan")

SELECT_FAN_CAD_FILE = SparqlQuery(
    query="""
PREFIX schema: <http://schema.org/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>

SELECT ?downloadURL
WHERE {
  <https://www.wikidata.org/wiki/Q131549102> dcterms:hasPart ?part .
  ?part dcat:distribution ?distribution .
  ?distribution dcat:downloadURL ?downloadURL .
}
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
