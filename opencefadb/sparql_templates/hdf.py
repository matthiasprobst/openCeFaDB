from h5rdmtoolbox import catalog as h5cat


def find_dataset_for_standard_name(
        standard_name: str,
        value_range: tuple[float, ...] = None,
) -> h5cat.SparqlQuery:
    query_str = f"""
PREFIX hdf:  <http://purl.allotrope.org/ontologies/hdf5/1.8#>
PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

SELECT ?dataset ?value
WHERE {{
  BIND(<{standard_name}> AS ?standardName)

  ?dataset a hdf:Dataset ;
           ssno:hasStandardName ?standardName ;
           hdf:value ?value .
"""
    if value_range is None:
        query_str += "}\n"
    else:
        query_str += f"""
  FILTER(
    xsd:double(?value) >= {value_range[0]} &&
    xsd:double(?value) <= {value_range[1]}
  )
}}
"""
    return h5cat.SparqlQuery(
        query=query_str,
        description=f"Find datasets with {standard_name}. Returns the dataset URI and its value." if value_range is None else f"Find datasets with {standard_name} where value in {value_range}."
    )


def find_datasets_by_standard_name_and_value_range(
        standard_name: str,
        value_range: tuple[float, float] = None,
) -> h5cat.SparqlQuery:
    query_str = f"""
PREFIX hdf:  <http://purl.allotrope.org/ontologies/hdf5/1.8#>
PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
PREFIX idx:  <urn:h5cat:index#>

SELECT ?hdfFile ?dataset ?value
WHERE {{
  ?dataset ssno:hasStandardName <{standard_name}> ;
           hdf:value ?value ;
           idx:inFile ?hdfFile .
"""
    if value_range is not None:
        lo, hi = value_range
        query_str += f"""
  FILTER(?value >= {lo} && ?value <= {hi})
"""
    query_str += "}\n"

    return h5cat.SparqlQuery(
        query=query_str,
        description=(
            f"Find datasets with {standard_name}."
            if value_range is None
            else f"Find datasets with {standard_name} where value in {value_range}."
        ),
    )


def find_hdf5_file_for_dataset(dataset_uri: str) -> h5cat.SparqlQuery:
    query_str = f"""
PREFIX idx: <urn:h5cat:index#>

SELECT ?hdfFile
WHERE {{
  <{dataset_uri}> idx:inFile ?hdfFile .
}}
"""
    return h5cat.SparqlQuery(
        query=query_str,
        description=f"Find HDF5 file containing dataset {dataset_uri}"
    )


def find_dataset_in_file_by_standard_name(
        hdf_file_uri: str,
        standard_name_uri: str,
) -> h5cat.SparqlQuery:
    query_str = f"""
PREFIX idx:  <urn:h5cat:index#>
PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
PREFIX hdf:  <http://purl.allotrope.org/ontologies/hdf5/1.8#>
PREFIX m4i:  <http://w3id.org/nfdi4ing/metadata4ing#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?dataset ?value ?units ?label ?altLabel ?hasSymbol ?hasMinimumValue ?hasMaximumValue
WHERE {{
  ?dataset idx:inFile <{hdf_file_uri}> ;
           hdf:value ?value ;
           ssno:hasStandardName <{standard_name_uri}> .
           
  OPTIONAL {{ ?dataset m4i:hasMinimumValue ?hasMinimumValue }}
  OPTIONAL {{ ?dataset m4i:hasMaximumValue ?hasMaximumValue }}
  OPTIONAL {{ ?dataset m4i:hasSymbol ?hasSymbol }}
  OPTIONAL {{ ?dataset m4i:hasUnit ?units }}
  OPTIONAL {{ ?dataset rdfs:label ?label }}
  OPTIONAL {{ ?dataset skos:altLabel ?altLabel }}
}}
"""
    return h5cat.SparqlQuery(
        query=query_str,
        description=f"Find datasets in {hdf_file_uri} with standard name {standard_name_uri}"
    )
