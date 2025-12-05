NUMERIC_DATASETS_SHALL_HAVE_UNIT = '''@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix hdf:  <http://purl.allotrope.org/ontologies/hdf5/1.8#> .
@prefix m4i:  <http://w3id.org/nfdi4ing/metadata4ing#> .
@prefix ex:   <http://example.org/ns#> .

ex:NumericDatasetsMustHaveUnit
  a sh:NodeShape ;
  sh:targetClass hdf:Dataset ;
  sh:sparql [
    a sh:SPARQLConstraint ;
    sh:message "Numeric hdf:Dataset (H5T_INTEGER or H5T_FLOAT) must have at least one m4i:hasUnit and the unit must be an IRI." ;
    sh:select """
      SELECT ?this WHERE {
        ?this a hdf:Dataset .

        # Only consider numeric datasets
        FILTER EXISTS {
          ?this hdf:datatype ?dt .
          FILTER (?dt IN (hdf:H5T_INTEGER, hdf:H5T_FLOAT))
        }

        # Violation if:
        #   - no m4i:hasUnit at all, OR
        #   - there is a m4i:hasUnit but at least one value is not an IRI
        FILTER (
          !EXISTS { ?this m4i:hasUnit ?unitValue . } ||
          EXISTS {
            ?this m4i:hasUnit ?unitValue .
            FILTER ( !isIRI(?unitValue) )
          }
        )
      }
    """ ;
  ] .

'''

SHALL_HAVE_CREATOR = '''@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix hdf: <http://purl.allotrope.org/ontologies/hdf5/1.8#> .
@prefix ex: <http://example.org/ns#> .

ex:HDFFileCreatedShape
    a sh:NodeShape ;
    sh:targetClass hdf:File ;                # apply only to hdf:File instances
    sh:property [
        sh:path dcterms:created ;            # must have this property
        sh:or (                             # accept either xsd:date or xsd:dateTime
            [ sh:datatype xsd:date ]
            [ sh:datatype xsd:dateTime ]
        ) ;
        sh:minCount 1 ;                      # at least one occurrence
        sh:maxCount 1 ;                      # optional but recommended
        sh:message "Each hdf:File must have exactly one dcterms:created value of type xsd:date." ;
    ] .
'''