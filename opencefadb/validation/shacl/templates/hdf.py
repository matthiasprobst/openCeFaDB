NUMERIC_DATASETS_SHALL_HAVE_UNIT_AND_KIND_OF_QUANTITY = '''@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix hdf:  <http://purl.allotrope.org/ontologies/hdf5/1.8#> .
@prefix m4i:  <http://w3id.org/nfdi4ing/metadata4ing#> .
@prefix ex:   <http://example.org/ns#> .

ex:NumericDatasetsMustHaveUnitAndKindOfQuantity
  a sh:NodeShape ;
  sh:targetClass hdf:Dataset ;
  sh:sparql [
    a sh:SPARQLConstraint ;
    sh:message "Numeric hdf:Dataset (H5T_INTEGER or H5T_FLOAT) must have at least one m4i:hasUnit and at least one m4i:hasKindOfQuantity; all values for both properties must be IRIs." ;
    sh:select """
      SELECT ?this WHERE {
        ?this a hdf:Dataset .

        # Only consider numeric datasets
        FILTER EXISTS {
          ?this hdf:datatype ?dt .
          FILTER (?dt IN (hdf:H5T_INTEGER, hdf:H5T_FLOAT))
        }

        # Violation if either requirement fails:
        #  - missing property entirely, OR
        #  - any value is not an IRI
        FILTER (
          # hasUnit missing OR has a non-IRI value
          !EXISTS { ?this m4i:hasUnit ?u . } ||
          EXISTS {
            ?this m4i:hasUnit ?u .
            FILTER ( !isIRI(?u) )
          } ||

          # hasKindOfQuantity missing OR has a non-IRI value
          !EXISTS { ?this m4i:hasKindOfQuantity ?kq . } ||
          EXISTS {
            ?this m4i:hasKindOfQuantity ?kq .
            FILTER ( !isIRI(?kq) )
          }
        )
      }
    """ ;
  ] .

'''

SHALL_HAVE_VALID_ATTRIBUTION = '''@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix schema: <https://schema.org/> .
@prefix ex:   <http://example.org/ns#> .

ex:WasAttributedToMustReferenceAgent
    a sh:NodeShape ;
    sh:targetSubjectsOf prov:wasAttributedTo ;

    sh:property [
        sh:path prov:wasAttributedTo ;
        sh:message "prov:wasAttributedTo must point to a prov:Agent, prov:Person, foaf:Organization, prov:Organization, schema:Organization or schema:Person." ;

        # Allow any of the valid agent classes:
        sh:or (
            [ sh:class prov:Agent ]
            [ sh:class prov:Person ]
            [ sh:class foaf:Organization ]
            [ sh:class schema:Person ]
            [ sh:class schema:Organization ]
            [ sh:class prov:Organization ]
        ) ;
    ] .
'''

SHALL_HAVE_CREATED_DATE = '''@prefix sh: <http://www.w3.org/ns/shacl#> .
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

SHALL_HAVE_CREATOR = '''@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix hdf: <http://purl.allotrope.org/ontologies/hdf5/1.8#> .
@prefix ex: <http://example.org/ns#> .

ex:HDFFileCreatorShape
    a sh:NodeShape ;
    sh:targetClass hdf:File ;
    sh:property [
        sh:path dcterms:creator ;
        sh:minCount 1 ;
        sh:message "Each hdf:File must have at least one dcterms:creator which is either an IRI or a prov:Person." ;
        sh:sparql [
            a sh:SPARQLConstraint ;
            sh:message "Each dcterms:creator must be either an IRI or a prov:Person." ;
            sh:select """
                SELECT ?this WHERE {
                  ?this dcterms:creator ?creator .

                  FILTER (
                    !isIRI(?creator) &&
                    NOT EXISTS { ?creator a prov:Person . }
                  )
                }
            """ ;
        ] ;
    ] .
    
'''

HDF_FILE_SHALL_HAVE_STANDARD_NAME_TABLE = '''@prefix sh:    <http://www.w3.org/ns/shacl#> .
@prefix ssno:  <https://matthiasprobst.github.io/ssno#> .
@prefix hdf:  <http://purl.allotrope.org/ontologies/hdf5/1.8#> .

ssno:HdfFileUsesStandardNameTableShape
    a sh:NodeShape ;
    sh:targetClass hdf:File ;
    sh:property [
        sh:path ssno:usesStandardNameTable ;
        sh:class ssno:StandardNameTable ;                 # object must be a StandardNameTable
        sh:minCount 1 ;
        sh:maxCount 1 ;                                # exactly one
        sh:message "A hdf:File file must use exactly one ssno:StandardNameTable via ssno:usesStandardNameTable."@en ;
    ] .
'''
