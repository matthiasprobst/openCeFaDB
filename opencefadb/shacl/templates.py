

hdf_file_must_have_creator = '''@prefix sh: <http://www.w3.org/ns/shacl#> .
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