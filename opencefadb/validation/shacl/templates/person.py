# write shacl template for a person with given name, family name, and email address using prov or foaf, in any case, orcid and name must be present


PERSON_SHACL = """@prefix sh:    <http://www.w3.org/ns/shacl#> .
@prefix prov:  <http://www.w3.org/ns/prov#> .
@prefix foaf:  <http://xmlns.com/foaf/0.1/> .
@prefix m4i:   <http://w3id.org/nfdi4ing/metadata4ing#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:    <http://example.org/ns#> .

ex:PersonShape
    a sh:NodeShape ;
    sh:targetClass prov:Person ;

    # foaf:firstName muss vorhanden sein (mindestens 1 Wert)
    sh:property [
        sh:path foaf:firstName ;
        sh:minCount 1 ;
    ] ;

    # foaf:lastName muss vorhanden sein (mindestens 1 Wert)
    sh:property [
        sh:path foaf:lastName ;
        sh:minCount 1 ;
    ] ;

    # m4i:orcidId muss vorhanden sein und ein IRI sein
    sh:property [
        sh:path m4i:orcidId ;
        sh:minCount 1 ;
        sh:nodeKind sh:IRI ;
    ] .
"""
