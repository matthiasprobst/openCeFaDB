NUMERICAL_VARIABLE_HAS_LABEL = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix m4i: <http://w3id.org/nfdi4ing/metadata4ing#> .
@prefix ex: <http://example.org/ns#> .  

# Every m4i:NumericalVariable must have an rdfs:label
ex:NumericalVariableMustHaveLabel
    a sh:NodeShape ;
    sh:targetClass m4i:NumericalVariable ;
    sh:property [
        sh:path rdfs:label ;
        sh:message "A m4i:NumericalVariable instances must have an rdfs:label." ;
        sh:minCount 1 ;
    ] .
"""
