UNIT_NODE_SHALL_HAVE_KIND_OF_QUANTITY = '''@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix m4i:  <http://w3id.org/nfdi4ing/metadata4ing#> .
@prefix qudt: <http://qudt.org/schema/qudt/> .

#################################################################
# Any node that uses m4i:hasUnit must also specify kind of quantity
# and both must point to the correct QUDT classes.
#################################################################

m4i:UnitAndQuantityKindShape
  a sh:NodeShape ;
  sh:targetSubjectsOf m4i:hasUnit ;

  # m4i:hasUnit -> IRI that is a qudt:Unit
  sh:property [
    sh:path m4i:hasUnit ;
    sh:minCount 1 ;
    sh:nodeKind sh:IRI ;
    sh:class qudt:Unit ;
    sh:message "m4i:hasUnit must point to an IRI that is a qudt:Unit." ;
  ] ;

  # m4i:hasKindOfQuantity -> IRI that is a qudt:QuantityKind
  sh:property [
    sh:path m4i:hasKindOfQuantity ;
    sh:minCount 1 ;
    sh:nodeKind sh:IRI ;
    sh:class qudt:QuantityKind ;
    sh:message "If a node has m4i:hasUnit, it must also have m4i:hasKindOfQuantity pointing to an IRI that is a qudt:QuantityKind." ;
  ] .
'''