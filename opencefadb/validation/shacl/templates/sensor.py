SHALL_HAVE_WELL_DESCRIBED_SSN_SENSOR = '''@prefix ex: <https://example.org/> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix schema: <http://schema.org/> .
@prefix sosa: <http://www.w3.org/ns/sosa/> .
@prefix ssn: <http://www.w3.org/ns/ssn/> .
@prefix ssn_system: <http://www.w3.org/ns/ssn/systems/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

#################################################################
# Top-level sensor shape
#################################################################

ex:SensorMetadataShape
  a sh:NodeShape ;
  sh:targetClass sosa:Sensor ;

  sh:property [
    sh:path rdfs:label ;
    sh:minCount 1 ;
    sh:datatype rdf:langString ;
    sh:message "Sensor must have rdfs:label with a language tag." ;
  ] ;

  sh:property [
    sh:path sosa:observes ;
    sh:minCount 1 ;
    sh:class sosa:ObservableProperty ;
    sh:message "Sensor must sosa:observes at least one sosa:ObservableProperty." ;
  ] ;

  sh:property [
    sh:path ssn_system:hasSystemCapability ;
    sh:minCount 1 ;
    sh:node ex:SystemCapabilityShape ;
    sh:message "Sensor must have at least one ssn_system:SystemCapability." ;
  ] .

#################################################################
# Capability shape (one range, optional accuracy)
#################################################################

ex:SystemCapabilityShape
  a sh:NodeShape ;
  sh:class ssn_system:SystemCapability ;

  sh:property [
    sh:path ssn:forProperty ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
    sh:class sosa:ObservableProperty ;
    sh:message "Each capability must have exactly one ssn:forProperty (an ObservableProperty)." ;
  ] ;

  # Exactly one MeasurementRange per capability
  sh:property [
    sh:path ssn_system:hasSystemProperty ;
    sh:qualifiedValueShape ex:MeasurementRangeShape ;
    sh:qualifiedMinCount 1 ;
    sh:qualifiedMaxCount 1 ;
    sh:message "Each SystemCapability must have exactly one ssn_system:MeasurementRange as a system property." ;
  ] ;

  # Optional Accuracy per capability (0..1), numeric or textual spec allowed
  sh:property [
    sh:path ssn_system:hasSystemProperty ;
    sh:qualifiedValueShape ex:AccuracyShape ;
    sh:qualifiedMinCount 0 ;
    sh:qualifiedMaxCount 1 ;
    sh:message "Each SystemCapability may have at most one ssn_system:Accuracy as a system property." ;
  ] ;

  # Ensure forProperty is among the sensor's observed properties
  sh:sparql [
    a sh:SPARQLConstraint ;
    sh:message "Capability ssn:forProperty must be one of the parent sensor's sosa:observes properties." ;
    sh:select """
      SELECT $this
      WHERE {
        ?sensor ssn_system:hasSystemCapability $this ;
                sosa:observes ?obs .
        $this ssn:forProperty ?p .
        FILTER NOT EXISTS { ?sensor sosa:observes ?p }
      }
    """ ;
  ] .

#################################################################
# MeasurementRange shape
#################################################################

ex:MeasurementRangeShape
  a sh:NodeShape ;
  sh:class ssn_system:MeasurementRange ;

  sh:property [
    sh:path schema:minValue ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
    sh:datatype xsd:double ;
    sh:message "MeasurementRange must have exactly one schema:minValue (xsd:double)." ;
  ] ;

  sh:property [
    sh:path schema:maxValue ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
    sh:datatype xsd:double ;
    sh:message "MeasurementRange must have exactly one schema:maxValue (xsd:double)." ;
  ] ;

  sh:property [
    sh:path schema:unitCode ;
    sh:minCount 1 ;
    sh:maxCount 1 ;
    sh:nodeKind sh:IRI ;
    sh:message "MeasurementRange must have exactly one schema:unitCode (IRI)." ;
  ] ;

  # Sanity check: min < max
  sh:sparql [
    a sh:SPARQLConstraint ;
    sh:message "MeasurementRange must satisfy minValue < maxValue." ;
    sh:select """
      SELECT $this
      WHERE {
        $this schema:minValue ?min ;
              schema:maxValue ?max .
        FILTER (?min >= ?max)
      }
    """ ;
  ] .

#################################################################
# Accuracy shape (supports numeric OR textual spec)
#################################################################

ex:AccuracyShape
  a sh:NodeShape ;
  sh:class ssn_system:Accuracy ;

  # Either numeric accuracy...
  sh:or (
    [
      sh:property [
        sh:path schema:value ;
        sh:minCount 1 ;
        sh:maxCount 1 ;
        sh:datatype xsd:double ;
        sh:message "If using numeric accuracy, provide schema:value as xsd:double." ;
      ] ;
      sh:property [
        sh:path schema:unitCode ;
        sh:minCount 1 ;
        sh:maxCount 1 ;
        sh:nodeKind sh:IRI ;
        sh:message "If using numeric accuracy, provide schema:unitCode as IRI." ;
      ] ;
    ]
    # ...or textual spec (good for PT100 class formulas, etc.)
    [
      sh:property [
        sh:path rdfs:comment ;
        sh:minCount 1 ;
        sh:datatype rdf:langString ;
        sh:message "If using textual accuracy spec, provide rdfs:comment with a language tag." ;
      ] ;
    ]
    [
      sh:property [
        sh:path schema:description ;
        sh:minCount 1 ;
        sh:datatype rdf:langString ;
        sh:message "If using textual accuracy spec, provide schema:description with a language tag." ;
      ] ;
    ]
  ) .

'''
