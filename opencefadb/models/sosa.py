from typing import Optional, Union

from ontolutils import Thing, urirefs, namespaces
from ontolutils.ex.sosa import Sensor, ObservableProperty
from ontolutils.ex.ssn import FeatureOfInterest
from ontolutils.ex.ssn import Observation
from ontolutils.typing import AnyThing, AnyIriOrListOf, AnyIriOf
from pydantic import Field

__version__ = "2017.10.19"

# "hadPrimarySource" is not included in the base Observation class, so we add it here
Observation.add_property(
    name="hadPrimarySource",
    property_type=Optional[AnyThing],
    namespace="http://www.w3.org/ns/prov#",
    namespace_prefix="prov",
    default=None
)


@namespaces(ssn="http://www.w3.org/ns/ssn/")
@urirefs(Input="ssn:Input")
class Input(Thing):
    """Input - Any information that is provided to a Procedure for its use."""


@namespaces(ssn="http://www.w3.org/ns/ssn/")
@urirefs(Output="ssn:Output")
class Output(Thing):
    """has Output - Relation between a Procedure and an Output of it."""


@namespaces(sosa="http://www.w3.org/ns/sosa/",
            ssn="http://www.w3.org/ns/ssn/")
@urirefs(Procedure="sosa:Procedure",
         hasInput="ssn:hasInput",
         hasOutput="ssn:hasOutput",
         implementedBy="ssn:implementedBy"
         )
class Procedure(Thing):
    hasInput: Optional[AnyIriOrListOf[Input]] = Field(default=None, alias="has_input")
    hasOutput: Optional[AnyIriOrListOf[Output]] = Field(default=None, alias="has_output")
    implementedBy: Optional[AnyThing] = Field(default=None, alias="implemented_by")


@namespaces(ssn="http://www.w3.org/ns/ssn/")
@urirefs(Stimulus="ssn:Stimulus")
class Stimulus(Thing):
    """Stimulus - An event in the real world that 'triggers' the Sensor. The properties associated to the Stimulus may be different to the eventual observed ObservableProperty. It is the event, not the object, that triggers the Sensor."""


@namespaces(sosa="http://www.w3.org/ns/sosa/",
            ssn="http://www.w3.org/ns/ssn/")
@urirefs(ObservationCollection="sosa:ObservationCollection",
         madeBySensor="sosa:madeBySensor",
         observedProperty="sosa:observedProperty",
         hasFeatureOfInterest="sosa:hasFeatureOfInterest",
         hasUltimateFeatureOfInterest="sosa:hasUltimateFeatureOfInterest",
         usedProcedure="sosa:usedProcedure",
         wasOriginatedBy="ssn:wasOriginatedBy",
         phenomenonTime="sosa:phenomenonTime",
         resultTime="sosa:resultTime",
         hasMember="sosa:hasMember",
         )
class ObservationCollection(Thing):
    # maxCardinality 1 -> single-valued (optional)
    madeBySensor: Union[AnyIriOf[Sensor]] = Field(
        default=None,
        alias="made_by_sensor",
        description="The sensor that made the observations (max 1)."
    )

    observedProperty: Union[AnyIriOf[ObservableProperty]] = Field(
        default=None,
        alias="observed_property",
        description="The property that was observed for the collection (max 1)."
    )

    usedProcedure: Union[AnyIriOf[Procedure]] = Field(
        default=None,
        alias="used_procedure",
        description="Procedure used for the observations in the collection (max 1)."
    )

    wasOriginatedBy: Union[AnyIriOf[Stimulus]] = Field(
        default=None,
        alias="was_originated_by",
        description="Stimulus that originated the observations (ssn:wasOriginatedBy)."
    )

    phenomenonTime: Union[AnyThing, str] = Field(
        default=None,
        alias="phenomenon_time",
        description="Time when the phenomenon was (single value)."
    )

    resultTime: Union[AnyThing, str] = Field(
        default=None,
        alias="result_time",
        description="Time when the result was generated (single value)."
    )

    # hasFeatureOfInterest / hasUltimateFeatureOfInterest are single-valued (max 1)
    hasFeatureOfInterest: Union[AnyIriOf[FeatureOfInterest]] = Field(
        default=None,
        alias="has_feature_of_interest",
        description="Relation to the feature of interest (max 1)."
    )

    hasUltimateFeatureOfInterest: Union[AnyIriOf[FeatureOfInterest]] = Field(
        default=None,
        alias="has_ultimate_feature_of_interest",
        description="Ultimate feature of interest for the collection (max 1)."
    )

    # minCardinality 1 -> required (non-empty) list of members
    hasMember: AnyIriOrListOf[Observation] = Field(
        default=None,
        alias="has_member",
        description="Members of the collection (min 1)."
    )
