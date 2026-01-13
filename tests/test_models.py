import pathlib
import unittest

from matplotlib import pyplot as plt
from ontolutils.ex import hdf5
from ontolutils.ex.sosa import Result, Observation, ObservationCollection
from ontolutils.namespacelib import QUDT_UNIT, QUDT_KIND
from ssnolib.m4i import NumericalVariable

from opencefadb import plotting
from opencefadb.entities import WIKIDATA_ITS_FAN_V1, WIKIDATA_FAN_OPERATING_POINT
from opencefadb.models.fan_curve import DefaultLabelResolver, SemanticFanCurve

__this_dir__ = pathlib.Path(__file__).parent

from opencefadb.models.operating_point import is_operating_point


class TestModels(unittest.TestCase):

    def setUp(self):
        self.valid_observation = Observation(
            id="https://example.org/observation/1",
            has_result=[
                Result(
                    id="https://example.org/result/vfr1",
                    has_numerical_variable=NumericalVariable(
                        id="https://example.org/numvar/1",
                        has_standard_name="https://example.org/standard_name/air_flow_rate",
                        hasUnit=QUDT_UNIT.M3_PER_HR
                    )
                ),
                Result(
                    id="https://example.org/result/dp1",
                    has_numerical_variable=NumericalVariable(
                        id="https://example.org/numvar/2",
                        has_standard_name="https://example.org/standard_name/static_pressure_difference",
                        hasUnit=QUDT_UNIT.PA,
                        has_kind_of_quantity=QUDT_KIND.StaticPressure
                    )
                ),
                Result(
                    id="https://example.org/result/n1",
                    has_numerical_variable=NumericalVariable(
                        id="https://example.org/numvar/3",
                        has_standard_name="https://example.org/standard_name/rotational_speed",
                        hasUnit=QUDT_UNIT.PER_MIN
                    )
                ),
            ],
        )

    def test_fan_curve(self):
        """A fan curve is made up of multiple observations, hence a ObservationCollection"""
        import requests
        requests.get("https://zenodo.org/records/17271932/files/Standard_Name_Table_for_the_Property_Descriptions_of_Centrifugal_Fans.jsonld",
                     timeout=60).raise_for_status()
        vfr1 = Result(
            id="https://example.org/result/vfr1",
            has_numerical_variable=NumericalVariable(
                id="https://example.org/numvar/1",
                label="Volumetric Flow Rate",
                has_standard_name="https://example.org/standard_name/air_flow_rate",
                has_numerical_value=10.0,
                units=QUDT_UNIT.M3_PER_HR,
                has_kind_of_quantity=QUDT_KIND.VolumeFlowRate
            )
        )
        dp1 = Result(
            id="https://example.org/result/dp1",
            has_numerical_variable=NumericalVariable(
                id="https://example.org/numvar/2",
                label="Static Pressure Difference",
                has_standard_name="https://example.org/standard_name/static_pressure_difference",
                has_numerical_value=80.0,
                units=QUDT_UNIT.PA,
                has_kind_of_quantity=QUDT_KIND.StaticPressure
            )
        )
        n1 = Result(
            id="https://example.org/result/n1",
            has_numerical_variable=NumericalVariable(
                id="https://example.org/numvar/3",
                label="Rotational Speed",
                has_standard_name="https://example.org/standard_name/rotational_speed",
                has_numerical_value=600.0,
                has_unit=QUDT_UNIT.REV_PER_MIN,
                has_kind_of_quantity=QUDT_KIND.RotationalVelocity
            )
        )

        vfr2 = Result(
            id="https://example.org/result/vfr2",
            has_numerical_variable=NumericalVariable(
                id="https://example.org/numvar/4",
                label="Volumetric Flow Rate",
                has_standard_name="https://example.org/standard_name/air_flow_rate",
                has_numerical_value=50.0,
                has_unit=QUDT_UNIT.M3_PER_HR,
                has_kind_of_quantity=QUDT_KIND.VolumeFlowRate
            )
        )
        dp2 = Result(
            id="https://example.org/result/dp2",
            has_numerical_variable=NumericalVariable(
                label="Static Pressure Difference",
                id="https://example.org/numvar/5",
                has_standard_name="https://example.org/standard_name/static_pressure_difference",
                has_numerical_value=30.0,
                has_unit=QUDT_UNIT.PA,
                has_kind_of_quantity=QUDT_KIND.StaticPressure
            )
        )
        n2 = Result(
            id="https://example.org/result/n2",
            has_numerical_variable=NumericalVariable(
                id="https://example.org/numvar/6",
                label="Rotational Speed",
                has_standard_name="https://example.org/standard_name/rotational_speed",
                has_numerical_value=600.0,
                has_unit=QUDT_UNIT.REV_PER_MIN,
                has_kind_of_quantity=QUDT_KIND.RotationalVelocity
            )
        )

        op1 = Observation(
            id="https://example.org/observation/1",
            hadPrimarySource=hdf5.File(
                id="https://example.org/file/1",
            ),
            has_result=[vfr1, dp1, n1]
        )
        op2 = Observation(
            id="https://example.org/observation/2",
            hadPrimarySource=hdf5.File(
                id="https://example.org/file/1",
            ),
            has_result=[vfr2, dp2, n2]
        )

        observation_collection = ObservationCollection(
            id="https://example.org/observation_collection/1",
            hasFeatureOfInterest=WIKIDATA_ITS_FAN_V1,
            hasMember=[op1, op2],
            type=WIKIDATA_FAN_OPERATING_POINT
        )
        ttl = observation_collection.serialize("ttl")

        sfc = SemanticFanCurve.from_observations(
            [op1, op2],
            id="https://example.org/observation_collection/1",
            hasFeatureOfInterest=WIKIDATA_ITS_FAN_V1,
            type=WIKIDATA_FAN_OPERATING_POINT
        )
        ttl_sfc = sfc.serialize("ttl")
        self.assertEqual(ttl_sfc, ttl)

        sfc_scaled = sfc.scale(
            n=NumericalVariable(
                has_numerical_value=1200.0,
                units=QUDT_UNIT.PER_MIN,
                has_kind_of_quantity=QUDT_KIND.RotationalVelocity
            ),
            # x="https://example.org/standard_name/air_flow_rate",
            # y="https://example.org/standard_name/static_pressure_difference",
            # n="https://example.org/standard_name/rotational_speed",
        )
        print(sfc_scaled.serialize("ttl"))

        DefaultLabelResolver.LABEL_SELECTION_ORDER = {
            "label",
            "standard_name",
        }

        with plotting.SingleAxis(
                scale=1.0,
                filename="test_fan_curve.svg",
        ) as dax:
            sfc.plot(
                x="https://example.org/standard_name/air_flow_rate",
                y="https://example.org/standard_name/static_pressure_difference",
                xlabel=None,
                ylabel=None,
                label="raw",
                marker="o",
                linestyle='-',
                ax=dax.ax,
            )
            sfc_scaled.plot(
                x="https://example.org/standard_name/air_flow_rate",
                y="https://example.org/standard_name/static_pressure_difference",
                xlabel=None,
                ylabel=None,
                label="scaled",
                marker="+",
                linestyle='-',
                ax=dax.ax,
            )
            plt.legend()
            plt.tight_layout()
            plt.show()

    def test_is_operating_point(self):
        is_op = is_operating_point(self.valid_observation, verbose=True)
        self.assertTrue(is_op)

        invalid_op = Observation(
            id="https://example.org/observation/2",
            has_result=[
                Result(
                    id="https://example.org/result/vfr1",
                    has_numerical_variable=NumericalVariable(
                        id="https://example.org/numvar/1",
                        has_standard_name="https://example.org/standard_name/air_flow_rate",
                        hasUnit=QUDT_UNIT.M3_PER_HR
                    )
                ),
                Result(
                    id="https://example.org/result/dp1",
                    has_numerical_variable=NumericalVariable(
                        id="https://example.org/numvar/2",
                        has_standard_name="https://example.org/standard_name/static_pressure_difference",
                        hasUnit=QUDT_UNIT.PA
                    )
                ),
            ],
        )
        is_op_invalid = is_operating_point(invalid_op, verbose=True)
        self.assertFalse(is_op_invalid)

        another_invalid_op = Observation(
            id="https://example.org/observation/3",
            has_result=[]
        )
        is_op_another_invalid = is_operating_point(another_invalid_op, verbose=True)
        self.assertFalse(is_op_another_invalid)

        another_invalid_op = Observation(
            id="https://example.org/observation/4",
            has_result=[
                Result(
                    id="https://example.org/result/vfr1"
                ),
                Result(
                    id="https://example.org/result/dp1",
                    has_numerical_variable=NumericalVariable(
                        id="https://example.org/numvar/2",
                    )
                ),
                Result(
                    id="https://example.org/result/n1",
                    has_numerical_variable=NumericalVariable(
                        id="https://example.org/numvar/3",
                        has_standard_name="https://example.org/standard_name/temperature",  # wrong kind
                        has_unit=QUDT_UNIT.K
                    )
                ),
            ],
        )
        is_op_another_invalid = is_operating_point(another_invalid_op, verbose=True)
        self.assertFalse(is_op_another_invalid)
