import pathlib
import unittest

import diss
from matplotlib import pyplot as plt
from ontolutils.ex import dcat, hdf5
from ontolutils.ex.ssn import Result
from ontolutils.namespacelib import QUDT_UNIT
from ssnolib.m4i import NumericalVariable

from opencefadb.entities import WIKIDATA_ITS_FAN_V1, WIKIDATA_FAN_OPERATING_POINT
from opencefadb.models import Observation, ObservationCollection
from opencefadb.models.fan_curve import SemanticFanCurve

__this_dir__ = pathlib.Path(__file__).parent


class TestModels(unittest.TestCase):

    def test_fan_curve(self):
        """A fan curve is made up of multiple observations, hence a ObservationCollection"""
        vfr1 = Result(
            id="https://example.org/result/vfr1",
            hasNumericalVariable=NumericalVariable(
                id="https://example.org/numvar/1",
                label="Volumetric Flow Rate",
                has_standard_name="https://example.org/standard_name/air_flow_rate",
                has_numerical_value=10.0,
                units=QUDT_UNIT.M3_PER_HR,
            )
        )
        dp1 = Result(
            id="https://example.org/result/dp1",
            hasNumericalVariable=NumericalVariable(
                id="https://example.org/numvar/2",
                label="Static Pressure Difference",
                has_standard_name="https://example.org/standard_name/static_pressure_difference",
                has_numerical_value=80.0,
                units=QUDT_UNIT.PA,
            )
        )
        n1 = Result(
            id="https://example.org/result/n1",
            hasNumericalVariable=NumericalVariable(
                id="https://example.org/numvar/3",
                label="Rotational Speed",
                has_standard_name="https://example.org/standard_name/rotational_speed",
                has_numerical_value=600.0,
                units=QUDT_UNIT.PER_MIN,
            )
        )

        vfr2 = Result(
            id="https://example.org/result/vfr2",
            hasNumericalVariable=NumericalVariable(
                id="https://example.org/numvar/4",
                label="Volumetric Flow Rate",
                has_standard_name="https://example.org/standard_name/air_flow_rate",
                has_numerical_value=50.0,
                units=QUDT_UNIT.M3_PER_HR,
            )
        )
        dp2 = Result(
            id="https://example.org/result/dp2",
            hasNumericalVariable=NumericalVariable(
                label="Static Pressure Difference",
                id="https://example.org/numvar/5",
                has_standard_name="https://example.org/standard_name/static_pressure_difference",
                has_numerical_value=30.0,
                units=QUDT_UNIT.PA,
            )
        )
        n2 = Result(
            id="https://example.org/result/n2",
            hasNumericalVariable=NumericalVariable(
                id="https://example.org/numvar/6",
                label="Rotational Speed",
                has_standard_name="https://example.org/standard_name/rotational_speed",
                has_numerical_value=600.0,
                units=QUDT_UNIT.PER_MIN,
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

        from opencefadb.models.fan_curve import DefaultLabelResolver

        DefaultLabelResolver.LABEL_SELECTION_ORDER = {
            "label",
            "standard_name",
        }

        with diss.plotting.DissSingleAxis(
                scale=1.0,
                filename="test_fan_curve.svg",
        ) as dax:
            sfc.plot(
                x="https://example.org/standard_name/air_flow_rate",
                y="https://example.org/standard_name/static_pressure_difference",
                xlabel=None,
                ylabel=None,
                label="Test Fan Curve",
                marker="o",
                linestyle='-',
                ax=dax.ax,
            )
            plt.legend()
            plt.tight_layout()
            # plt.show()

    def test_observation(self):
        dist = dcat.Distribution(
            id="https://example.org/distribution/1",
        )
        h5file = hdf5.File(
            id="https://example.org/file/1",
            distribution=dist
        )
        res = Result(
            id="https://example.org/result/1",
        )
        o = Observation(
            id="https://example.org/observation/1",
            has_result=res,
            hadPrimarySource=h5file
        )
        print(o.serialize("ttl"))
