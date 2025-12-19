import unittest

import h5rdmtoolbox as h5tbx
import rdflib
from ontolutils.ex.qudt import Unit
from ontolutils.ex.sosa import ObservableProperty, Sensor
from ontolutils.ex.ssn import Accuracy, SystemCapability, MeasurementRange

from opencefadb.validation import validate_hdf
from opencefadb.validation.shacl.templates.sensor import SHALL_HAVE_WELL_DESCRIBED_SSN_SENSOR


class TestPlotting(unittest.TestCase):

    def test_shall_have_a_well_described_ssn_sensor_description(self):
        oprop = ObservableProperty(
            id="http://example.org/observable_property/1",
        )
        measurement_range1 = MeasurementRange(
            id="http://example.org/measurement_range/1",
            min_value="0",
            max_value="250",
            unit_code=Unit(
                id="http://qudt.org/vocab/unit/PA"
            )
        )
        u_pa = Unit(
            id="http://qudt.org/vocab/unit/PA"
        )
        measurement_range2 = MeasurementRange(
            id="http://example.org/measurement_range/2",
            min_value="0",
            max_value="500",
            unit_code=u_pa
        )
        accuracy_1 = Accuracy(
            id="http://example.org/accuracy/1",
            value=0.01 * 250,
            unit_code=u_pa,
            comment="Max error bound (±1%FS) for range 0–250 Pa (FS=250 Pa).@en"
        )
        accuracy_2 = Accuracy(
            id="http://example.org/accuracy/2",
            value=0.01 * 500,
            unit_code=u_pa,
            comment="Max error bound (±1%FS) for range 0–500 Pa (FS=500 Pa).@en"
        )
        capability_1 = SystemCapability(
            id="http://example.org/system_capability/1",
            hasSystemProperty=[accuracy_1, measurement_range1],
            forProperty=oprop
        )
        capability_2 = SystemCapability(
            id="http://example.org/system_capability/2",
            hasSystemProperty=[accuracy_2, measurement_range2],
            forProperty=oprop
        )
        sensor = Sensor(
            id="http://example.org/tool/KalinskyDS2-1",
            observes=oprop,
            hasSystemCapability=[capability_1, capability_2],
            label="Kalinsky Sensor TYPE DS 1@en"
        )

        ttl = sensor.serialize("ttl")
        from pyshacl import validate as pyshacl_validate
        data_graph = rdflib.Graph().parse(data=ttl, format="ttl")
        shacl_graph = rdflib.Graph().parse(
            data=SHALL_HAVE_WELL_DESCRIBED_SSN_SENSOR,
            format="ttl"
        )

        conforms, results_graph, results_text = pyshacl_validate(
            data_graph,
            shacl_graph=shacl_graph,
            inference="rdfs",
            abort_on_first=False,
            meta_shacl=False,
            advanced=False,
            debug=False
        )
        self.assertTrue(conforms)

        with h5tbx.File() as h5:
            h5.create_group("/test_group")
            ttl = sensor.serialize("ttl")
        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=SHALL_HAVE_WELL_DESCRIBED_SSN_SENSOR
        )
