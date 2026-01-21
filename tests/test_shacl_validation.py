import unittest

import h5rdmtoolbox as h5tbx
import h5rdmtoolbox as h5tbx
import pytest
import rdflib
from ontolutils.ex.qudt import Unit
from ontolutils.ex.sosa import ObservableProperty, Sensor
from ontolutils.ex.ssn import Accuracy, SystemCapability, MeasurementRange
from rdflib import DCTERMS, PROV

from opencefadb.validation import validate_hdf
from opencefadb.validation.shacl.templates.hdf import SHALL_HAVE_CREATED_DATE, NUMERIC_DATASETS_SHALL_HAVE_UNIT_AND_KIND_OF_QUANTITY, \
    SHALL_HAVE_CREATOR
from opencefadb.validation.shacl.templates.sensor import SHALL_HAVE_WELL_DESCRIBED_SSN_SENSOR


class TestPlotting(unittest.TestCase):

    def test_hdf_numeric_datasets_shall_have_unit(self):
        with h5tbx.File() as h5:
            h5.create_dataset(
                "numeric_dataset_no_unit",
                data=[1, 2, 3, 4, 5],
                dtype='i4'
            )

        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=NUMERIC_DATASETS_SHALL_HAVE_UNIT_AND_KIND_OF_QUANTITY
        )
        self.assertFalse(res.conforms)

        with h5tbx.File() as h5:
            ds = h5.create_dataset(
                "numeric_dataset_no_unit",
                data=[1, 2, 3, 4, 5],
                dtype='i4',
                attrs={"units": "m/s"}
            )
            ds.rdf["units"].predicate = "http://w3id.org/nfdi4ing/metadata4ing#hasUnit"

        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=NUMERIC_DATASETS_SHALL_HAVE_UNIT_AND_KIND_OF_QUANTITY
        )
        self.assertFalse(res.conforms)

        with h5tbx.File() as h5:
            ds = h5.create_dataset(
                "numeric_dataset_no_unit",
                data=[1, 2, 3, 4, 5],
                dtype='i4',
                attrs={"units": "m/s"}
            )
            ds.rdf["units"].predicate = "http://w3id.org/nfdi4ing/metadata4ing#hasUnit"
            ds.rdf.object["units"] = "https://qudt.org/vocab/unit/M-PER-SEC"

        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=NUMERIC_DATASETS_SHALL_HAVE_UNIT_AND_KIND_OF_QUANTITY
        )
        self.assertTrue(res.conforms)

    def test_hdf_file_has_creator(self):
        with h5tbx.File() as h5:
            pass

        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=SHALL_HAVE_CREATOR
        )
        self.assertFalse(res.conforms)
        self.assertEqual(
            res.messages[0],
            "Each hdf:File must have at least one dcterms:creator which is either an IRI or a prov:Person."
        )

        # Now add the creator attribute
        with h5tbx.File(h5.hdf_filename, mode='a') as h5:
            h5.attrs["creator"] = "Matthias Probst"
            h5.frdf["creator"].predicate = DCTERMS.creator

        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=SHALL_HAVE_CREATOR
        )
        self.assertFalse(res.conforms)
        self.assertEqual(
            res.messages[0],
            "Each dcterms:creator must be either an IRI or a prov:Person."
        )

        with h5tbx.File(h5.hdf_filename, mode='a') as h5:
            h5.attrs["creator"] = "https://orcid.org/0000-0001-8729-0482"
            h5.frdf["creator"].predicate = DCTERMS.creator
        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=SHALL_HAVE_CREATOR
        )
        self.assertTrue(res.conforms)

        with h5tbx.File(h5.hdf_filename, mode='a') as h5:
            h5.attrs["creator"] = "Matthias Probst"
            h5.frdf["creator"].predicate = DCTERMS.creator
            h5.frdf["creator"].object = PROV.Person
        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=SHALL_HAVE_CREATOR
        )
        self.assertTrue(res.conforms)

    def test_hdf_file_has_created_data(self):
        with h5tbx.File() as h5:
            pass

        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=SHALL_HAVE_CREATED_DATE
        )
        self.assertFalse(res.conforms)
        self.assertEqual(
            res.messages[0],
            "Each hdf:File must have exactly one dcterms:created value of type xsd:date."
        )

        # Now add the creat attribute
        with h5tbx.File(h5.hdf_filename, mode='a') as h5:
            h5.attrs["created"] = "2025-01-10"
            h5.frdf["created"].predicate = "http://purl.org/dc/terms/created"

        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=SHALL_HAVE_CREATED_DATE
        )
        self.assertTrue(res.conforms)

    @pytest.mark.skip(reason="Skipping for now, as pyshacl seems to have issues with complex shapes.")
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
            format="ttl",
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
        if not conforms:
            print(results_text)
        self.assertTrue(conforms)

        with h5tbx.File() as h5:
            h5.create_group("/test_group")
            ttl = sensor.serialize("ttl")
        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=SHALL_HAVE_WELL_DESCRIBED_SSN_SENSOR
        )
