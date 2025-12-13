import unittest

import h5rdmtoolbox as h5tbx
from rdflib import DCTERMS, PROV

from opencefadb.validation import validate_hdf
from opencefadb.validation.shacl.templates.hdf import SHALL_HAVE_CREATED_DATE, NUMERIC_DATASETS_SHALL_HAVE_UNIT, \
    SHALL_HAVE_CREATOR


class TestFileValidation(unittest.TestCase):

    def test_hdf_numeric_datasets_shall_have_unit(self):
        with h5tbx.File() as h5:
            h5.create_dataset(
                "numeric_dataset_no_unit",
                data=[1, 2, 3, 4, 5],
                dtype='i4'
            )

        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=NUMERIC_DATASETS_SHALL_HAVE_UNIT
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
            shacl_data=NUMERIC_DATASETS_SHALL_HAVE_UNIT
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
            shacl_data=NUMERIC_DATASETS_SHALL_HAVE_UNIT
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
