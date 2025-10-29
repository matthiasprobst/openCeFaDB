import unittest

import h5rdmtoolbox as h5tbx
from h5rdmtoolbox.ld.shacl import validate_hdf

from opencefadb.shacl.templates import hdf_file_must_have_creator


class TestFileValidation(unittest.TestCase):

    def test_hdf_file_has_creator(self):
        with h5tbx.File() as h5:
            pass

        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=hdf_file_must_have_creator
        )
        self.assertFalse(res.conforms)
        self.assertEqual(
            res.messages[0],
            "Each hdf:File must have exactly one dcterms:created value of type xsd:date."
        )

        # Now add the creator attribute
        with h5tbx.File(h5.hdf_filename, mode='a') as h5:
            h5.attrs["created"] = "2025-01-10"
            h5.frdf["created"].predicate = "http://purl.org/dc/terms/created"

        res = validate_hdf(
            hdf_source=h5.hdf_filename,
            shacl_data=hdf_file_must_have_creator
        )
        self.assertTrue(res.conforms)
