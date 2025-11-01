import pathlib
from typing import Union

import h5py
from h5rdmtoolbox.ld.shacl import validate_hdf


def validate_measurement_hdf(source: Union[pathlib.Path, str, h5py.File]):
    from .shacl.templates.hdf import NUMERIC_DATASETS_SHALL_HAVE_UNIT
    validate_hdf(hdf_source=source, shacl_data=NUMERIC_DATASETS_SHALL_HAVE_UNIT)
