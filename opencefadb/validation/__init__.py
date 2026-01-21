import pathlib
from typing import Union

import h5py
from h5rdmtoolbox.ld.shacl import validate_hdf


def validate_measurement_hdf(source: Union[pathlib.Path, str, h5py.File]):
    from .shacl import MEASUREMENT_HDF_FILE_CONTENT_SHACL_DEFINITIONS
    for _shacl in MEASUREMENT_HDF_FILE_CONTENT_SHACL_DEFINITIONS:
        validate_hdf(hdf_source=source, shacl_data=_shacl)
