from .templates import dcat
from .templates import hdf
from .templates import m4i
from .templates import person
from .templates import sensor
from .templates import qudt

MEASUREMENT_HDF_FILE_CONTENT_SHACL_DEFINITIONS = (
    # dcat.MINIMUM_DATASET_SHACL,
    hdf.NUMERIC_DATASETS_SHALL_HAVE_UNIT_AND_KIND_OF_QUANTITY,
    hdf.SHALL_HAVE_CREATOR,
    hdf.SHALL_HAVE_CREATED_DATE,
    hdf.SHALL_HAVE_VALID_ATTRIBUTION,
    hdf.HDF_FILE_SHALL_HAVE_STANDARD_NAME_TABLE,
    m4i.NUMERICAL_VARIABLE_HAS_LABEL,
    person.PERSON_SHACL,
    sensor.SHALL_HAVE_WELL_DESCRIBED_SSN_SENSOR,
    qudt.UNIT_NODE_SHALL_HAVE_KIND_OF_QUANTITY
)
