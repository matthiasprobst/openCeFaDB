```
   ____                    _____     ______    _____  ____  
  / __ \                  / ____|   |  ____|  |  __ \|  _ \ 
 | |  | |_ __   ___ _ __ | |     ___| |__ __ _| |  | | |_) |
 | |  | | '_ \ / _ \ '_ \| |    / _ \  __/ _` | |  | |  _ < 
 | |__| | |_) |  __/ | | | |___|  __/ | | (_| | |__| | |_) |
  \____/| .__/ \___|_| |_|\_____\___|_|  \__,_|_____/|____/ 
        | |                                                 
        |_|     
```
# A FAIR Database for a Generic Centrifugal Fan

The `openCeFaDB` package provides access and interface to the OpenCeFaDB, a database for a generic centrifugal fan,
which focuses on accomplishing the FAIR principles in the database design.

Data is published on Zenodo, which contains raw data as HDF5 files together with the metadata in RDF/Turtle format.

The key design is to work with the (semantic) metadata to identify relevant data files for further analysis. Here are 
the main features of the database:

1. Separation of data and metadata: The actual data files (HDF5) are separated from the metadata (RDF/Turtle). This allows flexible and efficient querying of metadata without the need to load large data files.
2. Use of standard formats: The database uses standard formats for both data (HDF5) and metadata (RDF/Turtle), which
ensures compatibility with a wide range of tools and software.
3. FAIR principles: The database (content) is designed to be Findable, Accessible, Interoperable, and Reusable (FAIR).
4. Extensibility: The database is designed to be extensible, allowing for the addition of new data and metadata as needed.
5. Open access: The database is openly accessible to anyone, promoting transparency and collaboration in research.
6. Comprehensive metadata: The metadata includes detailed information about the data, including its provenance, and context, which enhances its usability and reusability.


In principle, the database interface provided through this package allows users to work with any RDF data. In order to 
narrow down the scope, the OpenCeFaDB defines a configuration file, which describes the files, that are relevant for the
database.

---

## Working with the OpenCeFaDB
The following steps are needed to work with the OpenCeFaDB:
1. Download the configuration (https://doi.org/10.5281/zenodo.18349358)
2. Download the metadata files defined in the configuration
3. Load the metadata into an RDF store
4. Query the metadata to identify relevant data files
5. Download the relevant raw (hdf) data files for further analysis

Since this above steps may require seme technical knowledge and knowledge about RDF stores, this repository provides ready-to-use
commands and functions to perform these tasks. We recommend using the command line interface (CLI) for initial setup of the database.

## Install the package

The package is available via PyPI. You can install it via pip:

```bash
pip install opencefadb
```

## Quickstart

A quickstart guide is provided as Jupyter Notebook in the `notebooks/` folder.


## Database Analysis

The further analysis should be made in Python scripts or Jupyter Notebooks.

## Viewer (experimental!)

The package also provides a simple viewer to explore the metadata in a streamlit app:

The viewer provides an easy-to-use interface to explore the metadata and identify relevant data files for further analysis. 
Upload options are provided to load the metadata from a local RDF store (GraphDB) or a SPARQL endpoint.

```bash
opencefadb viewer
```


![streamlit viewer screenshot](docs/viewer-screenshot.png)