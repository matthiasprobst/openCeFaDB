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
1. Download the configuration
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

Generally, use the help command to get an overview of the available commands:

```bash
opencefadb --help
```

Otherwise, follow the steps below to initialize the database and load the metadata into an RDF store.

### 1. Initialize the database

First, you need to download the configuration metadata file from the main opencefadb zenodo record:

https://zenodo.org/record/17903401

Note, that at the time accessing the above link, there may be a newer version already available. Follow the instructions
on the zenodo page to download the configuration file.

**Alternatively** (and recommended) you can use the CLI tool, too, which downloads the latest configuration (or a specific version) for you:

```bash
opencefadb pull
# or with with parameters:
# opencefadb pull --target-dir=. --version=latest
``` 

Next, initialize the database by passing the configuration file to the `init` command. This will download the metadata 
files to the working directory, which you also may define (In the example below, we use the current directory).

```bash
opencefadb init --config=opencefadb-config.ttl
# or with parameters:
# opencefadb init --config=opencefadb-config.ttl --working-directory=.
```

### 2. Add metadata to an RDF store

Now that we downloaded the metadata files, we need to add them to an RDF store of your choice.

There are currently two options supported via the CLI to add the metadata to an RDF store:
1. Using a local RDF database, e.g. GraphDB
2. Using a local SPARQL endpoint, e.g. via `rdflib`

#### Option 1: Using a (local) instance of GraphDB

Start a local instance of GraphDB.

If not yet done, create a repository in GraphDB to hold the metadata:
```bash
opencefadb graphdb create --name="opencefadb-sandbox" --url="http://localhost:7201"   
```
Note, that the `--name` and `--url` parameters must match your preferences and GraphDB setup respectively.

Then, you can load the metadata into the repository via console:
```bash
opencefadb graphdb add --repo="opencefadb-sandbox" --dir="metadata" --suffix=".ttl" --recursive --url="http://localhost:7201"
```

#### Option 2: Using a local SPARQL endpoint (rdflib)
You can also use a local SPARQL endpoint via `rdflib`. For this, first install `rdflib-endpoint`:
```bash
pip install rdflib-endpoint[cli]
```

Then, you can start a local SPARQL endpoint serving the downloaded metadata. The below command 
assumes that the metadata files are stored in the `metadata` subfolder of the current working directory and 
that all files have the `.ttl` suffix (which should be the case for the OpenceFaDB. If you have custom data in other 
RDF formats to add, adjust the command accordingly):

```bash
rdflib-endpoint serve .\metadata\**\*.ttl 
```

## Database Analysis

The further analysis should be made in Python scripts or Jupyter Notebooks.