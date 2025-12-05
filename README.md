# openCeFaDB

Thank you for your interest in the OpenCeFaDB, a database for a generic centrifugal fan, which focuses on
accomplishing the FAIR principles in the database desgin.

This is the Python package to interface and work with the database.

## Installation

Most features work for all Python versions higher than 3.9. However, due to limits in some dependencies,
the opencefadb is designed to work with Python 3.9 until 3.13.

We recommend using an Anaconda environment.

```bash
conda create -n opencefadb python=3.10
conda activate opencefadb
```

Install via pip:

```bash
pip install opencefadb
```

### CAD plotting

The CAD model can be plotted. For this you need to install occ via conda:

```bash
conda install -c conda-forge pythonocc-core
```

**Note** that this is not possible for python 3.11 or higher!

## Quickstart - CLI

There is a command line interface (CLI). Following, we provide a quickstart guide.

After installation, there are multiple actions you can perform via the command line:

### Initialize the database:

This creates the configuration files and downloads some files.

```bash
opencefadb init
```

### Reset the database:

If all goes wrong, you can reset the database:

```bash
opencefadb reset
```

Call `init` afterward to reinitialize the database.

### Configuration

You can configure which database stores/endpoints you want to use. There
are some default ones. Which one is used can be configured in the opencefadb-config.ini file. Here,
multiple profiles can be defined.

Select one, e.g. the test profile:

```bash
opencefadb config --profile test
```

### Info

At all times, you can get information about the database:

```bash
opencefadb info
```

### Get the fan parameters

```bash
opencefadb fan --show-parameters
```

### Get help

```bash
opencefadb --help
```

Note, that you can also call `--help` on subcommands:

```bash
opencefadb config --help
```