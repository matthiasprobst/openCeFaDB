import os
import pathlib
import sys
from textwrap import dedent
from typing import List, Optional

import click
import dotenv
import rdflib
import requests

from opencefadb import OpenCeFaDB

DEFAULT_GRAPHDB_URL = "http://localhost:7200"


@click.group()
def main():
    """OpenCeFaDB command-line interface."""


@main.group()
def graphdb():
    """GraphDB-related commands."""
    pass


@main.command("pull")
@click.option("version", "--version", required=False, type=str,
              help="Version of the config file, hence the zenodo record to download (default: latest).")
@click.option("target_dir", "--target-dir", required=False, type=click.Path(exists=False, file_okay=False),
              help="Target directory to save the config file (default: current working directory).")
@click.option("sandbox", "--sandbox", is_flag=True, help="Use the Zenodo sandbox (for testing; requires access token).")
def pull(version: Optional[str] = None, target_dir: Optional[str] = None, sandbox: bool = False):
    """downloads the latest OpenCeFaDB config file from zenodo."""
    config_filename = OpenCeFaDB.pull(version=version, target_directory=target_dir, sandbox=sandbox)
    click.echo(f"Downloaded config file to: {config_filename}")


@main.command("init")
@click.option("config_file", "--config", required=True, type=click.Path(exists=True, file_okay=True),
              help="Path to configuration file.")
@click.option("working_directory", "--working-directory", required=False, type=click.Path(exists=True, file_okay=False),
              help="Working directory.")
def init(config_file: str, working_directory: Optional[str] = None):
    """Database initialization commands."""
    from opencefadb import OpenCeFaDB
    OpenCeFaDB.initialize(
        working_directory=working_directory,
        config_filename=config_file
    )


@graphdb.command("create")
@click.option(
    "--name",
    "repo_name",
    required=False,
    help="Repository ID / name (e.g. 'test-repo').",
)
@click.option(
    "--title",
    default=None,
    help="Optional human-readable repository title.",
)
@click.option(
    "--url",
    "graphdb_url",
    default=DEFAULT_GRAPHDB_URL,
    show_default=True,
    help="Base URL of GraphDB (without trailing slash, default is http://localhost:7200).",
)
@click.option(
    "--config-file",
    "config_file",
    default=None,
    help="Path to custom config.ttl file. Other options are ignored if provided.",
)
@click.option("--env", "env_file", default=None, help="Path to .env file (loads GRAPHDB_USERNAME/PASSWORD).")
@click.option("--username", default=None, help="GraphDB username (overrides env vars).")
@click.option("--password", default=None, help="GraphDB password (overrides env vars, prompts if missing).")
def graphdb_create(repo_name: str = None,
                   title: str = None,
                   graphdb_url: str = None,
                   config_file: Optional[str] = None,
                   env_file: Optional[str] = None,
                   username: Optional[str] = None,
                   password: Optional[str] = None):
    """
    Create a new GraphDB repository using the REST API.
    """
    if config_file:
        click.echo("Using custom config file, other options will be ignored.")
        # Load custom config.ttl
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_ttl = f.read()
        except Exception as e:
            click.echo(f"Error: failed to read config file {config_file}: {e}", err=True)
            sys.exit(1)

        url = f"{graphdb_url.rstrip('/')}/rest/repositories"  # REST endpoint for repo creation. [web:6][web:9]
        files = {"config": (pathlib.Path(config_file).name, config_ttl, "text/turtle")}
        _load_env_file(env_file)

        # CLI args override env vars
        if username is None:
            username, password = _get_credentials()

        auth = _get_auth(graphdb_url, username, password)

        try:
            response = requests.post(url, files=files, auth=auth)
        except requests.RequestException as e:
            click.echo(f"Error: failed to contact GraphDB at {graphdb_url}: {e}", err=True)
            sys.exit(1)

        # get repo_name from config_ttl if not provided
        if repo_name is None:
            graphdb_config_graph = rdflib.Graph()
            graphdb_config_graph.parse(data=config_ttl, format='turtle')
            qres = graphdb_config_graph.query(
                """
                PREFIX rep: <http://www.openrdf.org/config/repository#>
                SELECT ?repoID WHERE {
                    ?repo a rep:Repository ;
                          rep:repositoryID ?repoID .
                } LIMIT 1
                """
            )
            for row in qres:
                repo_name = str(row.repoID)
                break

        if response.status_code in (200, 201, 204):
            click.echo(f"Repository '{repo_name}' created on {graphdb_url}")
        elif response.status_code == 409:
            click.echo(f"Repository '{repo_name}' already exists on {graphdb_url}", err=True)
            sys.exit(1)
        else:
            click.echo(
                f"Error: GraphDB responded with {response.status_code}:\n"
                f"{response.text}",
                err=True,
            )
            sys.exit(1)
        return

    if repo_name is None:
        click.echo("Error: --name is required if --config-file is not provided.", err=True)
        sys.exit(1)

    if title is None:
        title = repo_name

    # Minimal GraphDB config.ttl (file-based repo, rdfsplus-optimized ruleset).
    # This follows the standard template used in the GraphDB docs. [web:12]

    _load_env_file(env_file)

    # CLI args override env vars
    if username is None:
        username, password = _get_credentials()

    config_ttl = dedent(f"""
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix rep: <http://www.openrdf.org/config/repository#> .
        @prefix sr:  <http://www.openrdf.org/config/repository/sail#> .
        @prefix sail: <http://www.openrdf.org/config/sail#> .
        @prefix graphdb: <http://www.ontotext.com/trree/graphdb#> .

        [] a rep:Repository ;
           rep:repositoryID "{repo_name}" ;
           rdfs:label "{title}" ;
           rep:repositoryImpl [
               rep:repositoryType "graphdb:SailRepository" ;
               sr:sailImpl [
                   sail:sailType "graphdb:Sail" ;
                   graphdb:storage-folder "{repo_name}-storage" ;
                   graphdb:ruleset "rdfsplus-optimized" ;
                   graphdb:read-only "false" ;
                   graphdb:disable-sameAs "false"
               ]
           ] .
    """).strip()

    url = f"{graphdb_url.rstrip('/')}/rest/repositories"  # REST endpoint for repo creation. [web:6][web:9]
    files = {"config": ("config.ttl", config_ttl, "text/turtle")}
    auth = _get_auth(graphdb_url, username, password)

    try:
        response = requests.post(url, files=files, auth=auth)
    except requests.RequestException as e:
        click.echo(f"Error: failed to contact GraphDB at {graphdb_url}: {e}", err=True)
        sys.exit(1)

    if response.status_code in (200, 201, 204):
        click.echo(f"Repository '{repo_name}' created on {graphdb_url}")
    elif response.status_code == 409:
        click.echo(f"Repository '{repo_name}' already exists on {graphdb_url}", err=True)
        sys.exit(1)
    else:
        click.echo(
            f"Error: GraphDB responded with {response.status_code}:\n"
            f"{response.text}",
            err=True,
        )
        sys.exit(1)


@graphdb.command("add")
@click.option(
    "--repo",
    "repo_name",
    required=True,
    help="Target repository ID.",
)
@click.option(
    "--dir",
    "data_dir",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Directory containing RDF files.",
)
@click.option(
    "--suffix",
    default=".ttl",
    show_default=True,
    help="File suffix to match (e.g. '.ttl', '.owl').",
)
@click.option(
    "--recursive",
    is_flag=True,
    help="Recursively search subdirectories.",
)
@click.option(
    "--url",
    "graphdb_url",
    default=DEFAULT_GRAPHDB_URL,
    show_default=True,
    help="Base URL of GraphDB (without trailing slash).",
)
@click.option("--env", "env_file", default=None, help="Path to .env file (loads GRAPHDB_USERNAME/PASSWORD).")
@click.option("--username", default=None, help="GraphDB username (overrides env vars).")
@click.option("--password", default=None, help="GraphDB password (overrides env vars, prompts if missing).")
def graphdb_add(repo_name: str,
                data_dir: str,
                suffix: str,
                recursive: bool,
                graphdb_url: str,
                env_file: Optional[str],
                username: Optional[str],
                password: Optional[str]):
    """
    Add RDF files from directory to GraphDB repository.
    """
    base_url = graphdb_url.rstrip('/')
    statements_url = f"{base_url}/repositories/{repo_name}/statements"

    _load_env_file(env_file)

    # CLI args override env vars
    if username is None:
        username, password = _get_credentials()
    auth = _get_auth(graphdb_url, username, password)

    # Find all matching files
    pattern = f"**/*{suffix}" if recursive else f"*{suffix}"
    rdf_files: List[pathlib.Path] = list(
        pathlib.Path(data_dir).rglob(pattern) if recursive else pathlib.Path(data_dir).glob(pattern))

    if not rdf_files:
        click.echo(f"No files found matching '*{suffix}' in {data_dir}", err=True)
        sys.exit(1)

    click.echo(f"Found {len(rdf_files)} RDF files to upload to '{repo_name}'")

    headers = {
        'Content-Type': 'text/turtle',
        'Accept': 'application/json'
    }

    uploaded = 0
    for file_path in rdf_files:
        try:
            with open(file_path, 'rb') as f:
                response = requests.post(statements_url, headers=headers, data=f, auth=auth)

            if response.status_code in (200, 201, 204):
                click.echo(f"✓ {file_path}")
                uploaded += 1
            else:
                click.echo(f"✗ {file_path}: {response.status_code} - {response.text[:100]}...", err=True)

        except Exception as e:
            click.echo(f"✗ {file_path}: {e}", err=True)

    click.echo(f"\nUploaded {uploaded}/{len(rdf_files)} files successfully to {statements_url}")


def _load_env_file(env_file: Optional[str]):
    """Load .env file if provided."""
    if env_file and pathlib.Path(env_file).exists():
        dotenv.load_dotenv(env_file)
    elif env_file:
        click.echo(f"Warning: .env file not found: {env_file}", err=True)


def _get_credentials():
    """Get credentials from env vars with fallbacks."""
    username = os.getenv('GRAPHDB_USERNAME')
    password = os.getenv('GRAPHDB_PASSWORD')
    return username, password


def _get_auth(graphdb_url: str, username: Optional[str], password: Optional[str]):
    """Get requests.auth for GraphDB (Basic or token)."""
    if not username:
        return None

    if not password:
        password = click.prompt("GraphDB password", hide_input=True)

    # Try GraphDB token auth first
    token_auth = _get_graphdb_token(graphdb_url, username, password)
    if token_auth:
        return token_auth

    click.echo("Token auth failed, falling back to Basic Auth", err=True)
    return (username, password)


def _get_graphdb_token(base_url: str, username: str, password: str):
    """Get GraphDB JWT token via /rest/login."""
    login_url = f"{base_url.rstrip('/')}/rest/login"

    try:
        response = requests.post(
            login_url,
            json={"username": username, "password": password},
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code == 200:
            auth_header = response.headers.get('Authorization')
            if auth_header and auth_header.startswith('Bearer '):
                return lambda r: r.headers.update({'Authorization': auth_header})
    except:
        pass

    return None
