import logging
import pathlib
import shutil

import click

from opencefadb import __version__, paths
from opencefadb import configuration
from opencefadb import set_logging_level
# from opencefadb.core import connect_to_database
from opencefadb.query_templates.sparql import SELECT_FAN_PROPERTIES

logger = logging.getLogger("opencefadb")

_ASCII_ART = r"""
   ____                    _____     ______    _____  ____  
  / __ \                  / ____|   |  ____|  |  __ \|  _ \ 
 | |  | |_ __   ___ _ __ | |     ___| |__ __ _| |  | | |_) |
 | |  | | '_ \ / _ \ '_ \| |    / _ \  __/ _` | |  | |  _ < 
 | |__| | |_) |  __/ | | | |___|  __/ | | (_| | |__| | |_) |
  \____/| .__/ \___|_| |_|\_____\___|_|  \__,_|_____/|____/ 
        | |                                                 
        |_|                                                 
"""


@click.group(invoke_without_command=True)
@click.option('-V', '--version', is_flag=True, help='Show version')
@click.option('--log-level', help='Set the log level')
@click.pass_context
def cli(ctx, version, log_level):
    click.echo(_ASCII_ART)
    cfg = configuration.get_config()
    set_logging_level(cfg.logging_level)
    if log_level:
        logger.debug(f"Setting log level to {log_level}...")
        set_logging_level(log_level)
        logger.debug(f"Log level set to {logger.level}")
    if version:
        click.echo(f'opencefadb version {__version__}')
        return


# _cfg = configuration.get_config()
#
# _available_profiles = ', '.join(f"{section}" for section in _cfg._configparser.sections())


# @cli.command(help=f"Configure the database. The configuration file is located here: {paths['config']}")
# @click.option('--log-level', help='Set the log level')
# @click.option('--profile', help=f'Select the configuration profile. Available options: {_available_profiles}.')
# def config(log_level, profile):
#     click.echo(f"Configuration file: {pathlib.Path(paths['config']).resolve().absolute()}")
#     cfg = configuration.get_config()
#     if profile:
#         stp = configuration.get_setup()
#         logger.debug(f"Selecting profile {profile}...")
#         cfg.select_profile(profile)
#         stp.profile = profile
#         click.echo(f"Selected profile: {profile}")
#         click.echo(cfg)
#         return
#     if log_level:
#         from opencefadb import set_logging_level
#         logger.debug(f"Setting log level to {log_level}...")
#         cfg.logging_level = log_level
#         set_logging_level(log_level)
#         logger.debug(f"Log level set to {logger.level}")
#         return
#     click.echo(cfg)


def _initialize_database():
    stp = configuration.get_setup()
    cfg = configuration.get_config()
    cfg.select_profile(stp.profile)
    click.echo(f' > Selected profile: {stp.profile}')

    file_dir = pathlib.Path(cfg['DEFAULT']['rawdata_dir'])
    metadata_dir = pathlib.Path(cfg['DEFAULT']['metadata_dir'])

    file_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    logger.debug("Initialized...")
    logger.debug(f"Metadata directory: {metadata_dir}")
    logger.debug(f"File directory: {file_dir}")

    click.echo(" > Downloading all metadata from zenodo...")
    logger.debug("Downloading all metadata from zenodo...")
    from opencefadb.dbinit import initialize_database
    initialize_database(cfg.metadata_directory)


@cli.command(help="Initialize the database. This will download all metadata from Zenodo.")
def init():
    click.echo('Initializing database...')
    _initialize_database()
    click.echo("...done")


@cli.command()
@click.option('-y', is_flag=True, default=False, help="Answer yes to all questions")
@click.option('--init', is_flag=True, default=False, help="Automatically calls `opencefadb init` afterwards")
def reset(y, init):
    if y:
        reset_answer = True
    else:
        response = input("Resetting database... This deletes all downloaded files. Are you sure? [y/N]")
        reset_answer = response.lower() == 'y'

    if reset_answer:
        logger.debug("Resetting database...")
        cfg = configuration.get_config()
        metadata_dir = cfg.metadata_directory
        rawdata_dir = cfg.rawdata_directory
        if metadata_dir.exists():
            shutil.rmtree(metadata_dir)
        if rawdata_dir.exists():
            shutil.rmtree(rawdata_dir)

        cfg.delete()
        stp = configuration.get_setup()
        stp.delete()
        logger.debug("Done.")
        click.echo("Database reset! Call 'opencefadb init' to reinitialize the database")
        if init:
            click.echo("Calling 'opencefadb init'...")
            _initialize_database()

    else:
        click.echo('Aborted...')


@cli.command()
@click.option('--plot', is_flag=True, help='Plots the CAD. Requires special installation. See README.md')
@click.option('--name', required=False, default="asm", help='name of CAD (asm or fan)')
@click.option('--download', required=False, type=click.Path(), help='Download the CAD file(s)', show_default=True)
@click.option('-v', '--verbose', required=False, is_flag=True, help='Prints additional information')
@click.option('--print-properties', required=False, is_flag=True,
              help='Prints the Fan Properties to the screen. Requires the database to be initialized')
def fan(plot, name, download, verbose, print_properties):
    if download:
        db = connect_to_database()
        target_dir = pathlib.Path(download).resolve().absolute()
        click.echo(f"Downloading CAD file to '{target_dir}'...")
        filename = db.download_cad_file(target_dir=download)
        click.echo(f"...finished. File saved as {filename}")
    if plot:  # experimental!
        try:
            from opencefadb.cad import plotting
        except ImportError as e:
            click.echo(f"Error: {e}")
            return
        plotting.plot(name)

    if print_properties:
        logger.debug("Connecting to database...")
        db = connect_to_database()

        properties = db.select_fan_properties()
        if verbose:
            click.echo("Query:")
            click.echo("------")
            click.echo(SELECT_FAN_PROPERTIES.sparql_query)
            click.echo("")
        click.echo("Fan Properties:")
        click.echo("---------------")
        click.echo(properties)


@cli.command()
def info():
    cfg = configuration.get_config()
    click.echo(f"Configuration:")
    click.echo("--------------")
    click.echo(f"[{cfg.profile}]")
    for k in cfg[cfg.profile]:
        value = cfg[cfg.profile][k]
        click.echo(f" > {k}: {value}")
    click.echo("")


if __name__ == '__main__':
    cli()
