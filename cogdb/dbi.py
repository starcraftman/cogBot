"""
The database import tool.
See documentation with -h.
The purpose of this tool is to seed the EDDB database then connect to EDDN for updates.

To seed a fresh dev machine with latest galaxy_stations.json
    python -m cogdb.dbi --fetch --eddb-maps --ids --caches --recreate-tables

To recreate the database and load data from existing dump, ids will not be altered.
    python -m cogdb.dbi --recreate-tables
"""
import argparse
import asyncio
import datetime
import gzip
import math
import os
import pathlib
import sys
from pathlib import Path

import requests
import tqdm

import cogdb.eddb
import cogdb.spansh
from cogdb.spansh import GALAXY_JSON, GALAXY_URL, GALAXY_COMPRESSION_RATE
from cog.util import print_no_newline
import pvp.schema
import extras.map_gen


def make_parser():
    """
    Make the parser for command line usage.

    Returns: An instance of argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(description="Spansh.co.uk Importer")
    parser.add_argument('--yes', '-y', action="store_true",
                        help='Skip confirmation.')
    parser.add_argument('--jobs', '-j', type=int, default=math.floor(os.cpu_count() * 1.5),
                        help='The number of jobs to run.')
    parser.add_argument('--fetch', '-f', action="store_true",
                        help='Fetch the latest spansh dumps.')
    parser.add_argument('--ids', dest="ids", action="store_true",
                        help='Update the ids map. Implied by --fetch as well.')
    parser.add_argument('--caches', '-c', action="store_true",
                        help='Regenerate all caches for commodities, modules and unique ID maps.')
    parser.add_argument('--commodities', dest='commodities', action="store_true",
                        help='When set, import station modules and commodities.')
    parser.add_argument('--recreate-tables', '-r', dest="recreate", action="store_true",
                        help='Recreate all EDDB tables, spy_tables and spansh specific tables.')
    parser.add_argument('--no-cleanup', dest='cleanup', action="store_false",
                        help='Do not cleanup scratch files in galaxy directory.')
    parser.add_argument('--skip', '-k', action="store_true",
                        help='Skip parsing and importing latest spansh dump.')
    parser.add_argument('--eddb-maps', action="store_true",
                        help='Initialize the fixed faction, system and station maps.')

    return parser


def download_with_progress(url, destination, *, description=None):  # pragma: no cover
    """
    Download a file with progress bar displayed in CLI.

    Args:
        url: The URL to download.
        destination: Where to put the downloaded file.
        description: Text to display during progress.

    Returns: The total size downloaded.
    """
    if not description:
        description = url

    with requests.get(url, timeout=60, stream=True) as resp:
        total_size_bytes = int(resp.headers.get('Content-Length', 1024 * 1024))
        chunk = 1024

        with tqdm.tqdm(desc=description, total=total_size_bytes, unit="iB", unit_scale=True, unit_divisor=chunk) as progress,\
             open(destination, 'wb') as fout:
            for data in resp.iter_content(chunk_size=chunk):
                progress.update(fout.write(data))

    return total_size_bytes


def extract_with_progress(source, destination, *, description=None, size=0):  # pragma: no cover
    """
    Extract a gzip compressed file with progress

    Args:
        url: The URL to download.
        destination: Where to put the downloaded file.
        description: Text to display during progress.
        size: The size of the file being extracted.

    Returns: destination
    """
    if not description:
        description = 'Extracting: ' + Path(source).name

    chunk = 1024
    with tqdm.tqdm(desc=description, unit="iB", total=size, unit_scale=True, unit_divisor=chunk) as progress,\
         gzip.open(source, 'rb') as fin,\
         open(destination, 'wb') as fout:

        while True:
            data = fin.read(chunk)
            if not data:
                break
            fout.write(data)
            progress.update(chunk)


def confirm_msg(args):
    """
    Create a message the explains what will happen based on args.

    Args:
        args: A instance of argparse.Namespace.

    Returns: A string to print to user.
    """
    msg = "The following steps will take place.\n\n"
    if args.fetch:
        msg += """    Preserve the current galaxy_stations.json to a backup
    Download and extract the latest spansh dump
"""
    if args.eddb_maps:
        msg += "    Reset the ID maps based on prior EDDB ID usage\n"

    if args.fetch or args.ids:
        msg += "    Update the ID maps for the dump\n"
    if args.caches:
        msg += "    Update the cached modules and commodity preload information.\n"

    if not args.skip:
        if args.recreate:
            msg += "    Recreate all EDDB, spy and spansh tables and preload data.\n"
        else:
            msg += "    Empty all EDDB, spy and spansh tables and preload data.\n"

        msg += """    Parse all the information present in current galaxy_stations.json
    Replace the following possibly existing EDDB tables with that information:
        cogdb.eddb.{System, Faction, Influence, Station, StationFeatures, StationEconomy, FactionActiveState}
"""
    if args.commodities:
        msg += """        cogdb.spansh.{SModuleSold, SCommodityPricing}
            Note: Module sale information takes about 1.5GB, commodity pricing is 3-4GB.
            Note: Will slow down parsing greatly, makes heavy use of the disk holding the dump file.
"""

    if not args.yes:
        msg += "\nPlease confirm with yes or no: "

    return msg


def fetch_galaxy_json():  # pragma: no cover, long test
    """
    Fetch and extract the latest dump from spansh.
    """
    galaxy_backup = GALAXY_JSON.replace('.json', '.json.bak')
    compressed_json = f"{GALAXY_JSON}.gz"
    if Path(GALAXY_JSON).exists():
        os.rename(GALAXY_JSON, galaxy_backup)

    try:
        size = download_with_progress(GALAXY_URL, compressed_json) * GALAXY_COMPRESSION_RATE
        extract_with_progress(compressed_json, GALAXY_JSON, size=size)
        os.remove(compressed_json)
    except KeyboardInterrupt:
        if Path(compressed_json).exists():
            os.remove(compressed_json)
        if Path(galaxy_backup).exists() and not Path(GALAXY_JSON).exists():
            os.rename(galaxy_backup, GALAXY_JSON)
        raise


def refresh_module_commodity_cache():  # pragma: no cover, depends on local GALAXY_JSON
    """
    Update the module and commodity caches.
    """
    print_no_newline("Regenerating the commodity and module caches ...")
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        if not eddb_session.query(cogdb.eddb.PowerState).all():
            cogdb.eddb.preload_tables(eddb_session)

        for cls in [cogdb.eddb.SCommodity, cogdb.eddb.SModule]:
            eddb_session.query(cls).delete()

        cogdb.spansh.generate_module_commodities_caches(eddb_session, GALAXY_JSON)

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        cogdb.eddb.preload_tables(eddb_session)

    print(" Done!")

    """
    Run a sanity check on the database.
    Upon return all tables should be made and preloaded with constant data.
    """
    cogdb.schema.Base.metadata.create_all(cogdb.engine)
    cogdb.eddb.Base.metadata.create_all(cogdb.eddb_engine)

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        cogdb.eddb.preload_tables(eddb_session)

    gb_needed = 16 if not os.path.exists(GALAXY_JSON) else 8
    stat = os.statvfs(Path(GALAXY_JSON).parent)
    if stat.f_bavail * stat.f_frsize < gb_needed * 1024 ** 3:
        location = pathlib.Path(GALAXY_JSON).parent
        print(f"Warning: This program uses scratch space roughly equal to dump size. Please free up 8GB or choose new location: {location}.")
        sys.exit(1)


def handle_confirmation(args):  # pragma: no cover, simple confirmation
    """
    Print a message outlining the changes to be made step by step.
    Then prompt for confirmation from the user, any response not beginning with y terminates program.

    Args:
        args: The argparse.Namespace object that was parsed.
    """
    if args.yes:
        print(confirm_msg(args))
    else:
        confirm = input(confirm_msg(args)).lstrip().lower()
        print()
        if not confirm.startswith('y'):
            print("Aborting.")
            sys.exit(0)


def clean_existing_tables(recreate=False):  # pragma: no cover
    """
    If requested recreate all existing tables, otherwise simply purge all data.

    Args:
        recreate: When True, drop and recreate tables. Otherwise just purge the data in the tables.
    """
    if recreate:
        print_no_newline("Recreating all EDDB tables ...")
        cogdb.eddb.recreate_tables()
        cogdb.spy_squirrel.recreate_tables()
        pvp.schema.recreate_tables()
    else:
        print_no_newline("Emptying existing EDDB tables ...")
        cogdb.eddb.empty_tables()
        pvp.schema.empty_tables()


def sanity_check():  # pragma: no cover, underlying functions tested elsewhere or difficult to
    """
    Run a sanity check on the database.
    Upon return all tables should be made and preloaded with constant data.
    """
    cogdb.schema.Base.metadata.create_all(cogdb.engine)
    cogdb.eddb.Base.metadata.create_all(cogdb.eddb_engine)

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        cogdb.eddb.preload_tables(eddb_session)

    gb_needed = 16 if not os.path.exists(GALAXY_JSON) else 8
    stat = os.statvfs(Path(GALAXY_JSON).parent)
    if stat.f_bavail * stat.f_frsize < gb_needed * 1024 ** 3:
        location = pathlib.Path(GALAXY_JSON).parent
        print(f"Warning: This program uses scratch space roughly equal to dump size. Please free up 8GB or choose new location: {location}.")
        sys.exit(1)


def main():  # pragma: no cover
    """ Main entry for dbi command. """
    sanity_check()
    start = datetime.datetime.utcnow()
    parser = make_parser()
    args = parser.parse_args()

    # Ensure that when on clean machine IDs regenerated properly
    for fname in (cogdb.spansh.SYSTEM_MAPF, cogdb.spansh.FACTION_MAPF, cogdb.spansh.STATION_MAPF):
        if not os.path.exists(fname):
            args.eddb_maps = True
            args.ids = True

    cogdb.spansh.PROCESS_COMMODITIES = args.commodities
    handle_confirmation(args)

    if args.fetch:
        fetch_galaxy_json()

    if args.eddb_maps:
        extras.map_gen.init_eddb_maps()

    if args.ids or args.fetch:
        print_no_newline("\nUpdating ID maps based on existing dump ...")
        unique_names = cogdb.spansh.collect_unique_names(GALAXY_JSON)
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            mapped = cogdb.spansh.eddb_maps(eddb_session)
            missing_names = cogdb.spansh.determine_missing_keys(*unique_names, mapped=mapped)
            cogdb.spansh.update_all_name_maps(*missing_names)
        print(" Done!")

    if args.caches:
        refresh_module_commodity_cache()

    if args.skip:
        return

    clean_existing_tables(args.recreate)

    print_no_newline(" Done!\nPreloading constant EDDB data ...")
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        cogdb.eddb.preload_tables(eddb_session)
    print(" Done!")

    try:
        asyncio.new_event_loop().run_until_complete(
            cogdb.spansh.parallel_process(
                GALAXY_JSON, jobs=args.jobs
            )
        )

        print("Updating the SystemControls table based on present information.")
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            cogdb.eddb.populate_system_controls(eddb_session)

        print("Time taken", datetime.datetime.utcnow() - start)
    finally:
        if args.cleanup:
            cogdb.spansh.cleanup_scratch_files(Path(GALAXY_JSON).parent)


if __name__ == "__main__":
    main()
