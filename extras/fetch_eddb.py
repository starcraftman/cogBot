#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fetch the latest EDDB dump automatically.

This is used to pre-seed EDDB database. See cogdb.eddb
"""
import asyncio
import glob
import os
import sys

import aiofiles
import aiohttp


EDDB_D = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'data', 'eddb')
CHUNK_LIMIT = 10000
EDDB_URLS = [
    #  "https://eddb.io/archive/v6/attractions.json",  # Beacons, abandoned bases
    "https://eddb.io/archive/v6/commodities.json",
    "https://eddb.io/archive/v6/factions.json",
    # "https://eddb.io/archive/v6/listings.csv",  # Commodity pricing
    "https://eddb.io/archive/v6/modules.json",
    "https://eddb.io/archive/v6/stations.json",
    #  "https://eddb.io/archive/v6/systems.csv",  # All systems
    "https://eddb.io/archive/v6/systems_populated.json",
]


async def a_jq_post_process(fname):
    """
    Use jq command line to reprocess fname (a json) into ...
        - A pretty printed jsonl file for easy reading.
        - A ONE object per line file for parallel processing.
    """
    async with aiofiles.open(fname + 'l', "w") as fout_l:
        async with aiofiles.open(fname + '_per_line', "w") as fout_line:
            await asyncio.gather(
                asyncio.create_subprocess_shell(f'jq . -S {fname}', stdout=fout_l),
                asyncio.create_subprocess_shell(f'jq -c .[] {fname}', stdout=fout_line),
            )
    print("Created PRETTY file", fname + 'l')
    print("Created object PER LINE file", fname + '_per_line')


# Directed Header: Accept-Encoding: gzip, deflate, sdch
async def fetch(url, fname, sort=True):
    """
    Fetch a file and write it out in chunks to a file named.
    """
    print("Download started", url)

    headers = {"Accept-Encoding": "gzip, deflate, sdch"}
    async with aiofiles.open(fname, "wb") as fout,\
            aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as resp:
            chunk = await resp.content.read(CHUNK_LIMIT)
            while chunk:
                await fout.write(chunk)
                chunk = await resp.content.read(CHUNK_LIMIT)

    print("Downloaded to", fname)
    if sort and fname.endswith(".json"):
        await a_jq_post_process(fname)


def fetch_all(*, sort=True):
    """
    Synchronous function call that will update all eddb files to import.

    Args:
        sort: Use jq to sort the JSON files post download when True.
    """
    try:
        os.makedirs(EDDB_D)
    except OSError:
        pass

    # Cleanup files before writing
    to_remove = glob.glob(os.path.join(EDDB_D, '*.json'))
    if sort:
        to_remove += glob.glob(os.path.join(EDDB_D, '*.jsonl'))
        to_remove += glob.glob(os.path.join(EDDB_D, '*per_line'))
    for fname in to_remove:
        try:
            os.remove(fname)
        except OSError:
            print(f"Could not remove: {fname}")

    jobs = [fetch(url, os.path.join(EDDB_D, os.path.basename(url)), sort) for url in EDDB_URLS]
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*jobs))
    print("\n\nAll files updated in", EDDB_D)


def main():
    """Just handle input and then process request."""
    sort = True
    try:
        confirm = sys.argv[1]
    except IndexError:
        confirm = input("Proceeding will overwrite {} with the latest dumps from eddb.\
\nProceed? (y/n) ".format(EDDB_D))

    confirm = confirm.strip().lower()
    if confirm == "sort":
        sort = True
    elif not confirm.startswith('n'):
        sort = False
    else:
        print("Aborting fetch.")
        return

    fetch_all(sort=sort)


if __name__ == "__main__":
    main()
