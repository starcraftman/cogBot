#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fetch the latest EDDB dump automatically.

This is used to pre-seed EDDB database. See cogdb.eddb
"""
import asyncio
import os
import subprocess
import sys

import aiofiles
import aiohttp


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


def pretty_json(fname):
    with open(fname + 'l', 'w') as fout:
        subprocess.run(['jq', '.', '-S', fname], stdout=fout)


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
            chunk = await resp.content.read(1000)
            while chunk:
                await fout.write(chunk)
                chunk = await resp.content.read(1000)

    print("Downloaded to", fname)

    if sort and fname.endswith(".json"):
        print("Using jq to pretty print:", fname)
        await asyncio.get_event_loop().run_in_executor(None, pretty_json, fname)
        print("Created pretty file", fname + 'l')


def main():
    sort = False
    eddb_d = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'data', 'eddb')

    try:
        confirm = sys.argv[1]
    except IndexError:
        confirm = input("Proceeding will overwrite {} with the latest dumps from eddb.\
\nProceed? (y/n) ".format(eddb_d))
    confirm = confirm.strip().lower()

    if confirm == "sort":
        sort = True
    elif not confirm.startswith('y'):
        print("Aborting fetch.")
        return

    try:
        os.makedirs(eddb_d)
    except OSError:
        pass

    jobs = [fetch(url, os.path.join(eddb_d, os.path.basename(url)), sort) for url in EDDB_URLS]
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*jobs))
    print("All files updated in", eddb_d)


if __name__ == "__main__":
    main()
