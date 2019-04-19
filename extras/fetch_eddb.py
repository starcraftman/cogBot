#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fetch the latest EDDB dump automatically.

This is used to pre-seed EDDB database. See cogdb.eddb
"""
from __future__ import absolute_import, print_function
import asyncio
import os
import subprocess
import sys

import aiofiles
import aiohttp


import cog.util


EDDB_URLS = [
    "https://eddb.io/archive/v6/commodities.json",
    "https://eddb.io/archive/v6/factions.json",
    # "https://eddb.io/archive/v6/listings.csv",
    "https://eddb.io/archive/v6/modules.json",
    "https://eddb.io/archive/v6/stations.json",
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
    try:
        folder = os.path.abspath(sys.argv[1])
    except IndexError:
        folder = cog.util.rel_to_abs(os.path.sep.join(['data', 'eddb']))

    try:
        confirm = sys.argv[2].strip().lower()
    except IndexError:
        confirm = input("Proceeding will overwrite {} with the latest dumps from eddb.\
\nProceed? (y/n) ".format(folder))
    confirm = confirm.strip().lower()

    if confirm == "sort":
        sort = True
    elif not confirm.startswith('y'):
        print("Aborting fetch.")
        return

    try:
        os.makedirs(folder)
    except OSError:
        pass

    jobs = [fetch(url, os.path.join(folder, os.path.basename(url)), sort) for url in EDDB_URLS]
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*jobs))
    print("All files updated in", folder)


if __name__ == "__main__":
    main()
