#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fetch the latest EDDB dump automatically.
"""
from __future__ import absolute_import, print_function
import asyncio
import os
import sys

import aiofiles
import aiohttp
try:
    import simplejson as json
except ImportError:
    import json


import cog.util


EDDB_URLS = [
    "https://eddb.io/archive/v5/commodities.json",
    "https://eddb.io/archive/v5/factions.json",
    "https://eddb.io/archive/v5/modules.json",
    "https://eddb.io/archive/v5/stations.json",
    "https://eddb.io/archive/v5/systems_populated.json",
]


# Directed Header: Accept-Encoding: gzip, deflate, sdch
async def fetch(url, fname, sort=True):
    """
    Fetch a file and write it out in chunks to a file named.
    """
    print("Download started", url)

    async with aiofiles.open(fname, "wb") as fout, aiohttp.ClientSession() as session:
        async with session.get(url, encoding={"Accept-Encoding": "gzip, deflate, sdch"}) as resp:
            chunk = await resp.content.read(1000)
            while chunk:
                await fout.write(chunk)
                chunk = await resp.content.read(1000)

    print("Downloaded to", fname)

    if sort:
        async with aiofiles.open(fname, 'r') as fin:
            data = await fin.read()
        data = json.dumps(json.loads(data), indent=4, sort_keys=True)
        async with aiofiles.open(fname + 'l', "w") as fout:
            await fout.write(data)
        print("Creating sorted file", fname + 'l')


def main():
    try:
        folder = os.path.abspath(sys.argv[1])
    except IndexError:
        folder = cog.util.rel_to_abs(os.path.sep.join(['data', 'eddb']))

    resp = input("Proceeding will overwrite {} with the latest dumps from eddb.\
\nProceed? (y/n) ".format(folder))
    if not resp.lower().startswith('y'):
        print("Aborting fetch.")
        return

    try:
        os.makedirs(folder)
    except OSError:
        pass

    sort = True if len(sys.argv) == 3 else False
    jobs = [fetch(url, os.path.join(folder, os.path.basename(url)), sort) for url in EDDB_URLS]
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*jobs))

    print("All files updated in", folder)


if __name__ == "__main__":
    main()
