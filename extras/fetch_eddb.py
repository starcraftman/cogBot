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

import cog.util


EDDB_URLS = [
    "https://eddb.io/archive/v5/commodities.json",
    "https://eddb.io/archive/v5/factions.json",
    "https://eddb.io/archive/v5/modules.json",
    "https://eddb.io/archive/v5/stations.json",
    "https://eddb.io/archive/v5/systems_populated.json",
]


# Directed Header: Accept-Encoding: gzip, deflate, sdch
async def fetch(url, fname):
    async with aiofiles.open(fname, "wb") as fout, aiohttp.ClientSession() as session:
        async with session.get(url, encoding={"Accept-Encoding": "gzip, deflate, sdch"}) as resp:
            await fout.write(await resp.read())


def main():
    try:
        folder = os.path.abspath(sys.argv[1])
    except IndexError:
        folder = cog.util.rel_to_abs(os.path.sep.join(['data', 'eddb']))

    try:
        os.makedirs(folder)
    except OSError:
        pass
    jobs = [fetch(url, os.path.join(folder, os.path.basename(url))) for url in EDDB_URLS]
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*jobs))


if __name__ == "__main__":
    main()
