#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Fetch the latest EDDB dump automatically.

This is used to pre-seed EDDB database. See cogdb.eddb
"""
import sys
import cogdb.dbi_eddb


def main():
    """Just handle input and then process request."""
    print("This module deprecated.")
    return
    sort = True
    try:
        confirm = sys.argv[1]
    except IndexError:
        confirm = input("Proceeding will overwrite {} with the latest dumps from eddb.\
\nProceed? (y/n) ".format(cogdb.dbi_eddb.EDDB_D))

    confirm = confirm.strip().lower()
    if confirm == "sort":
        sort = True
    elif not confirm.startswith('n'):
        sort = False
    else:
        print("Aborting fetch.")
        return

    cogdb.dbi_eddb.fetch_all(sort=sort)


if __name__ == "__main__":
    main()
