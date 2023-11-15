#!/usr/bin/env python
"""
Module to merge in tables to a new dump.
"""
import sqlalchemy.sql as sqla_sql

import cogdb


def main():
    """
    Given a series of tables embedded here merge in all tables from SRC to DEST database.
    """
    src_database = 'eddb_server'
    dst_database = 'eddb'
    tables = [
        'pvp_cmdrs',
        'pvp_inara_squads',
        'pvp_inaras',
        'pvp_logs',
        'pvp_match_players',
        'pvp_matches',
        'pvp_locations',
        'pvp_deaths',
        'pvp_deaths_killers',
        'pvp_interdicteds',
        'pvp_interdictions',
        'pvp_kills',
        'pvp_escaped_interdicteds',
        'pvp_interdicteds_deaths',
        'pvp_interdicteds_kills',
        'pvp_interdictions_deaths',
        'pvp_interdictions_kills',
    ]
    with cogdb.eddb_engine.connect() as con:
        for tbl in reversed(tables):
            con.execute(sqla_sql.text(f'delete from {dst_database}.{tbl};'))
    with cogdb.eddb_engine.connect() as con:
        for tbl in tables:
            con.execute(sqla_sql.text(f'insert into {dst_database}.{tbl} select * from {src_database}.{tbl};'))


if __name__ == "__main__":
    main()
