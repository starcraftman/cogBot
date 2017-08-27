"""
Test remote queries to sidewinder's db.
"""
from __future__ import absolute_import, print_function
import datetime

from sqlalchemy.sql import text as sql_text

import cogdb.side


def test_next_bgs_tick(side_session):
    query = sql_text("SELECT tick FROM bgs_tick ORDER BY tick desc LIMIT 1")
    last_tick = side_session.execute(query).fetchone()[0]

    before_last = last_tick - datetime.timedelta(hours=4)
    msg = cogdb.side.next_bgs_tick(side_session, before_last)
    assert "BGS Tick in **4:00:00**" in msg

    after_last = last_tick + datetime.timedelta(hours=4)
    msg = cogdb.side.next_bgs_tick(side_session, after_last)
    assert "BGS Tick estimate unavailable. Try again later" in msg


def test_exploited_systems_by_age(side_session):
    query = sql_text("SELECT control, system FROM v_age ORDER BY system asc LIMIT 1")
    control, system = side_session.execute(query).fetchone()

    result = cogdb.side.exploited_systems_by_age(side_session, control)
    for row in result:
        assert row.control == control
    assert (result[0].control, result[0].system) == (control, system)
