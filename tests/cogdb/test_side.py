"""
Test remote queries to sidewinder's db.
"""
from __future__ import absolute_import, print_function
import datetime

from sqlalchemy.sql import text as sql_text

import cogdb.side
from cogdb.side import BGSTick, SystemAge


def test_bgstick__repr__(side_session):
    tick = BGSTick(day=datetime.date(2017, 5, 1), tick=datetime.datetime(2017, 5, 1, 16, 0), unix_from=1493654400, unix_to=1493740800)
    assert repr(tick) == "BGSTick(day=datetime.date(2017, 5, 1), tick=datetime.datetime(2017, 5, 1, 16, 0), unix_from=1493654400, unix_to=1493740800)"


def test_systemage__repr__(side_session):
    system = SystemAge(control='16 Cygni', system='16 Cygni', age=1)
    assert repr(system) == "SystemAge(control='16 Cygni', system='16 Cygni', age=1)"


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
