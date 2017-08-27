"""
Sidewinder's remote database.
"""
from __future__ import absolute_import, print_function
import logging

import sqlalchemy as sqla
import sqlalchemy.ext.declarative


# bgsspy.bgs_tick
# +-----------+------------------+------+-----+---------+-------+
# | Field     | Type             | Null | Key | Default | Extra |
# +-----------+------------------+------+-----+---------+-------+
# | day       | date             | NO   | PRI | NULL    |       |
# | tick      | datetime         | YES  |     | NULL    |       |
# | unix_from | int(10) unsigned | YES  | MUL | NULL    |       |
# | unix_to   | int(10) unsigned | YES  | MUL | NULL    |       |
# +-----------+------------------+------+-----+---------+-------+
# bgsspy.v_age
# +---------+-------------+------+-----+---------+-------+
# | Field   | Type        | Null | Key | Default | Extra |
# +---------+-------------+------+-----+---------+-------+
# | Control | varchar(30) | NO   |     | NULL    |       |
# | System  | varchar(30) | NO   |     | NULL    |       |
# | Age     | int(7)      | YES  |     | NULL    |       |
# +---------+-------------+------+-----+---------+-------+
SideBase = sqlalchemy.ext.declarative.declarative_base()


class BGSTick(SideBase):
    """ Represents an upcoming BGS Tick (estimated). """
    __tablename__ = "bgs_tick"

    day = sqla.Column(sqla.Date, primary_key=True)  # Ignore not accurate
    tick = sqla.Column(sqla.DateTime)  # Actual expected tick
    unix_from = sqla.Column(sqla.Integer)
    unix_to = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['day', 'tick', 'unix_from', 'unix_to']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))


class SystemAge(SideBase):
    """ Represents the age of eddn data received for control/system pair. """
    __tablename__ = "v_age"
    __table_args__ = (sqla.PrimaryKeyConstraint("control", "system"),)

    control = sqla.Column(sqla.String(30))
    system = sqla.Column(sqla.String(30))
    age = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['control', 'system', 'age']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))


def next_bgs_tick(session, now):
    """
    Fetch the next expected bgs tick.

    Return:
        - If next tick available, return it.
        - If not, return message it isn't available.
    """
    log = logging.getLogger("cogdb.side")
    result = session.query(BGSTick).filter(BGSTick.tick > now).order_by(BGSTick.tick).\
        limit(1).first()

    if result:
        log.info("BGS_TICK - %s -> %s", str(now), result.tick)
        bgs = "BGS Tick in **{}**    (Expected {})".format(result.tick - now, result.tick)
    else:
        log.warning("BGS_TICK - Remote query returned nothing")
        bgs = "BGS Tick estimate unavailable. Try again later or ask Sidewinder40."

    return bgs


def exploited_systems_by_age(session, control):
    """
    Return a list off all (possible empty) systems around the control
    that have outdated information.
    """
    result = session.query(SystemAge).filter(SystemAge.control == control).\
        order_by(SystemAge.system).all()

    log = logging.getLogger("cogdb.side")
    log.info("BGS - Received from query: %s", str(result))

    return result
