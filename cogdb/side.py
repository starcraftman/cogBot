"""
Sidewinder's remote database.
"""
from __future__ import absolute_import, print_function
import logging
import time

import sqlalchemy as sqla
import sqlalchemy.exc as sqla_exe
import sqlalchemy.ext.declarative

import cog.exc

LEN_FACTION = 64
LEN_STATION = 50
LEN_SYSTEM = 30
TIME_FMT = "%d/%m/%y %H:%M:%S"
SideBase = sqlalchemy.ext.declarative.declarative_base()


class Allegiance(SideBase):
    """ Represents the allegiance of a faction. """
    __tablename__ = "allegiance"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(11))

    def __repr__(self):
        keys = ['id', 'text']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Allegiance) and isinstance(other, Allegiance) and
                self.id == other.id)


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

    def __eq__(self, other):
        return isinstance(self, BGSTick) and isinstance(other, BGSTick) and self.day == other.day


class FactionState(SideBase):
    """ The state a faction is in. """
    __tablename__ = "faction_state"

    id = sqla.Column(sqla.Date, primary_key=True)  # Ignore not accurate
    text = sqla.Column(sqla.String(12))
    eddn = sqla.Column(sqla.String(12))

    def __repr__(self):
        keys = ['day', 'tick', 'unix_from', 'unix_to']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, FactionState) and isinstance(other, FactionState) and
                self.id == other.id)


class Faction(SideBase):
    """ Information about a faction. """
    __tablename__ = "factions"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_FACTION))
    state_id = sqla.Column(sqla.Integer)
    updated_at = sqla.Column(sqla.Integer)
    government_id = sqla.Column(sqla.Integer)
    allegiance_id = sqla.Column(sqla.Integer)
    home_system = sqla.Column(sqla.Integer)
    is_player_faction = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['id', 'name', 'state_id', 'government_id', 'allegiance_id', 'home_system',
                'is_player_faction', 'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(self, Faction) and isinstance(other, Faction) and self.id == other.id


class GovType(SideBase):
    """ All faction government types. """
    __tablename__ = "gov_type"

    id = sqla.Column(sqla.Date, primary_key=True)  # Ignore not accurate
    text = sqla.Column(sqla.String(13))
    eddn = sqla.Column(sqla.String(20))

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, GovType) and isinstance(other, GovType) and
                self.id == other.id)


class Influence(SideBase):
    """ Represents influence of a faction in a system. """
    __tablename__ = "influence"
    __table_args__ = (sqla.PrimaryKeyConstraint("system_id", "faction_id"),)

    system_id = sqla.Column(sqla.Integer)
    faction_id = sqla.Column(sqla.Integer)
    state_id = sqla.Column(sqla.Integer)
    influence = sqla.Column(sqla.Numeric(7, 4, None, False))
    is_controlling_faction = sqla.Column(sqla.Integer)
    updated_at = sqla.Column(sqla.Integer)
    pending_state_id = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['system_id', 'faction_id', 'state_id', 'pending_state_id', 'influence', 'is_controlling_faction',
                'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Influence) and isinstance(other, Influence) and
                self.system_id == other.system_id and self.faction_id == other.faction_id)


class Power(SideBase):
    """ Represents a powerplay leader. """
    __tablename__ = "powers"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(21))
    abbrev = sqla.Column(sqla.String(5))

    def __repr__(self):
        keys = ['id', 'text', 'abbrev']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Power) and isinstance(other, Power) and
                self.id == other.id)


class PowerState(SideBase):
    """ Represents the power state of a system (i.e. control, exploited). """
    __tablename__ = "power_state"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(10))

    def __repr__(self):
        keys = ['id', 'text']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, PowerState) and isinstance(other, PowerState) and
                self.id == other.id)


class Security(SideBase):
    """ Security states of a system. """
    __tablename__ = "security"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(8))
    eddn = sqla.Column(sqla.String(20))

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Security) and isinstance(other, Security) and
                self.id == other.id)


class SettlementSecurity(SideBase):
    """ The security of a settlement. """
    __tablename__ = "settlement_security"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(10))

    def __repr__(self):
        keys = ['id', 'text']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, SettlementSecurity) and isinstance(other, SettlementSecurity) and
                self.id == other.id)


class SettlementSize(SideBase):
    """ The size of a settlement. """
    __tablename__ = "settlement_size"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(3))

    def __repr__(self):
        keys = ['id', 'text']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, SettlementSize) and isinstance(other, SettlementSize) and
                self.id == other.id)


class StationType(SideBase):
    """ The type of a station. """
    __tablename__ = "station_type"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(20))

    def __repr__(self):
        keys = ['id', 'text']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, StationType) and isinstance(other, StationType) and
                self.id == other.id)


class Station(SideBase):
    """ Represents a station in a system. """
    __tablename__ = "stations"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_STATION))
    system_id = sqla.Column(sqla.Integer)
    updated_at = sqla.Column(sqla.Integer)
    distance_to_star = sqla.Column(sqla.Integer)
    station_type_id = sqla.Column(sqla.Integer)
    settlement_size_id = sqla.Column(sqla.Integer)
    settlement_security_id = sqla.Column(sqla.Integer)
    controlling_faction_id = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['id', 'name', 'distance_to_star', 'system_id', 'station_type_id',
                'settlement_size_id', 'settlement_security_id', 'controlling_faction_id',
                'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Station) and isinstance(other, Station) and
                self.id == other.id)


class System(SideBase):
    """ Repesents a system in the universe. """
    __tablename__ = "systems"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_SYSTEM))
    population = sqla.Column(sqla.Integer)
    income = sqla.Column(sqla.Integer)
    hudson_upkeep = sqla.Column(sqla.Integer)
    needs_permit = sqla.Column(sqla.Integer)
    update_factions = sqla.Column(sqla.Integer)
    power_id = sqla.Column(sqla.Integer)
    edsm_id = sqla.Column(sqla.Integer)
    security_id = sqla.Column(sqla.Integer)
    power_state_id = sqla.Column(sqla.Integer)
    controlling_faction_id = sqla.Column(sqla.Integer)
    control_system_id = sqla.Column(sqla.Integer)
    x = sqla.Column(sqla.Numeric(10, 5, None, False))
    y = sqla.Column(sqla.Numeric(10, 5, None, False))
    z = sqla.Column(sqla.Numeric(10, 5, None, False))
    dist_to_nanomam = sqla.Column(sqla.Numeric(7, 2, None, False))

    def __repr__(self):
        keys = ['id', 'name', 'population', 'income', 'hudson_upkeep',
                'needs_permit', 'update_factions', 'power_id', 'edsm_id',
                'security_id', 'power_state_id', 'controlling_faction_id',
                'control_system_id', 'x', 'y', 'z', 'dist_to_nanomam']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return isinstance(self, System) and isinstance(other, System) and self.id == other.id


class SystemAge(SideBase):
    """ Represents the age of eddn data received for control/system pair. """
    __tablename__ = "v_age"
    __table_args__ = (sqla.PrimaryKeyConstraint("control", "system"),)

    control = sqla.Column(sqla.String(LEN_SYSTEM))
    system = sqla.Column(sqla.String(LEN_SYSTEM))
    age = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['control', 'system', 'age']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, SystemAge) and isinstance(other, SystemAge) and
                self.control == other.control and self.system == other.system)


def next_bgs_tick(session, now):
    """
    Fetch the next expected bgs tick.

    Return:
        - If next tick available, return it.
        - If not, return message it isn't available.

    Raises:
        RemoteDBUnreachable - Cannot communicate with remote.
    """
    log = logging.getLogger("cogdb.side")
    try:
        result = session.query(BGSTick).filter(BGSTick.tick > now).order_by(BGSTick.tick).\
            limit(1).first()
        if result:
            log.info("BGS_TICK - %s -> %s", str(now), result.tick)
            bgs = "BGS Tick in **{}**    (Expected {})".format(result.tick - now, result.tick)
        else:
            log.warning("BGS_TICK - Remote out of estimates")
            raise cog.exc.NoMoreTargets("BGS Tick estimate unavailable. No more estimates, ask @Sidewinder40")
    except sqla_exe.OperationalError:
        raise cog.exc.RemoteDBUnreachable("Lost connection to Sidewinder's DB.")

    return bgs


def exploited_systems_by_age(session, control):
    """
    Return a list off all (possible empty) systems around the control
    that have outdated information.

    Raises:
        RemoteDBUnreachable - Cannot communicate with remote.
    """
    log = logging.getLogger("cogdb.side")
    try:
        result = session.query(SystemAge).filter(SystemAge.control == control).\
            order_by(SystemAge.system).all()

        log.info("BGS - Received from query: %s", str(result))
    except sqla_exe.OperationalError:
        raise cog.exc.RemoteDBUnreachable("Lost connection to Sidewinder's DB.")

    return result


def influence_in_system(session, system):
    """
    Query side's db for influence about factions in a given system.
    List will be empty if the system name does not match an existing.

    Returns a list of lists with the following:
        faction name, influence, is_player_faction, influence timestamp
    """
    subq = session.query(System.id).filter(System.name == system).subquery()
    infs = session.query(Faction.name, Influence.influence,
                         Faction.is_player_faction, Influence.updated_at).\
        filter(Influence.system_id == subq).filter(Faction.id == Influence.faction_id).\
        order_by(Influence.influence.desc()).all()

    return [[inf[0], float('{:.2f}'.format(inf[1])), bool(inf[2]),
             time.strftime(TIME_FMT, time.gmtime(inf[3]))] for inf in infs]
