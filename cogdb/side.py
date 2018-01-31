"""
Sidewinder's remote database.
"""
from __future__ import absolute_import, print_function
import logging
import datetime
import math
import time

import sqlalchemy as sqla
import sqlalchemy.exc as sqla_exe
import sqlalchemy.orm as sqla_orm
import sqlalchemy.ext.declarative
from sqlalchemy import func as sqlfunc
from sqlalchemy.ext.hybrid import hybrid_method

import cog.exc
import cog.util


HUD_GOVS = ['128', '144']  # Patronage/Feudal gov are 128/144
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


class FactionHistory(SideBase):
    """ Historical information about a faction. """
    __tablename__ = "factions_history"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_FACTION))
    state_id = sqla.Column(sqla.Integer)
    updated_at = sqla.Column(sqla.Integer, primary_key=True)
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


class FactionState(SideBase):
    """ The state a faction is in. """
    __tablename__ = "faction_state"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(12))
    eddn = sqla.Column(sqla.String(12))

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, FactionState) and isinstance(other, FactionState) and
                self.id == other.id)


class Government(SideBase):
    """ All faction government types. """
    __tablename__ = "gov_type"

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(13))
    eddn = sqla.Column(sqla.String(20))

    def __repr__(self):
        keys = ['id', 'text', 'eddn']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Government) and isinstance(other, Government) and
                self.id == other.id)


class Influence(SideBase):
    """ Represents influence of a faction in a system. """
    __tablename__ = "influence"

    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    faction_id = sqla.Column(sqla.Integer, primary_key=True)
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

    @property
    def date(self):
        return datetime.datetime.fromtimestamp(self.updated_at)

    @property
    def short_date(self):
        return '{}/{}'.format(self.date.day, self.date.month)


class InfluenceHistory(SideBase):
    """ Represents influence of a faction in a system. """
    __tablename__ = "influence_history"

    system_id = sqla.Column(sqla.Integer, primary_key=True)
    faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'), primary_key=True)
    state_id = sqla.Column(sqla.Integer)
    influence = sqla.Column(sqla.Numeric(7, 4, None, False))
    is_controlling_faction = sqla.Column(sqla.Integer)
    updated_at = sqla.Column(sqla.Integer, primary_key=True)
    pending_state_id = sqla.Column(sqla.Integer)

    def __repr__(self):
        keys = ['system_id', 'faction_id', 'state_id', 'pending_state_id', 'influence', 'is_controlling_faction',
                'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Influence) and isinstance(other, Influence) and
                self.system_id == other.system_id and self.faction_id == other.faction_id)

    @property
    def date(self):
        return datetime.datetime.fromtimestamp(self.updated_at)

    @property
    def short_date(self):
        return '{}/{}'.format(self.date.day, self.date.month)


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
    """
    Represents the power state of a system (i.e. control, exploited).

    |  0 | None      |
    | 16 | Control   |
    | 32 | Exploited |
    | 48 | Contested
    """
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


class Station(SideBase):
    """ Represents a station in a system. """
    __tablename__ = "stations"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(LEN_STATION))
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'))
    updated_at = sqla.Column(sqla.Integer)
    distance_to_star = sqla.Column(sqla.Integer)
    station_type_id = sqla.Column(sqla.Integer, sqla.ForeignKey('station_type.id'))
    settlement_size_id = sqla.Column(sqla.Integer)
    settlement_security_id = sqla.Column(sqla.Integer)
    controlling_faction_id = sqla.Column(sqla.Integer, sqla.ForeignKey('factions.id'))

    def __repr__(self):
        keys = ['id', 'name', 'distance_to_star', 'system_id', 'station_type_id',
                'settlement_size_id', 'settlement_security_id', 'controlling_faction_id',
                'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __eq__(self, other):
        return (isinstance(self, Station) and isinstance(other, Station) and
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

    @property
    def log_pop(self):
        """ The log base 10 of the population. For terse representation. """
        return '{:.1f}'.format(math.log(self.population, 10))

    @hybrid_method
    def dist_to(self, other):
        """
        Compute the distance from this system to other.
        """
        dist = 0
        for let in ['x', 'y', 'z']:
            temp = getattr(other, let) - getattr(self, let)
            dist += temp * temp

        return math.sqrt(dist)

    @dist_to.expression
    def dist_to(self, other):
        """
        Compute the distance from this system to other.
        """
        return sqlfunc.sqrt((other.x - self.x) * (other.x - self.x) +
                            (other.y - self.y) * (other.y - self.y) +
                            (other.z - self.z) * (other.z - self.z))


class SystemAge(SideBase):
    """ Represents the age of eddn data received for control/system pair. """
    __tablename__ = "v_age"

    control = sqla.Column(sqla.String(LEN_SYSTEM), primary_key=True)
    system = sqla.Column(sqla.String(LEN_SYSTEM), primary_key=True)
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
        RemoteError - Cannot communicate with remote.
        NoMoreTargets - Ran out of ticks.
    """
    log = logging.getLogger("cogdb.side")
    try:
        result = session.query(BGSTick).filter(BGSTick.tick > now).order_by(BGSTick.tick).\
            limit(1).first()
        if result:
            log.info("BGS_TICK - %s -> %s", str(now), result.tick)
            return "BGS Tick in **{}**    (Expected {})".format(result.tick - now, result.tick)
        else:
            log.warning("BGS_TICK - Remote out of estimates")
            side = cog.util.BOT.get_member_by_substr("sidewinder40")
            raise cog.exc.NoMoreTargets("BGS Tick estimate unavailable. No more estimates, " + side.mention)
    except sqla_exe.OperationalError:
        raise cog.exc.RemoteError("Lost connection to Sidewinder's DB.")


def exploited_systems_by_age(session, control):
    """
    Return a list off all (possible empty) systems around the control
    that have outdated information.

    Raises:
        RemoteError - Cannot communicate with remote.
    """
    log = logging.getLogger("cogdb.side")
    try:
        result = session.query(SystemAge).filter(SystemAge.control == control).\
            order_by(SystemAge.system).all()

        log.info("BGS - Received from query: %s", str(result))
    except sqla_exe.OperationalError:
        raise cog.exc.RemoteError("Lost connection to Sidewinder's DB.")

    return result


def influence_in_system(session, system):
    """
    Query side's db for influence about factions in a given system.
    List will be empty if the system name does not match an existing.

    Returns a list of lists with the following:
        faction name, influence, is_player_faction, government_type, influence timestamp
    """
    subq = session.query(System.id).filter(System.name == system).subquery()
    infs = session.query(Faction.name, Influence.influence, Government.text,
                         Faction.is_player_faction, Influence.updated_at).\
        filter(Influence.system_id == subq).\
        filter(Faction.id == Influence.faction_id).\
        filter(Faction.government_id == Government.id).\
        order_by(Influence.influence.desc()).all()

    return [[inf[0], float('{:.2f}'.format(inf[1])), inf[2], 'Y' if inf[3] else 'N',
             time.strftime(TIME_FMT, time.gmtime(inf[4]))] for inf in infs]


def stations_in_system(session, system_id):
    """
    Query to find all stations in a system. Map them into a dictionary
    where keys are faction id that owns station. Values is a list of all stations.

    Returns: A dict of form: d[faction_id] = [stations]

    Raises:
        RemoteError - Cannot reach the remote host.
    """
    try:
        stations = session.query(Station.name, Faction.id, StationType.text).\
            filter(Station.system_id == system_id).\
            join(Faction).join(StationType).all()
        stations_dict = {}
        for tup in stations:
            try:
                station = tup[0] + station_suffix(tup[2])
                stations_dict[tup[1]] += [station]
            except KeyError:
                stations_dict[tup[1]] = [station]

        return stations_dict
    except sqla_exe.OperationalError:
        raise cog.exc.RemoteError("Lost connection to Sidewinder's DB.")


def influence_history_in_system(session, system_id, fact_ids, time_window=None):
    """
    Query for historical InfluenceHistory of factions with fact_ids in a system_id.

    Optionally specify a start_time, query will return all InfluenceHistory after that time.
    By default return 5 days worth of InfluenceHistory.

    Returns: A dict of form d[faction_id] = [InfluenceHistory, InfluenceHistory ...]

    Raises:
        RemoteError - Cannot reach the remote host.
    """
    try:
        if not time_window:
            time_window = time.time() - (60 * 60 * 24 * 5)

        inf_history = session.query(InfluenceHistory).\
            filter(InfluenceHistory.system_id == system_id).\
            filter(InfluenceHistory.faction_id.in_(fact_ids)).\
            filter(InfluenceHistory.updated_at >= time_window).\
            order_by(InfluenceHistory.faction_id,
                     InfluenceHistory.updated_at.desc()).all()

        inf_dict = {}
        for hist in inf_history:
            try:
                inf_dict[hist.faction_id] += [hist]
            except KeyError:
                inf_dict[hist.faction_id] = [hist]

        return inf_dict
    except sqla_exe.OperationalError:
        raise cog.exc.RemoteError("Lost connection to Sidewinder's DB.")


def station_suffix(station_type):
    """ Simple switch, map specific types on to single letter. """
    suffix = ' (No Dock)'
    if 'Planetary' in station_type and station_type != 'Planetary Settlement':
        suffix = ' (P)'
    elif 'Starport' in station_type:
        suffix = ' (L)'
    elif 'Asteroid' in station_type:
        suffix = ' (AB)'
    elif 'Outpost' in station_type:
        suffix = ' (M)'

    return suffix


def system_overview(session, system):
    """
    Provide a total BGS view of a system.

    Returns: If bad input, returns None, None.
        System: The remote System object.
        factions_info: A list of dicts for every faction with keys:
            'name', their name
            'player', True if PMF
            'state', current state
            'pending', next state
            'inf_history', list of influence over last 5 days
            'stations', list of stations owned, may be None

    Raises:
        RemoteError - Cannot reach the remote host.
    """
    try:
        system = session.query(System).filter(System.name == system).one()

        current = sqla_orm.aliased(FactionState)
        pending = sqla_orm.aliased(FactionState)
        factions = session.query(Faction.id, Faction.name, Faction.is_player_faction,
                                 current.text, pending.text, Government.text, Influence).\
            filter(Influence.system_id == system.id).\
            filter(Faction.id == Influence.faction_id).\
            filter(Faction.government_id == Government.id).\
            filter(Influence.state_id == current.id).\
            filter(Influence.pending_state_id == pending.id).\
            order_by(Influence.influence.desc()).all()

        stations_by_id = stations_in_system(session, system.id)
        inf_history = influence_history_in_system(session, system.id, [inf[0] for inf in factions])

        factions_hist = []
        for faction in factions:
            try:
                stations = stations_by_id[faction[0]]
            except KeyError:
                stations = None
            try:
                hist = inf_history[faction[0]][:5]
            except KeyError:
                hist = []

            prefix = faction[-2][:4] + ' | '
            if faction[0] == system.controlling_faction_id:
                prefix = '-> ' + prefix

            factions_hist.append({
                'name': prefix + faction[1],
                'player': faction[2],
                'state': faction[3],
                'pending': faction[4],
                'inf_history': [faction[-1]] + hist,
                'stations': stations,
            })

        return (system, factions_hist)
    except sqla_exe.OperationalError:
        raise cog.exc.RemoteError("Lost connection to Sidewinder's DB.")
    except sqla_orm.exc.NoResultFound:
        return (None, None)


def count_factions_in_systems(session, system_ids):
    """
    Count the number of factions in all systems that are in the system_ids list.
    """
    systems = session.query(Influence.faction_id, System.name).\
        filter(Influence.system_id.in_(system_ids)).\
        join(System).order_by(Influence.system_id).all()

    systems_fact_count = {}
    for _, system in systems:
        try:
            systems_fact_count[system] += 1
        except KeyError:
            systems_fact_count[system] = 1

    return systems_fact_count


def inf_history_for_pairs(session, data_pairs):
    """
    Find the influence history for all pairs of systems and factions provided in data pairs.
    That is data_pairs is: [[system_id, faction_id], [system_id, faction_id], ...]

    Returns:
        Oldest historical influence data for each system_id/faction_id pair. Of form:
    """
    look_for = [sqla.and_(InfluenceHistory.system_id == pair[0],
                          InfluenceHistory.faction_id == pair[1])
                for pair in data_pairs]

    time_window = time.time() - (60 * 60 * 24 * 5)
    inf_history = session.query(InfluenceHistory).\
        filter(sqla.or_(*look_for)).\
        filter(InfluenceHistory.updated_at >= time_window).\
        order_by(InfluenceHistory.system_id, InfluenceHistory.faction_id,
                 InfluenceHistory.updated_at.desc()).all()

    pair_hist = {}
    for hist in inf_history:
        key = "{}_{}".format(hist.system_id, hist.faction_id)
        pair_hist[key] = hist.influence

    return pair_hist


def dash_overview(session, control_system):
    """
    Provide a simple dashboard overview of a control and its exploiteds.

    Returns: List of (System, Faction, Government) for exploiteds + control system.
    """
    try:
        control = session.query(System).filter_by(name=control_system).one()
        factions = session.query(System, Faction, Government, Influence).\
            filter(sqla.and_(System.dist_to(control) <= 15, System.power_state_id != 48)).\
            filter(Faction.id == System.controlling_faction_id).\
            filter(Faction.government_id == Government.id).\
            filter(Influence.faction_id == Faction.id, Influence.system_id == System.id).\
            order_by(System.name).all()

        facts_in_system = count_factions_in_systems(session,
                                                    [faction[0].id for faction in factions])

        data_pairs = [[faction[0].id, faction[1].id] for faction in factions]
        hist_for_pairs = inf_history_for_pairs(session, data_pairs)

        net_change = {}
        for system, faction, inf in [[faction[0], faction[1], faction[3]] for faction in factions]:
            try:
                net_inf = inf.influence - hist_for_pairs["{}_{}".format(system.id, faction.id)]
            except KeyError:
                net_inf = inf.influence
            net_change[system.name] = '{}{:.1f}'.format('+' if net_inf >= 0 else '', net_inf)

        return (control, factions, net_change, facts_in_system)
    except sqla_exe.OperationalError:
        raise cog.exc.RemoteError("Lost connection to Sidewinder's DB.")


def find_factions_with_gov(session, system, dist, gov_ids):
    """
    Return all factions that are dist or less away from system and have government type
    in gov_ids.

    Returns:
        List of matches with:
            [[System, Influence, Faction, Government], ...]
    """
    return session.query(System, Influence, Faction, Government).\
        filter(sqla.and_(System.dist_to(system) <= dist,
                         Influence.system_id == System.id,
                         Faction.id == Influence.faction_id,
                         Faction.government_id == Government.id,
                         Government.id.in_(gov_ids))).\
        order_by(System.dist_to(system)).\
        all()


def find_favorable(session, centre_name, max_dist=None, inc=20):
    """
    Find favorable feudals or patronages around a centre_name system.

    Keep expanding search radius by inc ly on every try.
    If max_dist provided, search that area only.

    Returns:
        List of lists (first line header) of form:
            [['System Name', 'Govt', 'Dist', 'Inf', 'Faction Name'], ...]
    """
    try:
        centre = session.query(System).filter(System.name == centre_name).one()
    except sqla_orm.exc.NoResultFound:
        raise cog.exc.InvalidCommandArgs("System name was not found, must be exact.")

    dist = max_dist if max_dist else inc
    while True:
        matches = find_factions_with_gov(session, centre, dist, HUD_GOVS)

        if matches or max_dist:
            break
        dist += inc

    lines = [[
        sys.name[:16],
        gov.text[:4],
        "{:5.2f}".format(sys.dist_to(centre)),
        "{:5.2f}".format(inf.influence),
        faction.name
    ] for sys, inf, faction, gov in matches]

    return [['System Name', 'Govt', 'Dist', 'Inf', 'Faction Name']] + lines


def expansion_candidates(session, centre, faction):
    """
    Given a system and a faction determine all possible candidates to expand.

    A system is a candidate if it is < 20ly away and has < 7 factions.

    Returns:
        [[system_name, dictance, faction_count], ...]
    """
    matches = session.query(System.name, System.dist_to(centre), Influence.state_id, Faction.id).\
        filter(sqla.and_(System.dist_to(centre) <= 20,
                         System.name != centre.name,
                         Influence.system_id == System.id,
                         Faction.id == Influence.faction_id)).\
        order_by(System.dist_to(centre)).\
        all()

    sys_order = []
    systems = {}
    for tup in matches:
        try:
            if tup[0] not in sys_order:
                sys_order += [tup[0]]
            systems[tup[0]] += [tup[-1]]
        except KeyError:
            systems[tup[0]] = [tup[-1]]
            systems[tup[0] + 'dist'] = "{:5.2f}".format(tup[1])

    result = []
    for sys in sys_order:
        if len(systems[sys]) < 7 and faction.id not in systems[sys]:
            result += [[sys, systems[sys + 'dist'], len(systems[sys])]]

    return [["System", "Dist", "Faction Count"]] + result


def get_system(session, system_name):
    """
    Get the System that has system_name.

    Returns:
        Matching System or None if not found.
    """
    try:
        return session.query(System).filter(System.name == system_name).one()
    except sqla_orm.exc.NoResultFound:
        raise cog.exc.InvalidCommandArgs("Bad system name specified.")


def get_factions_in_system(session, system_name):
    """
    Get all Factions in the system with name system_name.

    Returns:
        List of Factions, empty if improper system_name.
    """
    return session.query(Faction).\
        filter(sqla.and_(System.name == system_name,
                         Influence.system_id == System.id,
                         Faction.id == Influence.faction_id)).\
        order_by(Faction.name).\
        all()


def expand_to_candidates(session, system_name):
    """
    Considering system_name, determine all controlling nearby factions that could expand to it.

    Returns:
        [[system_name, distance, influence, state, faction_name], ...]
    """
    centre = get_system(session, system_name)
    blacklist = [fact.id for fact in get_factions_in_system(session, system_name)]
    matches = session.query(System.name, System.dist_to(centre),
                            Influence.influence, Government.text, FactionState.text, Faction.name).\
        filter(sqla.and_(System.dist_to(centre) <= 20,
                         System.name != centre.name,
                         Influence.system_id == System.id,
                         Influence.is_controlling_faction,
                         Faction.id == Influence.faction_id,
                         Faction.id.notin_(blacklist),
                         Influence.state_id == FactionState.id,
                         Government.id == Faction.government_id)).\
        order_by(System.dist_to(centre)).\
        all()

    lines = [[
        sys_name[:16],
        "{:5.2f}".format(dist),
        "{:5.2f}".format(inf),
        gov[:4],
        state,
        fact_name
    ] for sys_name, dist, inf, gov, state, fact_name in matches]

    return [["System", "Dist", "Inf", "Gov", "State", "Faction"]] + lines


def compute_dists(session, system_names):
    """
    Given a list of systems, compute the distance from the first to all others.

    Returns:
        Dict of {system: distance, ...}

    Raises:
        InvalidCommandArgs - One or more system could not be matched.
    """
    systems = session.query(System).filter(System.name.in_(system_names)).all()

    if len(systems) != len(system_names):
        system_names = [name.lower() for name in system_names]
        for system in systems:
            system_names.remove(system.name.lower())

        msg = "Some systems were not found:\n" + "\n    " + "\n    ".join(system_names)
        raise cog.exc.InvalidCommandArgs(msg)

    centre = [sys for sys in systems if sys.name.lower() == system_names[0].lower()][0]
    rest = [sys for sys in systems if sys.name.lower() != system_names[0].lower()]
    return {system.name: centre.dist_to(system) for system in rest}


def main():
    pass


if __name__ == "__main__":
    main()
