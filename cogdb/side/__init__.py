"""
Sidewinder's remote database.

These classes map to remote tables.
When querying from async code, await an executor to thread or process.
"""
import logging
import math
import time

import sqlalchemy as sqla
import sqlalchemy.exc as sqla_exe
import sqlalchemy.orm as sqla_orm
import sqlalchemy.ext.declarative
from sqlalchemy import func as sqlfunc
from sqlalchemy.sql import expression as sqlexp

import cogdb
import cogdb.eddb
from cogdb.eddb import LEN, TIME_FMT, HUDSON_BGS
from cogdb.side.allegiance import Allegiance
from cogdb.side.bgs_tick import BGSTick
from cogdb.side.faction import Faction, FactionState, FactionHistory
from cogdb.side.government import Government
from cogdb.side.influence import Influence, InfluenceHistory
from cogdb.side.power import Power, PowerState
from cogdb.side.security import Security, SettlementSecurity, SettlementSize
from cogdb.side.station import Station, StationType
from cogdb.side.system import System, SystemAge
import cog.exc
import cog.tbl
import cog.util


WATCH_BUBBLES = [
    "Abi",
    "Adeo",
    "Allowini",
    "Aornum",
    "Frey",
    "Gliese 868",
    "HR 2776",
    "Kaushpoos",
    "Mariyacoch",
    "Muncheim",
    "Nurundere",
    "Othime",
    "Phra Mool",
    "Shoujeman",
    "Wat Yu",
    "Wolf 906",
]
WATCH_FACTIONS = [
    "Civitas Dei",
    "EG Union",
    "Future of Udegoci",
]
PILOTS_FED_FACTION_ID = 76748  # N.B. 76748 is Useless pilots federation faction ID
# They are not useful for any faction related predictions/interactions.


# TODO: Determine why explicit joins now required? Change in library? For now fix.
def wrap_exceptions(func):
    """
    Wrap all top level queries that get used externally.
    Translate SQLAlchemy exceptions to internal ones.
    """
    def inner(*args, **kwargs):
        """ Simple inner function wrapper. """
        try:
            return func(*args, **kwargs)
        except sqla_exe.OperationalError as exc:
            raise cog.exc.RemoteError("Lost connection to Sidewinder's DB.") from exc

    return inner


@wrap_exceptions
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
    log = logging.getLogger(__name__)
    result = session.query(BGSTick).filter(BGSTick.tick > now).order_by(BGSTick.tick).\
        limit(1).first()

    if not result:
        log.warning("BGS_TICK - Remote out of estimates")
        side = cog.util.BOT.get_member_by_substr("sidewinder40")
        raise cog.exc.NoMoreTargets(f"BGS Tick estimate unavailable. No more estimates, {side.mention}")

    log.info("BGS_TICK - %s -> %s", str(now), result.tick)
    return f"BGS Tick in **{result.tick - now}**    (Expected {result.tick})"


@wrap_exceptions
def exploited_systems_by_age(session, control):
    """
    Return a list off all (possible empty) systems around the control
    that have outdated information.

    Raises:
        RemoteError - Cannot communicate with remote.
    """
    log = logging.getLogger(__name__)
    result = session.query(SystemAge).\
        filter(SystemAge.control == control).\
        order_by(SystemAge.system).\
        all()

    log.info("BGS - Received from query: %s", str(result))

    return result


@wrap_exceptions
def influence_in_system(session, system):
    """
    Query side's db for influence about factions in a given system.
    List will be empty if the system name does not match an existing.

    Returns a list of lists with the following:
        faction name, influence, is_player_faction, government_type, influence timestamp
    """
    subq = session.query(System.id).filter(System.name == system).scalar_subquery()
    infs = session.query(Influence.influence, Influence.updated_at,
                         Faction.name, Faction.is_player_faction, Government.text).\
        filter(Influence.system_id == subq,
               Faction.id != PILOTS_FED_FACTION_ID).\
        join(Faction, Influence.faction_id == Faction.id).\
        join(Government, Faction.government_id == Government.id).\
        order_by(Influence.influence.desc()).\
        all()

    return [[inf[2], float(f'{inf[0]:.2f}'), inf[4], 'Y' if inf[3] else 'N',
             time.strftime(TIME_FMT, time.gmtime(inf[1]))] for inf in infs]


@wrap_exceptions
def stations_in_system(session, system_id):
    """
    Query to find all stations in a system. Map them into a dictionary
    where keys are faction id that owns station. Values is a list of all stations.

    Returns: A dict of form: d[faction_id] = [stations]

    Raises:
        RemoteError - Cannot reach the remote host.
    """
    stations = session.query(Station.name, Faction.id, StationType.text).\
        filter(Station.system_id == system_id).\
        join(Faction, StationType).\
        all()
    stations_dict = {fact_id: [] for _, fact_id, _ in stations}
    for name, fact_id, station_type in stations:
        try:
            station = f"{name}{station_suffix(station_type)}"
            stations_dict[fact_id] += [station]
        except KeyError:
            stations_dict[fact_id] = [station]

    return stations_dict


@wrap_exceptions
def influence_history_in_system(session, system_id, fact_ids, time_window=None):
    """
    Query for historical InfluenceHistory of factions with fact_ids in a system_id.

    Optionally specify a start_time, query will return all InfluenceHistory after that time.
    By default return 5 days worth of InfluenceHistory.

    Returns: A dict of form d[faction_id] = [InfluenceHistory, InfluenceHistory ...]

    Raises:
        RemoteError - Cannot reach the remote host.
    """
    if not time_window:
        time_window = time.time() - (60 * 60 * 24 * 5)

    inf_history = session.query(InfluenceHistory).\
        filter(InfluenceHistory.system_id == system_id,
               InfluenceHistory.faction_id.in_(fact_ids),
               InfluenceHistory.updated_at >= time_window).\
        order_by(InfluenceHistory.faction_id,
                 InfluenceHistory.updated_at.desc()).\
        all()

    inf_dict = {}
    for hist in inf_history:
        try:
            if inf_dict[hist.faction_id][-1].short_date != hist.short_date:
                inf_dict[hist.faction_id] += [hist]
        except KeyError:
            inf_dict[hist.faction_id] = [hist]

    return inf_dict


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
    elif 'Carrier' in station_type:
        suffix = ' (C)'

    return suffix


@wrap_exceptions
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
            filter(Influence.system_id == system.id,
                   Faction.id == Influence.faction_id,
                   Faction.id != PILOTS_FED_FACTION_ID,
                   Faction.government_id == Government.id,
                   Influence.state_id == current.id,
                   Influence.pending_state_id == pending.id).\
            order_by(Influence.influence.desc()).\
            all()

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

            try:
                if faction[-1].short_date == hist[0].short_date:
                    hist = hist[1:]
            except IndexError:
                pass

            factions_hist.append({
                'name': prefix + faction[1],
                'player': faction[2],
                'state': faction[3],
                'pending': faction[4],
                'inf_history': [faction[-1]] + hist,
                'stations': stations,
            })

        return (system, factions_hist)
    except sqla_orm.exc.NoResultFound:
        return (None, None)


def count_factions_in_systems(session, system_ids):
    """
    Count the number of factions in all systems that are in the system_ids list.
    """
    systems = session.query(System.name, sqlfunc.count(Influence.faction_id)).\
        filter(Influence.system_id.in_(system_ids)).\
        join(System).order_by(Influence.system_id).\
        group_by(System.name).\
        all()

    return dict(systems)


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

    #  time_window = time.time() - (60 * 60 * 24 * 5)
    inf_history = session.query(sqlfunc.concat(sqlexp.cast(InfluenceHistory.system_id, sqla.types.Unicode), "_", sqlexp.cast(InfluenceHistory.faction_id, sqla.types.Unicode)), InfluenceHistory.influence).\
        filter(sqla.or_(*look_for)).\
        group_by(InfluenceHistory.system_id, InfluenceHistory.faction_id).\
        order_by(InfluenceHistory.updated_at.asc()).\
        limit(100).\
        all()

    return dict(inf_history)


@wrap_exceptions
def dash_overview(session, control_system):
    """
    Provide a simple dashboard overview of a control and its exploiteds.

    Returns: List of (System, Faction, Government) for exploiteds + control system.
    """
    control = session.query(System).filter(System.name == control_system).one()
    factions = session.query(System, Faction, Government, Influence, SystemAge.age).\
        filter(System.dist_to(control) <= 15,
               System.power_state_id != 48).\
        outerjoin(SystemAge, SystemAge.system == System.name).\
        join(Influence, System.id == Influence.system_id).\
        filter(Influence.is_controlling_faction == 1).\
        join(Faction, Influence.faction_id == Faction.id).\
        join(Government, Faction.government_id == Government.id).\
        order_by(System.name).\
        limit(100).\
        all()

    facts_in_system = count_factions_in_systems(session,
                                                [faction[0].id for faction in factions])
    data_pairs = [[faction[0].id, faction[1].id] for faction in factions]
    hist_for_pairs = inf_history_for_pairs(session, data_pairs)

    net_change = {}
    for system, faction, inf in [[faction[0], faction[1], faction[3]] for faction in factions]:
        try:
            net_inf = inf.influence - hist_for_pairs[f"{system.id}_{faction.id}"]
        except KeyError:
            net_inf = inf.influence
        net_inf_prefix = '+' if net_inf >= 0 else ''
        net_change[system.name] = f'{net_inf_prefix}{net_inf:.1f}'

    return (control, factions, net_change, facts_in_system)


@wrap_exceptions
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
    except sqla_orm.exc.NoResultFound as exc:
        raise cog.exc.InvalidCommandArgs("System name was not found, must be exact.") from exc

    dist = max_dist if max_dist else inc
    keep_looking = True
    subq = session.query(Government.id).\
        filter(Government.text.in_(HUDSON_BGS[0])).\
        scalar_subquery()

    while keep_looking:
        matches = session.query(System.name, System.dist_to(centre),
                                Influence.influence, Faction.name, Government.text).\
            join(Influence, Influence.system_id == System.id).\
            join(Faction, Influence.faction_id == Faction.id).\
            join(Government, Faction.government_id == Government.id).\
            filter(System.dist_to(centre) <= dist,
                   Government.id.in_(subq)).\
            order_by(System.dist_to(centre)).\
            limit(100).\
            all()

        if matches or max_dist:
            keep_looking = False
        dist += inc

    lines = []
    for sys_name, sys_dist, inf, faction, gov in matches:
        lines += [[
            sys_name[:16],
            gov[:4],
            f"{sys_dist:5.2f}",
            f"{inf:5.2f}",
            faction,
        ]]

    return [['System Name', 'Govt', 'Dist', 'Inf', 'Faction Name']] + lines


# FIXME: This doesn't seem to work as intended, faction arg unused
@wrap_exceptions
def expansion_candidates(session, centre, faction):
    """
    Given a system and a faction determine all possible candidates to expand.

    A system is a candidate if it is < 20ly away and has < 7 factions.

    Returns:
        [[system_name, dictance, faction_count], ...]
    """
    dist_centre = System.dist_to(centre)
    matches = session.query(System.name, dist_centre, sqlfunc.count(Faction.id)).\
        join(Influence, Influence.system_id == System.id).\
        join(Faction, Influence.faction_id == Faction.id).\
        filter(System.id != centre.id,
               dist_centre <= 20,
               Faction.id != PILOTS_FED_FACTION_ID).\
        group_by(System).\
        order_by(dist_centre).\
        all()

    result = [["System", "Dist", "Faction Count"]]
    result += [[name, f"{dist:5.2f}", cnt] for name, dist, cnt in matches if cnt < 7]
    return result


@wrap_exceptions
def get_factions_in_system(session, system_name):
    """
    Get all Factions in the system with name system_name.

    Returns:
        List of Factions, empty if improper system_name.
    """
    return session.query(Faction).\
        filter(System.name == system_name,
               Influence.system_id == System.id,
               Faction.id == Influence.faction_id,
               Faction.id != PILOTS_FED_FACTION_ID).\
        order_by(Faction.name).\
        all()


@wrap_exceptions
def expand_to_candidates(session, system_name):
    """
    Considering system_name, determine all controlling nearby factions that could expand to it.

    Returns:
        [[system_name, distance, influence, state, faction_name], ...]
    """

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        centre = cogdb.eddb.get_systems(eddb_session, [system_name])[0]
        blacklist = session.query(Faction.id).\
            join(Influence, Influence.faction_id == Faction.id).\
            join(System, Influence.system_id == System.id).\
            filter(System.name == system_name,
                   Influence.system_id == System.id,
                   Faction.id == Influence.faction_id).\
            scalar_subquery()
        matches = session.query(System.name, System.dist_to(centre), System.population, Influence,
                                Faction, FactionState.text, Government.text).\
            join(Influence, Influence.system_id == System.id).\
            join(Faction, Influence.faction_id == Faction.id).\
            join(FactionState, Faction.state_id == FactionState.id).\
            join(Government, Faction.government_id == Government.id).\
            filter(System.dist_to(centre) <= 20,
                   System.name != centre.name,
                   Influence.is_controlling_faction == 1,
                   Faction.id.notin_(blacklist)).\
            order_by(System.dist_to(centre)).\
            all()

        lines = [["System", "Dist", "Pop", "Inf", "Gov", "State", "Faction"]]
        for sys_name, sys_dist, sys_pop, inf, fact, fact_state, gov in matches:
            lines += [[
                sys_name[:16],
                f"{sys_dist:5.2f}",
                f"{math.log(sys_pop, 10):3.1f}",
                f"{inf.influence:5.2f}",
                gov[:4],
                fact_state,
                fact.name
            ]]

    return lines


def get_monitor_systems(session, controls):
    """
    Get all uncontested systems within the range of mentioned controls.
    Always include all systems with EG Union.

    Returns: List of system_ids
    """
    eg_systems = session.query(System.id).\
        join(Influence, Influence.system_id == System.id).\
        join(Faction, Influence.faction_id == Faction.id).\
        filter(Faction.name == "EG Union").\
        order_by(System.id).\
        all()

    # Generate orable filter criteria for systems in the bubbles of controls
    pstates = session.query(PowerState.id).\
        filter(PowerState.text.in_(["Exploited", "Control"])).\
        scalar_subquery()
    look_for = [
        System.dist_to(sqla_orm.aliased(System,
                                        session.query(System).filter(System.name == control).subquery())) <= 15
        for control in controls
    ]
    uncontesteds = session.query(System.id).\
        filter(System.power_state_id.in_(pstates),
               sqla.or_(*look_for)).\
        order_by(System.id).\
        all()

    return sorted(list({x[0] for x in eg_systems + uncontesteds}))


@wrap_exceptions
def monitor_events(session, system_ids):
    """
    Monitor a number of systems by given ids for special events within them.

    Returns: A list of messages to send.
    """
    monitor_states = session.query(FactionState.id).\
        filter(FactionState.text.in_(["Election", "War", "Civil War", "Expansion", "Retreat"])).\
        scalar_subquery()
    control_state_id = session.query(PowerState.id).\
        filter(PowerState.text == "Control").\
        scalar_subquery()

    current = sqla_orm.aliased(FactionState)
    pending = sqla_orm.aliased(FactionState)
    sys = sqla_orm.aliased(System)
    sys_control = sqla_orm.aliased(System)
    events = session.query(Influence.influence, sys.name, Faction.name, Government.text,
                           current.text, pending.text,
                           sqla.func.ifnull(sys_control.name, 'N/A').label('control')).\
        filter(Influence.system_id.in_(system_ids),
               sqla.or_(Influence.state_id.in_(monitor_states),
                        Influence.pending_state_id.in_(monitor_states))).\
        join(sys, Influence.system_id == sys.id).\
        join(Faction, Influence.faction_id == Faction.id).\
        join(Government, Faction.government_id == Government.id).\
        join(current, Influence.state_id == current.id).\
        join(pending, Influence.pending_state_id == pending.id).\
        outerjoin(
            sys_control,
            sqla.and_(
                sys_control.power_state_id == control_state_id,
                sys_control.dist_to(sys) < 15
            )).\
        order_by('control', sys.name, current.text, pending.text).\
        limit(1000).\
        all()

    wars = [["Control", "System", "Faction", "Gov", "Inf", "Current", "Pending"]]
    expansions = wars[:]
    retreats = wars[:]
    elections = wars[:]

    for event in events:
        states = [event[-3], event[-2]]
        line = [[event[-1][:20], event[1][:20], event[2][:20], event[3][:3],
                 f"{round(event[0], 2):5.2f}", event[-3], event[-2]]]

        if "Election" in states:
            elections += line

        if "War" in states:
            wars += line

        if "Expansion" in states:
            expansions += line

        if "Retreat" in states:
            retreats += line

    header = "**__Events in Monitored Systems__**\n\n**Elections**\n"
    msgs = cog.tbl.format_table(elections, header=True, prefix=header)
    msgs += cog.tbl.format_table(wars, header=True, prefix="\n\n**Wars**\n")
    msgs += cog.tbl.format_table(expansions, header=True, prefix="\n\n**Expansions**\n")
    msgs += cog.tbl.format_table(retreats, header=True, prefix="\n\n**Retreats**\n")

    return cog.util.merge_msgs_to_least(msgs)


@wrap_exceptions
def control_dictators(session, system_ids):
    """
    Show newly controlling dictators in the last 7 days.
    Show all controlling dictators in the monitored systems.

    Returns: A list of messages to send.
    """
    gov_dic = session.query(Government.id).\
        filter(Government.text.in_(["Anarchy", "Dictatorship"])).\
        scalar_subquery()
    control_state_id = session.query(PowerState.id).\
        filter(PowerState.text == "Control").\
        scalar_subquery()

    current = sqla_orm.aliased(FactionState)
    pending = sqla_orm.aliased(FactionState)
    sys = sqla_orm.aliased(System)
    sys_control = sqla_orm.aliased(System)
    dics = session.query(Influence, sys.name, Faction.name, Government.text,
                         current.text, pending.text,
                         sqla.func.ifnull(sys_control.name, 'N/A').label('control')).\
        join(sys, Influence.system_id == sys.id).\
        join(Faction, Influence.faction_id == Faction.id).\
        join(Government, Faction.government_id == Government.id).\
        join(current, Influence.state_id == current.id).\
        join(pending, Influence.pending_state_id == pending.id).\
        outerjoin(
            sys_control,
            sqla.and_(
                sys_control.power_state_id == control_state_id,
                sys_control.dist_to(sys) < 15
            )).\
        filter(Influence.system_id.in_(system_ids),
               Government.id.in_(gov_dic)).\
        order_by('control', sys.name).\
        all()

    # Get influence history for last 7 days of requested pairs
    look_for = [sqla.and_(InfluenceHistory.system_id == pair[0].system_id,
                          InfluenceHistory.faction_id == pair[0].faction_id)
                for pair in dics]
    time_window = time.time() - (60 * 60 * 24 * 7)
    inf_history = session.query(sqlfunc.concat(sqlexp.cast(InfluenceHistory.system_id, sqla.types.Unicode),
                                               "_",
                                               sqlexp.cast(InfluenceHistory.faction_id, sqla.types.Unicode)),
                                InfluenceHistory.is_controlling_faction).\
        filter(InfluenceHistory.updated_at >= time_window,
               sqla.or_(*look_for)).\
        order_by(InfluenceHistory.system_id, InfluenceHistory.faction_id).\
        all()

    # Group is_controlling_faction by system_faction
    pair_hist = {x[0]: set() for x in inf_history}
    for hist in inf_history:
        pair_hist[hist[0]].add(hist[1])

    lines = [["Control", "System", "Faction", "Gov", "Inf", "State", "Pending State"]]
    for dic in dics:
        key = f"{dic[0].system_id}_{dic[0].faction_id}"
        try:
            if len(pair_hist[key]) == 2:  # 2 entries in last 7 days means changed control
                lines += [[dic[-1], dic[1][:16], dic[2][:32], dic[3],
                           "{round(dic[0].influence, 2):5.2f}", dic[-3], dic[-2]]]
        except KeyError:
            pass

    con_lines = [["Control", "System", "Faction", "Gov", "Inf", "State", "Pending State"]]
    for dic in [x for x in dics if x[0].is_controlling_faction]:
        con_lines += [[dic[-3], dic[1][:16], dic[2][:32], dic[3],
                       f"{round(dic[0].influence, 2):5.2f}", dic[-2], dic[-1]]]

    msgs = cog.tbl.format_table(lines, header=True, prefix="**\n\nControlling Anarchies/Dictators Change** (last 7 days)\n")
    msgs += cog.tbl.format_table(con_lines, header=True, prefix="\n\n**Current Controlling Anarchies/Dictators**\n")

    return msgs


@wrap_exceptions
def moving_dictators(session, system_ids):
    """
    Show newly controlling dictators in the last 5 days.
    Show all controlling dictators in monitored systems.

    Subqueries galore, you've been warned.

    Returns: A list of messages to send.
    """
    gov_dic = session.query(Government.id).\
        filter(Government.text.in_(["Anarchy", "Dictatorship"])).\
        scalar_subquery()
    control_state_id = session.query(PowerState.id).\
        filter(PowerState.text == "Control").\
        scalar_subquery()

    current = sqla_orm.aliased(FactionState)
    pending = sqla_orm.aliased(FactionState)
    sys = sqla_orm.aliased(System)
    sys_control = sqla_orm.aliased(System)
    dics = session.query(Influence, sys.name, Faction.name, Government.text,
                         current.text, pending.text,
                         sqla.func.ifnull(sys_control.name, 'N/A').label('control')).\
        join(sys, Influence.system_id == sys.id).\
        join(Faction, Influence.faction_id == Faction.id).\
        join(Government, Faction.government_id == Government.id).\
        join(current, Influence.state_id == current.id).\
        join(pending, Influence.pending_state_id == pending.id).\
        outerjoin(
            sys_control,
            sqla.and_(
                sys_control.power_state_id == control_state_id,
                sys_control.dist_to(sys) < 15
            )).\
        filter(Influence.system_id.in_(system_ids),
               Government.id.in_(gov_dic)).\
        order_by('control', sys.name).\
        all()

    look_for = [sqla.and_(InfluenceHistory.system_id == inf[0].system_id,
                          InfluenceHistory.faction_id == inf[0].faction_id)
                for inf in dics]
    time_window = time.time() - (60 * 60 * 24 * 2)
    inf_history = session.query(InfluenceHistory).\
        filter(sqla.or_(*look_for)).\
        filter(InfluenceHistory.updated_at >= time_window).\
        order_by(InfluenceHistory.system_id, InfluenceHistory.faction_id,
                 InfluenceHistory.updated_at.desc()).\
        all()

    pair_hist = {}
    for hist in inf_history:
        key = f"{hist.system_id}_{hist.faction_id}"
        pair_hist[key] = hist

    lines = [["Control", "System", "Faction", "Gov", "Date",
              "Inf", "Inf (2 days ago)", "State", "Pending State"]]
    for dic in dics:
        key = f"{dic[0].system_id}_{dic[0].faction_id}"
        try:
            lines += [[dic[-1], dic[1][:16], dic[2][:16], dic[3][:3],
                       dic[0].short_date, f"{round(dic[0].influence, 2):5.2f}",
                       f"{round(pair_hist[key].influence, 2):5.2f}", dic[-3], dic[-2]]]
        except KeyError:
            lines += [[dic[-1], dic[1][:16], dic[2][:16], dic[3][:3],
                       dic[0].short_date, f"{round(dic[0].influence, 2):5.2f}", "N/A",
                       dic[-3], dic[-2]]]

    prefix = "**\n\nInf Movement Anarchies/Dictators**)\n"
    prefix += "N/A: Means no previous information, either newly expanded to system or not tracking.\n"
    return cog.tbl.format_table(lines, header=True, prefix=prefix)


@wrap_exceptions
def monitor_factions(session, faction_names=None):
    """
    Get all information on the provided factions. By default use set list.

    Returns: A list of messages to send.
    """
    current = sqla_orm.aliased(FactionState)
    pending = sqla_orm.aliased(FactionState)
    sys = sqla_orm.aliased(System)
    sys_control = sqla_orm.aliased(System)
    if not faction_names:
        faction_names = WATCH_FACTIONS
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        faction_ids = [x[0] for x in eddb_session.query(cogdb.eddb.Faction.id).
                       filter(cogdb.eddb.Faction.name.in_(faction_names)).
                       all()]

    control_state_id = session.query(PowerState.id).\
        filter(PowerState.text == "Control").\
        scalar_subquery()
    matches = session.query(Influence.influence, sys.name, Faction.name,
                            Government.text, current.text, pending.text,
                            sqla.func.ifnull(sys_control.name, 'N/A')).\
        filter(Influence.faction_id.in_(faction_ids)).\
        join(sys, Influence.system_id == sys.id).\
        join(Faction, Influence.faction_id == Faction.id).\
        join(Government, Faction.government_id == Government.id).\
        join(current, Influence.state_id == current.id).\
        join(pending, Influence.pending_state_id == pending.id).\
        outerjoin(sys_control, sqla.and_(
            sys_control.power_state_id == control_state_id,
            sys_control.dist_to(sys) < 15
        )).\
        limit(1000).\
        all()

    lines = [["Control", "System", "Faction", "Gov", "Inf",
              "State", "Pending State"]]
    for match in matches:
        lines += [[match[-1], match[1][:16], match[2][:16], match[3][:3],
                   f"{round(match[0], 2):5.2f}", match[-3], match[-2]]]

    return cog.tbl.format_table(lines, header=True, prefix="\n\n**Monitored Factions**\n")


@wrap_exceptions
def get_system_ages(session, controls, cutoff=1):
    """
    Get the list of Systems that have stale EDDN data.

    Returns: A dictionary of form:
        {'control': [system, system], ...}
    """
    ages = session.query(SystemAge).\
        filter(SystemAge.control.in_(controls)).\
        having(SystemAge.age >= cutoff).\
        all()

    map_ages = {x: [] for x in controls}
    for age in ages:
        map_ages[age.control] += [age]

    return map_ages


def service_status(side_session):
    """
    Poll sidewinder remote and return useful information based on state.
    Used for dashboard summary.

    Args:
        side_session: A session onto Sidewinder's db.

    Returns: A list of cells to format into a 2 column wide table.
    """
    try:
        estimate = side_session.query(BGSTick).order_by(BGSTick.unix_from.desc()).limit(1).one()
        cells = [
            ['Sidewinder DB', 'Up'],
            ['Last estimated tick', f'{estimate.tick}'],
        ]
    except sqla_exe.OperationalError:
        cells = [
            ['Sidewinder DB', 'Down'],
            ['Last estimated tick', 'Unknown'],
        ]

    return cells


def main():  # pragma: no cover
    """ Main function to test against side. """
    #  with cogdb.session_scope(cogdb.SideSession) as side_session:
    # system_ids = get_monitor_systems(side_session, WATCH_BUBBLES)
    # print(monitor_dictators(side_session, ["Othime", "Frey"]))
    # print(moving_dictators(side_session, system_ids))
    # print(get_monitor_factions(side_session))


if __name__ == "__main__":  # pragma: no cover
    main()
