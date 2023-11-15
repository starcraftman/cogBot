"""
Top level integration of all EDDB related SQLAlchemy objects.

Note there may be duplication between here and side.py.
The latter is purely a mapping of sidewinder's remote.
This module is for internal use, the duplication allows divergence.

N.B. Don't put subqueries in FROM of views for now, doesn't work on test docker.
"""
import asyncio
import datetime
import enum
import math
import string
import sys

# Selected backend set in ijson.backend as string.
import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.orm.session
import sqlalchemy.sql as sqla_sql
import sqlalchemy.ext.declarative
from sqlalchemy.sql.expression import or_

import cogdb
import cogdb.common
from cogdb.eddb.common import Base, LEN
from cogdb.eddb.allegiance import Allegiance
from cogdb.eddb.commodity_eddb import Commodity, CommodityCat
from cogdb.eddb.commodity_spansh import SCommodity, SCommodityGroup, SCommodityPricing
from cogdb.eddb.conflict import Conflict, ConflictState, EVENT_CONFLICTS
from cogdb.eddb.economy import Economy
from cogdb.eddb.faction import (Faction, FactionHappiness, FactionState,
                                FactionActiveState, FactionPendingState, FactionRecoveringState)
from cogdb.eddb.government import Government
from cogdb.eddb.influence import EVENT_HISTORY_INFLUENCE, Influence, HistoryTrack, HistoryInfluence
from cogdb.eddb.module_eddb import Module, ModuleGroup
from cogdb.eddb.module_spansh import SModule, SModuleSold, SModuleGroup
from cogdb.eddb.power import Power, PowerState
from cogdb.eddb.security import Security, SettlementSecurity, SettlementSize
from cogdb.eddb.ship import Ship, ShipSold
from cogdb.eddb.station import CarrierSighting, Station, StationEconomy, StationFeatures, StationType
from cogdb.eddb.system import (
    System, SystemControl, SystemControlV, SystemContestedV, VIEW_CONTESTEDS, VIEW_SYSTEM_CONTROLS
)

import cog.exc
import cog.tbl
import cog.util


TIME_FMT = "%d/%m/%y %H:%M:%S"
# These are the faction types strong/weak verse.
HUDSON_BGS = [['Feudal', 'Patronage'], ["Dictatorship"]]
WINTERS_BGS = [["Corporate"], ["Communism", "Cooperative", "Feudal", "Patronage"]]
CONTROL_DISTANCE = 15  # A control exploits all systems in this distance
HISTORY_INF_LIMIT = 40
HOUR_SECONDS = 60 * 60
HISTORY_INF_TIME_GAP = HOUR_SECONDS * 4  # min seconds between data points
DEFAULT_DIST = 75
DEFAULT_ARRIVAL = 5000
# To select planetary stations
TABLES_TO_PRELOAD = [
    Allegiance,
    CommodityCat,
    SCommodityGroup,
    SCommodity,
    ConflictState,
    Economy,
    FactionHappiness,
    FactionState,
    Government,
    ModuleGroup,
    SModuleGroup,
    SModule,
    Power,
    PowerState,
    Security,
    SettlementSecurity,
    SettlementSize,
    Ship,
    StationType,
]


class TraderType(enum.Enum):
    """
    Types of traders to filter for get_nearest_station_economies.
    """
    BROKERS_GUARDIAN = 1
    BROKERS_HUMAN = 2
    MATS_DATA = 3
    MATS_RAW = 4
    MATS_MANUFACTURED = 5


def preload_system_aliases(session):
    """
    Preload several shortcut names for systems either unpopulated or long.
    Bit of a hack, mainly need their x, y, z. IDs are put at the back, likely never hit.

    Args:
        session: A session onto the EDDB db.
    """
    # https://eddb.io/attraction/73536  | Dav's Hope
    # https://eddb.io/attraction/73512  | Jameson Crash Site
    # https://eddb.io/attraction/73522  | Anaconda Shipwreck
    session.query(System).\
        filter(System.id.in_([99999990, 99999991, 99999992])).\
        delete()
    session.add_all([
        System(
            id=99999990,
            name='Davs',
            x=-104.625,
            y=-0.8125,
            z=-151.90625,
            updated_at=1,
        ), System(
            id=99999991,
            name='Cobra',
            x=-101.90625,
            y=-95.46875,
            z=-165.59375,
            updated_at=1,
        ), System(
            id=99999992,
            name='Anaconda',
            x=68.84375,
            y=48.75,
            z=76.75,
            updated_at=1,
        )
    ])


# PowerState notes
#   HomeSystem and Prepared are EDDN only states.
#   HomeSystem is redundant as I map Power.home_system
#   Prepared can be tracked shouldn't overlap
#   InPreparedRadius is from Spansh
# StationType: Alias: 'Bernal' -> 'Ocellus'
def preload_tables(eddb_session):
    """
    Preload all constant data into the core EDDB tables.
    These tables are based on older EDDB.io data.
    Upon return, data for each table is guaranteed to be loaded.

    Deprecation Note:
        Commodity, CommodityCat, Module and ModuleGroup
        are all deprecated in favour of
        cogdb.spansh.{SCommodity, SCommodityGroup, SModule, SModuleGroup}

    Args:
        eddb_session: A session onto the EDDB.
    """
    for cls in TABLES_TO_PRELOAD:
        cogdb.common.preload_table_from_file(eddb_session, cls=cls)

    preload_system_aliases(eddb_session)
    eddb_session.commit()


def get_power_by_name(session, partial=''):
    """
    Given part of a name of a power in game, resolve the Power of the database and return it.

    Args:
        session: A session onto the db.
        partial: Part of a name, could be abbreviation or part of the name. So long as it resolves to one power valid.

    Returns: A Power who was matched. Alternatively if no match found, return None.
    """
    try:
        return session.query(Power).\
            filter(Power.abbrev == partial).\
            one()
    except sqla_orm.exc.NoResultFound:
        pass

    try:
        # If not modified to be loose, modify both sides
        if partial[0] != '%' and partial[-1] != '%':
            partial = f'%{partial}%'
        return session.query(Power).\
            filter(Power.text.ilike(partial)).\
            one()
    except sqla_orm.exc.NoResultFound as exc:
        powers = session.query(Power).\
            filter(Power.id != 0).\
            all()
        msg = "To match a power, use any of these abreviations:\n\n"\
            + '\n'.join([f"{pow.abbrev:6} => {pow.text}" for pow in powers])
        raise cog.exc.InvalidCommandArgs(msg) from exc

    return None


def get_systems(session, system_names):
    """
    Given a list of names, find all exact matching systems.

    Returns:
        [System, System, ...]

    Raises:
        InvalidCommandArgs - One or more systems didn't match.
    """
    systems = session.query(System).\
        filter(System.name.in_(system_names)).\
        all()

    if len(systems) != len(system_names):
        for system in systems:
            system_names = [s_name for s_name in system_names
                            if s_name.lower() != system.name.lower()]
        msg = "Could not find the following system(s):"
        msg += "\n\n" + "\n".join(system_names)
        raise cog.exc.InvalidCommandArgs(msg)

    return systems


def get_systems_around(session, centre_name, distance=CONTROL_DISTANCE):
    """
    Given a central system and a distance, return all systems around it.
    Includes the centre system.

    Returns:
        [System, System, ...]

    Raises:
        centre_name was not found then exception.
    """
    centre = session.query(System).filter(System.name == centre_name).one()
    return session.query(System).\
        filter(System.dist_to(centre) <= distance).\
        order_by(System.name).\
        all()


def get_influences_by_id(eddb_session, influence_ids):
    """
    Get a list of Influence objects by their ids.

    Args:
        eddb_session: A session onto the db.
        influence_ids: A list of id numbers.

    Returns: A list of Influence objects found for ids.
    """
    return eddb_session.query(Influence).\
        join(System, Influence.system_id == System.id).\
        filter(Influence.id.in_(influence_ids)).\
        order_by(System.name, Influence.influence.desc()).\
        all()


def base_get_stations(session, centre_name, *, sys_dist=DEFAULT_DIST, arrival=DEFAULT_ARRIVAL):
    """A base query to get all stations around a centre_name System.
    Applies base criteria filters for max system distance from centre and arrival distance in system.

    Given a reference centre system, find nearby orbitals within:
        < sys_dist ly from original system
        < arrival ls from the entry start

    Importantly station_system(System), Station, StationType and StationFeatures are available to further
    filter the query.

    Returns:
        A partially completed query based on above, has no extra filters.
    """
    exclude = session.query(StationType.text).\
        filter(
            or_(
                StationType.text.like("%Planet%"),
                StationType.text.like("%Fleet%"))).\
        scalar_subquery()

    centre = sqla_orm.aliased(System)
    station_system = sqla_orm.aliased(System)
    stations = session.query(station_system.name, station_system.dist_to(centre).label('dist_c'),
                             Station.name, Station.distance_to_star, Station.max_landing_pad_size).\
        select_from(station_system).\
        join(centre, centre.name == centre_name).\
        join(Station, Station.system_id == station_system.id).\
        join(StationType, Station.type_id == StationType.id).\
        join(StationFeatures, Station.id == StationFeatures.id).\
        filter(
            station_system.dist_to(centre) < sys_dist,
            Station.distance_to_star < arrival,
            StationType.text.notin_(exclude))

    return stations


def get_nearest_stations_with_features(session, centre_name, *, features=None, sys_dist=DEFAULT_DIST, arrival=DEFAULT_ARRIVAL, include_medium=False):
    """Find the nearest stations with the required features present.
    Features is a list of strings that are possible features on StationFeatures.
    Returned results will have stations with ALL requested features.

    Returns:
        List of matches:
            [system_name, system_dist, station_name, station_arrival_distance]
    """
    stations = base_get_stations(session, centre_name, sys_dist=sys_dist, arrival=arrival)

    if not features:
        features = []
    for feature in features:
        stations = stations.filter(getattr(StationFeatures, feature))

    pads = ['M', 'L'] if include_medium else ['L']
    stations = stations.filter(Station.max_landing_pad_size.in_(pads))

    stations = stations.order_by('dist_c', Station.distance_to_star).\
        limit(20).\
        all()

    return [[a, round(b, 2), f"[{e}] {cog.util.shorten_text(c, 16)}", d]
            for [a, b, c, d, e] in stations]


def get_nearest_traders(session, centre_name, *,
                        trader_type=TraderType.BROKERS_GUARDIAN, sys_dist=DEFAULT_DIST, arrival=DEFAULT_ARRIVAL):
    """Find the nearest stations that sell broker equipment or trade mats.
    The determination is made based on station economy and features.

    Args:
        session: A session onto the db.
        centre_name: Search for stations close to this system.
        trader_type: A type in TraderType enum, indicating desired trader.
        sys_dist: The max distance away from centre_name to search.
        arrival: The max distance from arrival in system to allow.

    Returns:
        List of matches:
            [system_name, system_dist, station_name, station_arrival_distance]
    """
    exclude = session.query(StationType.text).\
        filter(
            or_(
                StationType.text.like("%Planet%"),
                StationType.text.like("%Fleet%"))).\
        scalar_subquery()
    high_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'High Tech').\
        scalar_subquery()
    mil_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'Military').\
        scalar_subquery()
    ind_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'Industrial').\
        scalar_subquery()
    ref_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'Refinery').\
        scalar_subquery()
    ext_econ_id = session.query(Economy.id).\
        filter(Economy.text == 'Extraction').\
        scalar_subquery()

    centre = sqla_orm.aliased(System)
    station_system = sqla_orm.aliased(System)
    stations = session.query(station_system.name, station_system.dist_to(centre).label('dist_c'),
                             Station.name, Station.distance_to_star, Station.max_landing_pad_size).\
        select_from(Station).\
        join(station_system, Station.system_id == station_system.id).\
        join(StationType, Station.type_id == StationType.id).\
        join(StationFeatures, Station.id == StationFeatures.id).\
        join(centre, centre.name == centre_name).\
        filter(
            station_system.population > 1000000,
            station_system.population < 22000000,
            station_system.dist_to(centre) < sys_dist,
            Station.distance_to_star < arrival,
            StationType.text.notin_(exclude))

    if trader_type == TraderType.BROKERS_GUARDIAN:
        stations = stations.filter(StationFeatures.techBroker,
                                   station_system.primary_economy_id == high_econ_id)
    elif trader_type == TraderType.BROKERS_HUMAN:
        stations = stations.filter(StationFeatures.techBroker,
                                   station_system.primary_economy_id != high_econ_id)
    elif trader_type == TraderType.MATS_DATA:
        stations = stations.filter(
            StationFeatures.materialtrader,
            or_(
                station_system.primary_economy_id == high_econ_id,
                station_system.primary_economy_id == mil_econ_id,
                station_system.secondary_economy_id == high_econ_id,
                station_system.secondary_economy_id == mil_econ_id,
            )
        )
    elif trader_type == TraderType.MATS_RAW:
        stations = stations.filter(
            StationFeatures.materialtrader,
            or_(
                station_system.primary_economy_id == ref_econ_id,
                station_system.primary_economy_id == ext_econ_id,
            )
        )
    elif trader_type == TraderType.MATS_MANUFACTURED:
        stations = stations.filter(
            StationFeatures.materialtrader,
            or_(
                station_system.primary_economy_id == ind_econ_id,
                station_system.secondary_economy_id == ind_econ_id,
            )
        )

    stations = stations.order_by('dist_c', Station.distance_to_star).\
        limit(20).\
        all()

    return [[a, round(b, 2), f"[{e}] {cog.util.shorten_text(c, 16)}", d]
            for [a, b, c, d, e] in stations]


def get_shipyard_stations(session, centre_name, *, sys_dist=DEFAULT_DIST, arrival=DEFAULT_ARRIVAL, include_medium=False):
    """Get the nearest shipyard stations.
    Filter stations to find those with shipyards, these stations have all repair features.
    If mediums requested, filter to find all repair required features: repair, rearm, feuel, outfitting

    Returns:
        List of matches:
            [system_name, system_dist, station_name, station_arrival_distance]
    """
    return get_nearest_stations_with_features(
        session, centre_name, features=['outfitting', 'rearm', 'refuel', 'repair'],
        sys_dist=sys_dist, arrival=arrival, include_medium=include_medium
    )


def nearest_system(centre, systems):
    """
    Given a centre system, choose next nearest in systems.

    Returns:
        [dist_to_centre, System]
    """
    best = [centre.dist_to(systems[0]), systems[0]]
    for system in systems[1:]:
        dist = centre.dist_to(system)
        if dist < best[0]:
            best = [dist, system]

    return best


def find_route(session, start, systems):
    """
    Given a starting system, construct the best route by always selecting the next nearest system.

    Returns:
        [total_distance, [Systems]]
    """
    if not isinstance(start, System):
        start = get_systems(session, [start])[0]
    if not isinstance(systems[0], System):
        systems = get_systems(session, systems)

    course = [start]
    total_dist = 0

    while systems:
        choice = nearest_system(start, systems)
        course += [choice[1]]
        total_dist += choice[0]
        systems.remove(choice[1])
        start = choice[1]

    return [total_dist, course]


def find_best_route(session, systems):
    """
    Find the best route through systems provided by name or System.
    Apply the N nearest algorithm across all possible starting candidates.
    Then select the one with least total distance.

    Returns:
        [total_distance, [System, System, ...]]
    """
    if isinstance(systems[0], System):
        if not isinstance(systems[0], type('')):
            systems = [sys.name for sys in systems]
        systems = get_systems(session, systems)

    best = []
    for start in systems:
        systems_copy = systems[:]
        systems_copy.remove(start)
        result = find_route(session, start, systems_copy)
        if not best or result[0] < best[0]:
            best = result

    return best


def find_route_closest_hq(session, systems):
    """
    Given a set of system names in eddb:
        - Find the system that is closest to the HQ.
        - Route the remaining systems to minimize jump distance starting at closest to HQ.

    Args:
        session: Session onto the db.
        systems: A list of system names to route.

    Returns:
        [total_distance, [Systems]]
    """
    start = get_system_closest_to_HQ(session, systems)
    systems = [x for x in systems if x.lower() != start.name.lower()]

    return find_route(session, start, systems)


def get_nearest_controls(session, *, centre_name='sol', power='%hudson', limit=3):
    """
    Find nearest control systems of a particular power.

    Args:
        session: The EDDBSession variable.

    Kwargs:
        centre_name: The central system to find closest powers to.
        power: The power you are looking for.
        limit: The number of nearest controls to return, default 3.
    """
    centre = session.query(System).filter(System.name == centre_name).one()
    results = session.query(System).\
        join(Power, System.power_id == Power.id).\
        join(PowerState, System.power_state_id == PowerState.id).\
        filter(PowerState.text == 'Control',
               Power.text.ilike(power)).\
        order_by(System.dist_to(centre))

    if limit > 0:
        results = results.limit(limit)

    return results.all()


def get_controls_of_power(session, *, power='%hudson'):
    """
    Find the names of all controls of a given power.

    Args:
        session: The EDDBSession variable.

    Kwargs:
        power: The loose like match of the power, i.e. "%hudson".
    """
    return [
        x[0] for x in session.query(System.name).
        join(Power, System.power_id == Power.id).
        join(PowerState, System.power_state_id == PowerState.id).
        filter(PowerState.text == 'Control',
               Power.text.ilike(power)).
        order_by(System.name)
    ]


def get_systems_of_power(session, *, power='%hudson'):
    """
    Find the names of all exploited and contested systems of a power.

    Args:
        session: The EDDBSession variable.

    Kwargs:
        power: The loose like match of the power, i.e. "%hudson".
    """
    return [
        x[0] for x in session.query(System.name).
        join(Power, System.power_id == Power.id).
        filter(Power.text.ilike(power)).
        order_by(System.name)
    ]


def is_system_of_power(session, system_name, *, power='%hudson'):
    """
    Returns True if a system is under control of a given power.

    Args:
        session: The EDDBSession variable.
        system: The name of the system.

    Kwargs:
        power: The loose like match of the power, i.e. "%hudson".

    Returns: True if system is owned by power.
    """
    return session.query(SystemControlV.control).\
        filter(
            SystemControlV.power.ilike(power),
            sqla.or_(
                SystemControlV.system == system_name,
                SystemControlV.control == system_name
            )).\
        all()


def compute_dists(session, system_names):
    """
    Given a list of systems, compute the distance from the first to all others.

    Returns:
        Dict of {system: distance, ...}

    Raises:
        InvalidCommandArgs - One or more system could not be matched.
    """
    system_names = [name.lower() for name in system_names]
    try:
        centre = session.query(System).filter(System.name.ilike(system_names[0])).one()
    except sqla_orm.exc.NoResultFound as exc:
        raise cog.exc.InvalidCommandArgs(f"The start system {system_names[0]} was not found.") from exc
    systems = session.query(System.name, System.dist_to(centre)).\
        filter(System.name.in_(system_names[1:])).\
        order_by(System.name).\
        all()

    if len(systems) != len(system_names[1:]):
        for system in systems:
            system_names.remove(system[0].lower())

        msg = "Some systems were not found:\n%s" % "\n    " + "\n    ".join(system_names)
        raise cog.exc.InvalidCommandArgs(msg)

    return systems


def bgs_funcs(system_name):
    """
    Generate strong and weak functions to check gov_type text.

    Returns:
        strong(gov_type), weak(gov_type)
    """
    bgs = HUDSON_BGS
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        if is_system_of_power(eddb_session, system_name, power='%winters'):
            bgs = WINTERS_BGS

    def strong(gov_type):
        """ Strong vs these governments. """
        return gov_type in bgs[0]

    def weak(gov_type):
        """ Weak vs these governments. """
        return gov_type in bgs[1]

    return strong, weak


def get_power_hq(substr):
    """
    Loose match substr against keys in powers full names.

    Returns:
        [Full name of power, their HQ system name]

    Raises:
        InvalidCommandArgs - Unable to identify power from substring.
    """
    matches = [key for key in HQS if substr in key]
    if len(matches) != 1:
        msg = "Power must be substring of the following:"
        msg += "\n  " + "\n  ".join(sorted(HQS.keys()))
        raise cog.exc.InvalidCommandArgs(msg)

    return [string.capwords(matches[0]), HQS[matches[0]]]


def get_system_closest_to_HQ(session, systems, *, power='hudson'):
    """
    Return the system closest to the HQ of the given power.

    Args:
        session; A session onto the db.
        systems: A list of names of systems.

    Kwargs:
        power: A substring of the power we want to find closest system to.

    Returns: A System object of the closest system to the power's HQ.

    Raises:
        InvalidCommandArgs - A bad name of power was given.
    """
    _, hq_system = get_power_hq(power)
    subq_hq_system = session.query(System).\
        filter(System.name == hq_system).\
        one()

    return session.query(System).\
        filter(System.name.in_(systems)).\
        order_by(System.dist_to(subq_hq_system)).\
        limit(1).\
        one()


def get_closest_station_by_government(session, system, gov_type, *, limit=10):
    """Find the closest pirson megaship to designated system.

    Results will be a list of tuples of [(Station, System), ...] ordered by distance to system starting at.

    Args:
        session: A session onto the db.
        system: The system to find prison megaships near.
        limit: The number of matching stations to return.

    Raises:
        InvalidCommandArgs: Could not find the system.
    """
    try:
        found = session.query(System).\
            filter(System.name == system).\
            one()
    except sqla_orm.exc.NoResultFound as exc:
        raise cog.exc.InvalidCommandArgs(f"Could not find system: {system}\n\nPlease check for typos.") from exc

    return session.query(Station, System, System.dist_to(found)).\
        join(System, Station.system_id == System.id).\
        join(Faction, Station.controlling_minor_faction_id == Faction.id).\
        join(Government, Faction.government_id == Government.id).\
        filter(Government.text == gov_type).\
        order_by(System.dist_to(found)).\
        limit(limit).\
        all()


def get_all_systems_named(session, system_names, *, include_exploiteds=False):
    """Compute a full list of individual systems from a mixed list of controls and exploiteds.

    Args:
        session: A session onto the db.
        system_names: A list of system names.
        include_exploiteds: For any control system named, include all exploited systems in range.

    Returns: The list of system names resolved and those that were not found.
    """
    found = session.query(System).\
        filter(System.name.in_(system_names)).\
        all()

    exploits = []
    if include_exploiteds:
        for system in found:
            if system.power_state.text == "Control":
                exploits += get_systems_around(session, system.name)

    found_systems = sorted(list(set(found + exploits)), key=lambda x: x.name)
    found_names_lower = [x.name.lower() for x in found_systems]
    not_found = [x for x in system_names if x.lower() not in found_names_lower]

    return found_systems, not_found


def populate_system_controls(session):
    """
    Compute all pairs of control and exploited systems
    based on the current EDDB information.

    Insert the computed information into SystemControl objects.
    """
    session.query(SystemControl).delete()

    subq_pcontrol = session.query(PowerState.id).\
        filter(PowerState.text == 'Control').\
        scalar_subquery()
    control_ids = session.query(System.id).\
        filter(System.power_state_id == subq_pcontrol).\
        scalar_subquery()

    subq_pexploits = session.query(PowerState.id).\
        filter(PowerState.text.in_(['Exploited', 'Contested'])).\
        scalar_subquery()
    exploited = sqla_orm.aliased(System)
    systems = session.query(System.id, System.power_id, exploited.id, exploited.power_state_id).\
        filter(System.id.in_(control_ids)).\
        join(exploited, System.dist_to(exploited) <= 15).\
        filter(exploited.power_state_id.in_(subq_pexploits)).\
        all()

    for c_id, p_id, s_id, sp_id in systems:
        session.add(SystemControl(system_id=s_id, control_id=c_id, power_id=p_id,
                                  power_state_id=sp_id))


def add_history_track(eddb_session, system_names):
    """Add all systems to bgs tracking.

    Args:
        eddb_session: A session onto the db.
        systems: The list of systems to add.
    """
    system_ids = [
        x[0] for x in eddb_session.query(System.id).
        filter(System.name.in_(system_names)).
        all()
    ]
    eddb_session.query(HistoryTrack).\
        filter(HistoryTrack.system_id.in_(system_ids)).\
        delete()
    eddb_session.add_all([HistoryTrack(system_id=x) for x in system_ids])


def remove_history_track(eddb_session, system_names):
    """Add all systems to bgs tracking.

    Args:
        eddb_session: A session onto the db.
        systems: The list of systems to add.
    """
    system_ids = [
        x[0] for x in eddb_session.query(System.id).
        filter(System.name.in_(system_names)).
        all()
    ]
    eddb_session.query(HistoryTrack).\
        filter(HistoryTrack.system_id.in_(system_ids)).\
        delete()


def add_history_influence(eddb_session, latest_inf):
    """Add a history influence to the database.

    For the following, key pair means (system_id, faction_id).
    The following rules apply to adding data points:
        - Only add if latest_inf.system_id is in the HistoryTrack entries.
        - Ensure the number of entries for the key pair is <= HISTORY_INF_LIMIT
        - Add data points when the time since last point is > HISTORY_INF_TIME_GAP
            OR
        - Add data points when influence change > 1% and time since last point is > 1 hour (floor to prevent flood)

    Args:
        eddb_session: A session onto the db.
        latest_inf: An Influence entry that was updated (should be flush to db).
    """
    if not eddb_session.query(HistoryTrack).filter(HistoryTrack.system_id == latest_inf.system_id).all():
        return

    data = eddb_session.query(HistoryInfluence).\
        filter(
            HistoryInfluence.system_id == latest_inf.system_id,
            HistoryInfluence.faction_id == latest_inf.faction_id).\
        order_by(HistoryInfluence.updated_at.desc()).\
        all()

    # Only keep up to limit
    while len(data) > HISTORY_INF_LIMIT:
        last = data[-1]
        data = data[:-1]
        eddb_session.delete(last)

    if data:
        last = data[-1]
        # Influence stored as decimal of [0.0, 1.0]
        inf_diff = math.fabs(last.influence - latest_inf.influence)
        time_diff = latest_inf.updated_at - last.updated_at
        if (inf_diff >= 0.01 and time_diff > HOUR_SECONDS) or time_diff >= HISTORY_INF_TIME_GAP:
            eddb_session.add(HistoryInfluence.from_influence(latest_inf))

    else:
        eddb_session.add(HistoryInfluence.from_influence(latest_inf))


def service_status(eddb_session):
    """
    Poll for the status of the local EDDB database.

    Args:
        eddb_session: A session onto the EDDB db.

    Returns: A list of cells to format into a 2 column wide table.
    """
    now = datetime.datetime.utcnow()
    try:
        oldest = eddb_session.query(System).\
            order_by(System.updated_at.desc()).\
            limit(1).\
            one()
        cells = [
            ['Latest EDDB System Update', f'{oldest.name} ({now - oldest.updated_date} ago)']
        ]
    except sqla_orm.exc.NoResultFound:
        cells = [
            ['Oldest EDDB System', 'EDDB Database Empty'],
        ]

    return cells


def dump_db(session, classes, fname):
    """
    Dump db to a file.
    """
    with open(fname, "w", encoding='utf-8') as fout:
        for cls in classes:
            for obj in session.query(cls):
                fout.write(repr(obj) + '\n')


def drop_tables(*, all_tables=False):  # pragma: no cover | destructive to test
    """Ensure all safe to drop tables are dropped.

    Args:
        all_tables: When True, drop all EDDB tables.
    """
    meta = sqlalchemy.MetaData(bind=cogdb.eddb_engine)
    meta.reflect()
    for tbl in reversed(meta.sorted_tables):
        try:
            if is_safe_to_drop(tbl.name) or all_tables:
                tbl.drop()
        except sqla.exc.OperationalError:
            pass


def empty_tables(*, all_tables=False):
    """Ensure all safe to drop tables are empty.
    Due to sheer size of SModuleSold and SCommodityPricing, it more efficient to drop.

    Args:
        all_tables: When True, empty all EDDB tables.
    """
    sqla.orm.session.close_all_sessions()
    for table in (SCommodityPricing, SModuleSold):
        try:
            table.__table__.drop(cogdb.eddb_engine)
        except sqla.exc.OperationalError:
            pass

    meta = sqlalchemy.MetaData(bind=cogdb.eddb_engine)
    meta.reflect()
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for tbl in reversed(meta.sorted_tables):
            try:
                if is_safe_to_drop(tbl.name) or all_tables:
                    eddb_session.query(tbl).delete()
            except sqla.exc.OperationalError:
                pass
    Base.metadata.create_all(cogdb.eddb_engine)
    reset_autoincrements()


def reset_autoincrements():
    """
    Reset the autoincrement counts for particular tables whose counts keep rising via insertion.
    """
    with cogdb.eddb_engine.connect() as con:
        for cls in [SCommodity, SCommodityPricing, SModule, SModuleSold]:
            con.execute(sqla_sql.text(f"ALTER TABLE {cls.__tablename__} AUTO_INCREMENT = 1"))


def is_safe_to_drop(tbl_name):
    """Check if the table is safe to drop.

    Basically any table that can be reconstructed or imported from external can be dropped.
    For now, tables prefixed with 'spy_' and 'history_' will return False.
    """
    return not tbl_name.startswith('spy_') and not tbl_name.startswith('history_') and not tbl_name.startswith('pvp_')


# TODO: Bit messy but works for now.
#       Core SQLAlchemy lacks proper views, might be in libraries.
def recreate_tables():  # pragma: no cover | destructive to test
    """
    Recreate all tables in the database, mainly for schema changes and testing.
    """
    sqlalchemy.orm.session.close_all_sessions()

    drop_cmds = [
        "DROP VIEW eddb.v_systems_contested"
        "DROP VIEW eddb.v_systems_controlled",
        "DROP EVENT clean_conflicts",
        "DROP EVENT clean_history_influence",
    ]
    try:
        with cogdb.eddb_engine.connect() as con:
            for drop_cmd in drop_cmds:
                con.execute(sqla.sql.text(drop_cmd))
    except (sqla.exc.OperationalError, sqla.exc.ProgrammingError):
        pass

    drop_tables(all_tables=True)
    Base.metadata.create_all(cogdb.eddb_engine)
    try:
        SystemContestedV.__table__.drop(cogdb.eddb_engine)
    except sqla.exc.OperationalError:
        pass
    try:
        SystemControlV.__table__.drop(cogdb.eddb_engine)
    except sqla.exc.OperationalError:
        pass

    # Create views
    create_cmds = [
        VIEW_CONTESTEDS.strip(),
        VIEW_SYSTEM_CONTROLS.strip(),
        EVENT_CONFLICTS.strip(),
        EVENT_HISTORY_INFLUENCE.strip(),
    ]
    with cogdb.eddb_engine.connect() as con:
        for create_cmd in create_cmds:
            con.execute(sqla.sql.text(create_cmd))
    reset_autoincrements()


async def monitor_eddb_caches(*, delay_hours=4):  # pragma: no cover
    """
    Monitor and recompute cached tables:
        - Repopulates SystemControls from latest data.

    Kwargs:
        delay_hours: The hours between refreshing cached tables. Default: 2
    """
    while True:
        await asyncio.sleep(delay_hours * 3600)

        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            await asyncio.get_event_loop().run_in_executor(
                None, populate_system_controls, eddb_session
            )


def main_test_area(eddb_session):  # pragma: no cover
    """ A test area for testing things with schema. """
    station = eddb_session.query(Station).filter(Station.is_planetary).limit(5).all()[0]
    print(station.name, station.economies)

    #  Check relationships
    system = eddb_session.query(System).filter(System.name == 'Sol').one()
    print(system.allegiance, system.government, system.power, system.power_state, system.security)
    print('------')
    print(system.controls, system.controlling_faction)
    print('------')
    print(system.stations)
    print('------')
    print(system.controlling_faction.home_system)
    print('------')

    station = eddb_session.query(Station).\
        filter(
            Station.system_id == system.id,
            Station.name == "Daedalus").\
        one()
    print(station.system, station.type, station.features, station.faction)

    __import__('pprint').pprint(get_nearest_stations_with_features(
        eddb_session, centre_name='rana'), features=['apexinterstellar']
    )
    stations = get_shipyard_stations(eddb_session, input("Please enter a system name ... "))
    if stations:
        print(cog.tbl.format_table(stations))

    print('31 Aquilae')
    sys_alias = sqla_orm.aliased(System)
    sys_control = sqla_orm.aliased(System)
    systems = eddb_session.query(sys_alias.name, sys_control.name).\
        filter(sys_alias.name == '31 Aquilae').\
        join(SystemControl, sys_alias.id == SystemControl.system_id).\
        join(sys_control, sys_control.id == SystemControl.control_id).\
        all()
    __import__('pprint').pprint(systems)
    print('Togher')
    system = eddb_session.query(sys_alias).\
        filter(sys_alias.name == 'Togher').\
        one()
    __import__('pprint').pprint(system.controls)
    print('Wat Yu')
    system = eddb_session.query(sys).\
        filter(sys_alias.name == 'Wat Yu').\
        one()
    __import__('pprint').pprint(system.exploiteds)
    __import__('pprint').pprint(system.contesteds)


try:
    Base.metadata.create_all(cogdb.eddb_engine)
    with cogdb.session_scope(cogdb.EDDBSession) as init_session:
        preload_tables(init_session)
        PLANETARY_TYPE_IDS = [
            x[0] for x in
            init_session.query(StationType.id).filter(StationType.text.ilike('%planetary%')).all()
        ]
        HQS = {
            p.text.lower(): p.home_system.name for p in
            init_session.query(Power).filter(Power.text != 'None').all()
        }
    del init_session
except (AttributeError, sqla_orm.exc.NoResultFound, sqla.exc.ProgrammingError):  # pragma: no cover
    PLANETARY_TYPE_IDS = None
    HQS = None
FACTION_STATE_PAIRS = [
    ('active_states', FactionActiveState),
    ('pending_states', FactionPendingState),
    ('recovering_states', FactionRecoveringState)
]


if __name__ == "__main__":
    with cogdb.session_scope(cogdb.EDDBSession) as e_session:
        main_test_area(e_session)
