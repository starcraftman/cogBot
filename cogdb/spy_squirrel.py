"""
Module to parse and import data from spying squirrel.
"""
import asyncio
import concurrent.futures as cfut
import datetime
import json
import logging
import os
import pathlib
import random
import re
import subprocess as sub
import tempfile
import time

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.exc as sqla_e

import cogdb.common
import cogdb.eddb
from cogdb.eddb import (
    Base, Faction, Influence, Power, PowerState, Ship, System,
    SpySystem, SpyPrep, SpyVote, SpyBounty, SpyTraffic, ship_type_to_id_map
)
from cogdb.schema import FortSystem, UMSystem, EUMType, EUMSheet
import cog.util


# Map of powers used in incoming JSON messages
POWER_ID_MAP = {
    100000: "Aisling Duval",
    100010: "Edmund Mahon",
    100020: "Arissa Lavigny-Duval",
    100040: "Felicia Winters",
    100050: "Denton Patreus",
    100060: "Zachary Hudson",
    100070: "Li Yong-Rui",
    100080: "Zemina Torval",
    100090: "Pranav Antal",
    100100: "Archon Delaine",
    100120: "Yuri Grom",
}
# Based on cogdb.eddb.PowerState values
JSON_POWER_STATE_TO_EDDB = {
    "control": 16,
    "takingControl": 64,
    "turmoil": 240,
}
MAX_SPY_MERITS = 99999
# Remove entries older than this relative current timestamp
HELD_POWERS = {}
HELD_RUNNING = """Scrape for power: {power_name} already running.
Started at {date}. Please try again later."""
HELD_DELAY = (20, 120)
JSON_DOPPLER_MAP = {
    'base': 2,
    'refined': 1,
    'response': 1,
}
ELITE_YEAR_OFFSET = 1286


def convert_json_date(date_text):
    """
    Convert an embedded date in response json to the real time it was updated.

    Returns: The UTC timestamp represented by the date in text.
    """
    parsed = datetime.datetime.strptime(date_text, '%d %b %Y')
    parsed = parsed.replace(year=parsed.year - ELITE_YEAR_OFFSET, tzinfo=datetime.timezone.utc)
    return parsed.timestamp()


def json_powers_to_eddb_map():
    """
    Returns a simple map FROM power_id in JSON messages TO Power.id in EDDB.
    """
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        eddb_power_names_to_id = {power.text: power.id for power in eddb_session.query(Power)}
        json_powers_to_eddb_id = {
            power_id: eddb_power_names_to_id[power_name]
            for power_id, power_name in POWER_ID_MAP.items()
        }

    return json_powers_to_eddb_id


def fetch_json_secret(secrets_path, name):
    """
    Check if required secrets available, if not fetch them.
    Executing this function requires local install of doppler + credentials.
    If in doubt contact project owner for help.

    Args:
        secrets_path: Path to a directory to put secrets.
        name: Name of the json secret to fetch
    """
    pat = pathlib.Path(os.path.join(secrets_path, f'{name}.json'))
    cmd = ['doppler', 'secrets', 'get', '--plain'] +\
          [f'JSON_{name.upper()}_{num}' for num in range(1, JSON_DOPPLER_MAP[name] + 1)]
    if not pat.exists():
        print(f"fetching: {pat}")
        with open(str(pat), 'wb') as fout:
            fout.write(sub.check_output(cmd))


def load_json_secret(fname):
    """
    Load a json file example for API testing.

    Args:
        fname: The filename to load or fetch from doppler.
    """
    path = pathlib.Path(os.path.join(tempfile.gettempdir(), fname))
    if not path.exists():
        fetch_json_secret(tempfile.gettempdir(), fname.replace('.json', ''))

    with path.open('r', encoding='utf-8') as fin:
        return json.load(fin)


def load_base_json(base):
    """
    Load the base json and parse all information from it.

    Args:
        base: The base json to load.

    Returns:
        A dictionary mapping powers by name onto the systems they control and their status.
    """
    if isinstance(base, type("")):
        base = json.loads(base)

    json_powers_to_eddb_id = json_powers_to_eddb_map()
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for bundle in base['powers']:
            power_id = json_powers_to_eddb_id[bundle['powerId']]

            for sys_addr, data in bundle['systemAddr'].items():
                eddb_system = eddb_session.query(System).\
                    filter(System.ed_system_id == sys_addr).\
                    one()
                kwargs = {
                    'ed_system_id': sys_addr,
                    'system_name': eddb_system.name,
                    'power_id': power_id,
                    'power_state_id': 0,
                    'fort_trigger': data['thrFor'],
                    'um_trigger': data['thrAgainst'],
                    'income': data['income'],
                    'upkeep_current': data['upkeepCurrent'],
                    'upkeep_default': data['upkeepDefault'],
                }
                # Protect against missing power state possibilities
                try:
                    kwargs['power_state_id'] = JSON_POWER_STATE_TO_EDDB[bundle['state']]
                except KeyError:
                    logging.getLogger(__name__).error("Failed to find power_state_id for %s", bundle['state'])

                try:
                    system = eddb_session.query(SpySystem).\
                        filter(
                            SpySystem.ed_system_id == sys_addr,
                            SpySystem.power_id == power_id).\
                        one()
                    system.update(**kwargs)
                except sqla.orm.exc.NoResultFound:
                    system = SpySystem(**kwargs)
                    eddb_session.add(system)


def load_refined_json(refined):
    """
    Load the refined json and parse all information from it.

    Args:
        refined: The refined json to load.

    Returns:
        A dictionary mapping powers by name onto the systems they control and their status.
    """
    if isinstance(refined, type("")):
        refined = json.loads(refined)

    updated_at = int(refined["lastModified"])
    json_powers_to_eddb_id = json_powers_to_eddb_map()

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for bundle in refined["preparation"]:
            power_id = json_powers_to_eddb_id[bundle['powerid']]
            if 'consolidation' in bundle:
                try:
                    spyvote = eddb_session.query(SpyVote).\
                        filter(SpyVote.power_id == power_id).\
                        one()
                    spyvote.vote = bundle['consolidation']['rank']
                    spyvote.updated_at = updated_at
                except sqla.orm.exc.NoResultFound:
                    spyvote = SpyVote(
                        power_id=power_id,
                        vote=bundle['consolidation']['rank'],
                        updated_at=updated_at
                    )
                    eddb_session.add(spyvote)

            for ed_system_id, merits in bundle['rankedSystems']:
                eddb_system = eddb_session.query(System).\
                    filter(System.ed_system_id == ed_system_id).\
                    one()
                try:
                    spyprep = eddb_session.query(SpyPrep).\
                        filter(
                            SpyPrep.ed_system_id == ed_system_id,
                            SpyPrep.power_id == power_id).\
                        one()
                    spyprep.merits = merits
                    spyprep.updated_at = updated_at
                    spyprep.system_name = eddb_system.name
                except sqla.orm.exc.NoResultFound:
                    spyprep = SpyPrep(
                        power_id=power_id,
                        ed_system_id=ed_system_id,
                        system_name=eddb_system.name,
                        merits=merits,
                        updated_at=updated_at
                    )
                    eddb_session.add(spyprep)
        eddb_session.commit()

        for bundles, pstate_id in [[refined["gainControl"], 64], [refined["fortifyUndermine"], 16]]:
            for bundle in bundles:
                power_id = json_powers_to_eddb_id[bundle['powerid']]
                ed_system_id = bundle['systemAddr']
                eddb_system = eddb_session.query(System).\
                    filter(System.ed_system_id == ed_system_id).\
                    one()
                kwargs = {
                    'power_id': power_id,
                    'ed_system_id': ed_system_id,
                    'system_name': eddb_system.name,
                    'power_state_id': pstate_id,
                    'fort': bundle['qtyFor'],
                    'um': bundle['qtyAgainst'],
                    'updated_at': updated_at,
                }
                try:
                    system = eddb_session.query(SpySystem).\
                        filter(
                            SpySystem.ed_system_id == ed_system_id,
                            SpySystem.power_id == power_id).\
                        one()
                    system.update(**kwargs)
                except sqla.orm.exc.NoResultFound:
                    system = SpySystem(**kwargs)
                    eddb_session.add(system)


def parse_params(data):
    """
    Generically parse the params object of JSON messages.

    Where needed provide decoding and casting as needed.

    Args:
        data: A JSON object, with fields as expected.

    Returns: A simplified object with information from params.
    """
    flat = {}
    for ent in data:
        # Ensure no collisions in keys of flat dict
        f_key = ent["key"]
        cnt = 0
        while f_key in flat:
            cnt += 1
            f_key = f"{ent['key']}{cnt}"

        if ent["type"] in ("string", "list") and "$" not in ent["value"] and ent["key"] != "type":
            flat[f_key] = cog.util.hex_decode(ent["value"])
        else:
            flat[f_key] = ent["value"]

        if ent["type"] == "int":
            flat[f_key] = int(ent["value"])

    return flat


def parse_response_news_summary(data):
    """
    Capabale of parsing the faction news summary.

    Args:
        data: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(data['params'])

    parts = list(info["list"].split(':'))
    info["influence"] = float(parts[1].split('=')[1])
    info["happiness"] = int(re.match(r'.*HappinessBand(\d)', parts[2]).group(1))
    info["name"] = info["factionName"]
    del info["list"]
    del info["factionName"]

    return info


def parse_response_trade_goods(data):
    """
    Capabale of parsing the trade goods available.

    Args:
        data: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(data['params'])
    return [val for key, val in info.items()]


def parse_response_bounties_claimed(data):
    """
    Capabale of parsing claimed and given bounties.

    Args:
        data: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    return parse_params(data['params'])


def parse_response_top5_bounties(data):
    """
    Capabale of parsing the top 5 bounties.

    Args:
        data: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(data['params'])
    updated_at = convert_json_date(data['date'])

    # Transform params information into better structure
    return {
        i: {
            'pos': i,
            'value': info[f"bountyValue{i}"],
            'commanderId': info[f"commanderId{i}"],
            'lastLocation': info[f"lastLocation{i}"],
            'name': info[f"name{i}"],
            'category': info['type'],
            'updated_at': updated_at,
        } for i in range(1, 6)
    }


def parse_response_traffic_totals(data):
    """
    Capabale of parsing the top 5 bounties.

    Args:
        data: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(data['params'])

    result = {
        'total': info['total'],
        'by_ship': {}
    }
    del info['total']
    for val in info.values():
        name, num = val.split('; - ')
        result['by_ship'][name.replace('_NAME', '')[1:].lower()] = int(num)

    return result


def parse_response_power_update(data):
    """
    Capabale of parsing the power update information.

    Args:
        data: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    params = data['params']
    return {
        "power": params[0]["value"],
        "stolen_forts": int(params[1]["value"]),
        "held_merits": int(params[2]["value"]),
    }


def load_response_json(response):
    """
    Capable of fully parsing and processing information in a response JSON.

    Information will first be processed into a large readable object.
    Then object will be parsed to update the database.

    Args:
        response: A large JSON object returned from POST.

    Returns: A list of Influence.ids that were updated.
    """
    results = {}

    for news_info in response.values():
        result = {}
        for entry in news_info['news']:
            try:
                parser = PARSER_MAP[entry['type']]
            except KeyError:
                logging.getLogger(__name__).error("RESPONSE PARSER FAILED: %s", entry['type'])
                __import__('pprint').pprint(news_info['news'])
            try:
                result[parser['name']] += [parser['func'](entry)]
            except KeyError:
                result[parser['name']] = [parser['func'](entry)]

        # Separate top5s if both present
        if 'top5' in result:
            for group in result['top5']:
                cat = group[1]["category"]
                result[f'top5_{cat}'] = group
            del result['top5']

        # Prune any lists of 1 element, to not be lists.
        for key, value in result.items():
            if isinstance(value, list) and len(value) == 1:
                result[key] = value[0]

        # FIXME: 15 Geminorum ==> 14 Geminorum, player database issue
        sys_name = result['factions'][0]['system']
        if sys_name == "15 Geminorum":
            sys_name = "14 Geminorum"
            for faction in result['factions']:
                faction['system'] = sys_name

        # Put system name in top level for convenience
        results[sys_name] = result

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        influence_ids = response_json_update_influences(eddb_session, results)
        response_json_update_system_info(eddb_session, results)

    return influence_ids


def response_json_update_influences(eddb_session, info):
    """
    Update the eddb.Influence objects for the system in question.

    If any system doesn't contain the faction, add it to database.
    In addition, add the influence to history.

    Args:
        eddb_session: The session onto the db.
        info: The object with information parsed from response.json.
    """
    log = logging.getLogger(__name__)

    influence_ids = []
    for sys_name, sys_info in info.items():
        if 'factions' not in sys_info:
            continue

        # Handle updating data for influence of factions
        for faction in sys_info['factions']:
            try:
                found = eddb_session.query(Influence).\
                    join(System, Influence.system_id == System.id).\
                    join(Faction, Influence.faction_id == Faction.id).\
                    filter(
                        System.name == sys_name,
                        Faction.name == faction['name']).\
                    one()
                log.info("Updating faction %s in %s", faction['name'], sys_name)
            except sqla.orm.exc.NoResultFound:
                # Somehow influence not there, faction must be new to system
                try:
                    system_id = eddb_session.query(System.id).\
                        filter(System.name == sys_name).\
                        one()[0]
                    faction_id = eddb_session.query(Faction.id).\
                        filter(Faction.name == faction['name']).\
                        one()[0]
                    found = Influence(system_id=system_id, faction_id=faction_id)
                    eddb_session.add(found)
                    eddb_session.flush()
                    log.info("Adding faction %s in %s", faction['name'], sys_name)
                except sqla_orm.exc.NoResultFound as exc:
                    log.error('IMP Exception on query: %s', str(exc))
                    log.error("IMP Failed to find combination of: %s | %s", sys_name, faction['name'])
                    continue

            found.happiness_id = faction['happiness']
            found.influence = faction['influence']
            influence_ids += [found.id]
            cogdb.eddb.add_history_influence(eddb_session, found)

    return influence_ids


def response_json_update_system_info(eddb_session, info):
    """
    Update the system wide information based on response.

    Information updated includes:
        - held merits and stolen forts
        - the bounties in system
        - the traffic in system

    Args:
        eddb_session: The session onto the db.
        info: The object with information parsed from response.json.
    """
    ship_map = ship_type_to_id_map(traffic_text=False)
    ship_map_traffic = ship_type_to_id_map(traffic_text=True)
    log = logging.getLogger(__name__)

    now_time = time.time()
    for sys_name, sys_info in info.items():
        try:
            system = eddb_session.query(SpySystem).\
                filter(SpySystem.system_name == sys_name).\
                one()
            log.info("Updating SpySystem %s", sys_name)
        except sqla.orm.exc.NoResultFound:
            eddb_system = eddb_session.query(System).\
                filter(System.name == sys_name).\
                one()
            kwargs = {
                'ed_system_id': eddb_system.ed_system_id,
                'system_name': eddb_system.name,
                'power_id': eddb_system.power_id,
                'power_state_id': eddb_system.power_state_id,
                'held_merits': sys_info['power']['held_merits'],
                'stolen_forts': sys_info['power']['stolen_forts'],
            }
            system = SpySystem(**kwargs)
            eddb_session.add(system)
            log.warning("Adding SpySystem for held merits: %s", sys_name)

        # Always update held time based on response
        system.held_updated_at = now_time
        if 'power' in sys_info:
            system.held_merits = sys_info['power']['held_merits']
            system.stolen_forts = sys_info['power']['stolen_forts']
            log.info("Updating held merits in %s", sys_name)

        if 'top5_power' in sys_info:
            log.warning("Parsing top 5 bounties for: %s", sys_name)
            for b_info in sys_info['top5_power'].values():
                b_info['system'] = sys_name
                bounty = SpyBounty.from_bounty_post(b_info, power_id=system.power_id, ship_map=ship_map)
                eddb_session.add(bounty)

        # Only keep current traffic values for now
        eddb_session.query(SpyTraffic).\
            filter(SpyTraffic.system == sys_name).\
            delete()
        log.warning("Parsing ship traffic for: %s", sys_name)
        if 'traffic' in sys_info:
            for ship_name, cnt in sys_info['traffic']['by_ship'].items():
                try:
                    traffic = SpyTraffic(
                        cnt=cnt,
                        ship_id=ship_map_traffic[ship_name],
                        system=sys_name,
                    )
                    eddb_session.add(traffic)
                except KeyError:
                    log.error("Not found %s", ship_name)


def compare_sheet_fort_systems_to_spy(session, eddb_session):
    """Compare the fort systems to the spy systems and determine the
       intersection, then take the SpySystem.fort and SpySystem.um values if they are greater.

    Args:
        session: A session onto the db.
        eddb_session: A session onto the EDDB db.
    """
    fort_targets = session.query(FortSystem).all()
    fort_dict = {x.name.lower(): x for x in fort_targets}

    systems = {
        system.name.lower(): {
            'sheet_col': system.sheet_col,
            'sheet_order': system.sheet_order,
            'fort': system.fort_status,
            'um': system.um_status,
        } for system in fort_targets
    }

    spy_systems = eddb_session.query(SpySystem).\
        join(System, System.ed_system_id == SpySystem.ed_system_id).\
        filter(System.name.in_(list(fort_dict.keys()))).\
        all()
    for spy_sys in spy_systems:
        spy_name = spy_sys.system.name.lower()
        systems[spy_name]['fort'] = spy_sys.fort
        fort_dict[spy_name].fort_status = spy_sys.fort

        systems[spy_name]['um'] = spy_sys.um + spy_sys.held_merits
        fort_dict[spy_name].um_status = spy_sys.um + spy_sys.held_merits

    return list(sorted(systems.values(), key=lambda x: x['sheet_order']))


def compare_sheet_um_systems_to_spy(session, eddb_session, *, sheet_src=EUMSheet.main):
    """Compare the um systems to the spy systems and determine the
       intersection, then find the new progress_us and progress_them values.

    Args:
        session: A session onto the db.
        eddb_session: A session onto the EDDB db.
        sheet_src: The source of the um information. EUMSheet enum expected.
    """
    um_targets = session.query(UMSystem).\
        filter(UMSystem.sheet_src == sheet_src).\
        all()
    um_dict = {x.name.lower(): x for x in um_targets}

    systems = {
        system.name.lower(): {
            'sheet_col': system.sheet_col,
            'progress_us': system.progress_us,
            'progress_them': system.progress_them,
            'map_offset': system.map_offset,
            'type': system.type,
        } for system in um_targets
    }

    spy_systems = eddb_session.query(SpySystem).\
        join(System, System.ed_system_id == SpySystem.ed_system_id).\
        filter(System.name.in_(list(um_dict.keys()))).\
        all()
    for spy_sys in spy_systems:
        spy_name = spy_sys.system.name.lower()
        system = systems[spy_name]

        if system['type'] == EUMType.expand:
            spy_progress_us = spy_sys.fort + spy_sys.held_merits
            spy_progress_them = spy_sys.um / spy_sys.um_trigger
        else:
            spy_progress_us = spy_sys.um + spy_sys.held_merits
            spy_progress_them = spy_sys.fort / spy_sys.fort_trigger
        del system['type']

        if spy_progress_us > systems[spy_name]['progress_us']:
            system['progress_us'] = spy_progress_us
            um_dict[spy_name].progress_us = spy_progress_us

        if spy_progress_them > systems[spy_name]['progress_them']:
            system['progress_them'] = spy_progress_them
            um_dict[spy_name].progress_them = spy_progress_them

    return list(sorted(systems.values(), key=lambda x: x['sheet_col']))


def get_spy_systems_for_galpow(eddb_session, power_id):
    """
    Get the systems and votes required to update a power tab on galpow.

    Included in the return is the following for the power_id selected:
        - controsl SpySystems
        - preps SpyPreps
        - expansion SpySystems
        - vote SpyVote

    Args:
        eddb_session: A session onto the db.
        power_id: The Power.id to select information for.

    Returns:
        controls, preps, expansions, vote
    """
    controls = eddb_session.query(SpySystem).\
        join(PowerState, SpySystem.power_state_id == PowerState.id).\
        filter(SpySystem.power_id == power_id,
               PowerState.text == "Control").\
        order_by(sqla.func.lower(SpySystem.system_name)).\
        all()
    preps = eddb_session.query(SpyPrep).\
        filter(SpyPrep.power_id == power_id).\
        order_by(SpyPrep.merits.desc()).\
        limit(10).\
        all()
    expansions = eddb_session.query(SpySystem).\
        join(PowerState, SpySystem.power_state_id == PowerState.id).\
        filter(SpySystem.power_id == power_id,
               PowerState.text == "Expansion").\
        order_by(sqla.func.lower(SpySystem.system_name)).\
        all()
    try:
        vote = eddb_session.query(SpyVote).\
            filter(SpyVote.power_id == power_id).\
            one()
    except sqla.orm.exc.NoResultFound:
        vote = None

    return controls, preps, expansions, vote


def get_vote_of_power(eddb_session, power='%hudson'):
    """
    Get the current spy vote amount for a particular power.

    Args:
        eddb_session: A session onto the db.
        power: The loose match of power name. By default hudson's current vote.

    Returns: The current amount of consolidation of a system.
    """
    try:
        vote_amount = eddb_session.query(SpyVote).\
            join(Power, SpyVote.power_id == Power.id).\
            filter(Power.text.ilike(power)).\
            one().vote
    except sqla.orm.exc.NoResultFound:
        vote_amount = 0

    return vote_amount


async def check_federal_held():  # pragma: no cover, would ping API point needlessly
    """
    Schedule a scrape of federal powers if there are SpySystems that need held updated.
    If remote API is down simply silently log the failure.
    """
    now = datetime.datetime.utcnow()
    log = logging.getLogger(__name__)
    log.warning("Checking held merits for federal systesm: %s", now)

    for power_name in ('Felicia Winters', 'Zachary Hudson'):
        log.warning("Checking held merits for: %s, %s", power_name, datetime.datetime.utcnow())
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            try:
                await execute_power_scrape(eddb_session, power_name)
            except cog.exc.RemoteError as exc:
                log.error("RemoteError on federal scrape: %s", str(exc))
            except cog.exc.InvalidCommandArgs:
                log.debug("Scrape was already running, ignore.")

        await asyncio.sleep(random.randint(*HELD_DELAY) * random.randint(1, 3))  # Randomly delay between


async def execute_power_scrape(eddb_session, power_name, *, callback=None, hours_old=7):  # pragma: no cover, would ping API point needlessly
    """Schedule a scrape of controls of a given power for detailed information.

    This function will prevent multiple concurrent scrapes at same time.

    Args:
        eddb_session: A session onto the EDDB db.
        power_name: The name of the power to scrape.
        callback: If present, messages will be sent back over the callback.

    Raises:
        cog.exc.RemoteError: Remote api was down
    """
    if power_name in HELD_POWERS:
        raise cog.exc.InvalidCommandArgs(
            HELD_RUNNING.format(power_name=power_name, date=HELD_POWERS[power_name]['start_date'])
        )

    systems = get_controls_outdated_held(eddb_session, power=power_name, hours_old=hours_old)
    sys_names = ", ".join([x.name for x in systems])

    if callback:
        msg = f"Will update the following systems:\n\n{sys_names}"
        await callback(msg)
        logging.getLogger(__name__).info(msg)

    HELD_POWERS[power_name] = {
        'start_date': datetime.datetime.utcnow(),
        'start_time': time.time(),
    }
    try:
        influence_ids = await post_systems(systems, callback=callback)
    finally:
        del HELD_POWERS[power_name]

    return influence_ids


async def post_systems(systems, callback=None):  # pragma: no cover, would ping API point needlessly
    """
    Helper function, take a list of systems and query their information.

    Args:
        systems: The list of cogdb.eddb.Systems that are found.

    Returns: A list of cogdb.eddb.Influence ids updated.

    Raises:
        RemoteError: The remote site is down.
    """
    log = logging.getLogger(__name__)

    influence_ids = []
    delay_values = [random.randint(x - 4, x + 8) for x in HELD_DELAY]  # Randomly choose bounds too
    api_url = f'{cog.util.CONF.scrape.api}?token={cog.util.CONF.scrape.token}'
    for sys in systems:
        log.warning("POSTAPI Request: %s.", sys.name)

        retry = 0
        while retry < 180:
            try:
                response_text = await cog.util.post_json_url(
                    api_url, {sys.name: sys.ed_system_id}, timeout=180
                )
                break
            except asyncio.TimeoutError:
                retry += 30
                log.error("POSTAPI Timeout: %s. Retry in %d seconds.", sys.name, retry)
                await asyncio.sleep(retry)  # Linear retry backoff

        response_json = json.loads(str(response_text))
        log.warning("POSTAPI Received: %s.", sys.name)
        with cfut.ProcessPoolExecutor(max_workers=1) as pool:
            influence_ids += await asyncio.get_event_loop().run_in_executor(
                pool, load_response_json, response_json
            )
        log.warning("POSTAPI Finished Parsing: %s.", sys.name)
        if callback:
            await callback(f'{sys.name} has been updated.')
        log.warning("POSTAPI Finished Parsing: %s.", sys.name)
        delay = random.randint(*delay_values)
        log.warning("POSTAPI Waiting %d seconds. Will resume at: %s",
                    delay, datetime.datetime.utcnow() + datetime.timedelta(seconds=delay))
        await asyncio.sleep(delay)  # Randomly delay chosen post calls

    if callback:
        sys_names = ", ".join([x.name for x in systems])[:1800]
        await callback(f'Scrape of {len(systems)} systems has completed. The following were updated:\n\n{sys_names}')

    return influence_ids


def get_controls_outdated_held(eddb_session, *, power='%hudson', hours_old=7):
    """
    Get all control Systems of a power mentioned where the held_updated_at date
    is at least hours_old.

    Args:
        eddb_session: A session onto the EDDB db.
        power: The loose like match of the power, i.e. "%hudson".
        hours_old: Update any systems that have held_data this many hours old. Default: >= 7
    """
    cutoff = time.time() - (hours_old * 60 * 60)

    return eddb_session.query(System).\
        join(SpySystem, System.name == SpySystem.system_name).\
        join(PowerState, System.power_state_id == PowerState.id).\
        join(Power, System.power_id == Power.id).\
        filter(
            Power.text.ilike(power),
            PowerState.text == "Control",
            SpySystem.held_updated_at < cutoff).\
        order_by(System.name).\
        all()


async def service_status(eddb_session):
    """
    Poll for the status of the spy service.

    Args:
        eddb_session: A session onto the EDDB db.

    Returns: A list of cells to format into a 2 column wide table.
    """
    now = datetime.datetime.utcnow()
    try:
        liveness = 'Up'
    except cog.exc.RemoteError:
        liveness = 'Down'

    cells = [['Spy Squirrel', liveness]]
    try:
        recent = eddb_session.query(SpySystem).\
            order_by(SpySystem.updated_at.asc(), SpySystem.system_name).\
            limit(1).\
            one()
        cells += [['Oldest Fort Info', f'{recent.system_name} ({now - recent.updated_date} ago)']]
    except sqla_orm.exc.NoResultFound:
        cells += [['Oldest Fort Info', 'N/A']]
    try:
        hudson_power = eddb_session.query(Power.id).filter(Power.text.ilike("%hudson")).scalar()
        hudson = eddb_session.query(SpySystem).\
            join(Power, Power.id == SpySystem.power_id).\
            filter(SpySystem.power_id == hudson_power,
                   SpySystem.power_state_id == 16).\
            order_by(SpySystem.held_updated_at.asc(), SpySystem.system_name).\
            limit(1).\
            one()
        cells += [['Hudson Oldest Held Merits', f'{hudson.system_name} ({now - hudson.held_updated_date} ago)']]
    except sqla_orm.exc.NoResultFound:
        cells += [['Hudson Oldest Held Merits', 'N/A']]
    try:
        winters_power = eddb_session.query(Power.id).filter(Power.text.ilike("%winters")).scalar()
        winters = eddb_session.query(SpySystem).\
            filter(SpySystem.power_id == winters_power,
                   SpySystem.power_state_id == 16).\
            order_by(SpySystem.held_updated_at.asc(), SpySystem.system_name).\
            limit(1).\
            one()
        cells += [['Winters Oldest Held Merits', f'{winters.system_name} ({now - winters.held_updated_date} ago)']]
    except sqla_orm.exc.NoResultFound:
        cells += [['Winters Oldest Held Merits', 'N/A']]

    return cells


def drop_tables():  # pragma: no cover | destructive to test
    """
    Drop the spy tables entirely.
    """
    sqla.orm.session.close_all_sessions()
    for table in SPY_TABLES:
        try:
            table.__table__.drop(cogdb.eddb_engine)
        except sqla_e.OperationalError:
            pass


def empty_tables():
    """
    Ensure all spy tables are empty.
    """
    sqla.orm.session.close_all_sessions()
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for table in SPY_TABLES:
            eddb_session.query(table).delete()


def recreate_tables():  # pragma: no cover | destructive to test
    """
    Recreate all tables in the related to this module, mainly for schema changes and testing.
    Always reload preloads.
    """
    sqla.orm.session.close_all_sessions()
    drop_tables()
    Base.metadata.create_all(cogdb.eddb_engine)


def main():  # pragma: no cover | destructive to test
    """
    Main function to load the test data during development.
    """
    recreate_tables()

    try:
        load_base_json(load_json_secret('base.json'))
        load_refined_json(load_json_secret('refined.json'))
        load_response_json(load_json_secret('response.json'))
    except FileNotFoundError:
        print("Could not load required json.")
        print("Please install and configure doppler.")


SPY_TABLES = [SpyPrep, SpyVote, SpySystem, SpyTraffic, SpyBounty]
PARSER_MAP = {
    "NewsSummaryFactionStateTitle": {
        'func': parse_response_news_summary,
        'name': 'factions',
    },
    "PowerUpdate": {
        'func': parse_response_power_update,
        'name': 'power',
    },
    "bountiesClaimed": {
        'func': parse_response_bounties_claimed,
        'name': 'bountiesClaimed',
    },
    "bountiesGiven": {
        'func': parse_response_bounties_claimed,
        'name': 'bountiesGiven',
    },
    "stateTradeGoodCommodities": {
        'func': parse_response_trade_goods,
        'name': 'trade',
    },
    "stateTradeBadCommodities": {
        'func': parse_response_trade_goods,
        'name': 'trade',
    },
    "top5Bounties": {
        'func': parse_response_top5_bounties,
        'name': 'top5',
    },
    "trafficTotals": {
        'func': parse_response_traffic_totals,
        'name': 'traffic',
    },
}
# Ensure the tables are created before use when this imported
Base.metadata.create_all(cogdb.eddb_engine)


if __name__ == "__main__":
    main()
