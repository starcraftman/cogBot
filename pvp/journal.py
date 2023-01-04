"""
Simple parser for Frontier's Player Journal written out while playing Elite: Dangerous.

Provide ability to read, parse and insert events into the database.

Reference:
    https://edcodex.info/?m=doc
"""
import datetime
import json
import logging

import sqlalchemy as sqla

import cogdb
import cogdb.eddb
from cogdb.eddb import LEN as EDDB_LEN
from cogdb.eddn import TIME_STRP
from cogdb.spy_squirrel import ship_type_to_id_map
import cog.inara
import pvp.schema

COMBAT_RANK_TO_VALUE = {x: ind for ind, x in enumerate(cog.inara.COMBAT_RANKS)}
VALUE_TO_COMBAT_RANK = {y: x for x, y in COMBAT_RANK_TO_VALUE.items()}


def parse_died(eddb_session, data):
    """
    Parse Died messages in log file.
    Handles both single death and wing deaths.

    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    is_wing_kill = "Killers" in data
    if 'KillerName' in data:  # Reformat object so single case same as wing
        data['Killers'] = [{
            "Name": clean_cmdr_name(data["KillerName"]),
            "Ship": data["KillerShip"],
            "Rank": data["KillerRank"],
        }]

    death = pvp.schema.PVPDeath(
        cmdr_id=data['cmdr_id'],
        is_wing_kill=is_wing_kill,
        event_at=data['event_at'],
    )
    eddb_session.add(death)
    eddb_session.flush()

    ship_map = ship_name_map()
    for killer in data["Killers"]:
        try:
            ship_id = ship_map[killer['Ship'].lower()]
        except KeyError:
            ship_id = ship_map['sidewinder']
            logging.getLogger(__name__).error("Could not map ship named: %s", killer['Ship'])

        eddb_session.add(
            pvp.schema.PVPDeathKiller(
                cmdr_id=data['cmdr_id'],
                ship_id=ship_id,
                pvp_death_id=death.id,
                name=clean_cmdr_name(killer['Name']),
                rank=COMBAT_RANK_TO_VALUE[killer['Rank']],
                event_at=data['event_at'],
            )
        )
        eddb_session.flush()

    return death


def parse_pvpkill(eddb_session, data):
    """
    Parse PVPKill messages in log file.

    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    kill = pvp.schema.PVPKill(
        cmdr_id=data.get('cmdr_id'),
        victim_name=clean_cmdr_name(data['Victim']),
        victim_rank=data['CombatRank'],
        event_at=data['event_at'],
    )
    eddb_session.add(kill)
    eddb_session.flush()

    return kill


def parse_pvpinterdiction(eddb_session, data):
    """
    Parse Interdiction messages in log file.

    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    if not data['IsPlayer']:
        return None

    interdiction = pvp.schema.PVPInterdiction(
        cmdr_id=data.get('cmdr_id'),
        victim_name=clean_cmdr_name(data['Interdicted']),
        is_player=data['IsPlayer'],
        is_success=data['Success'],
        victim_rank=data['CombatRank'] if data['IsPlayer'] else None,
        event_at=data['event_at'],
    )
    eddb_session.add(interdiction)
    eddb_session.flush()

    return interdiction


def parse_pvpinterdicted(eddb_session, data):
    """
    Parse Interdiction messages in log file.

    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    if not data['IsPlayer']:
        return None

    interdicted = pvp.schema.PVPInterdicted(
        cmdr_id=data.get('cmdr_id'),
        did_submit=data['Submitted'],
        is_player=data['IsPlayer'],
        interdictor_name=clean_cmdr_name(data['Interdictor']),
        interdictor_rank=data['CombatRank'] if data['IsPlayer'] else None,
        event_at=data['event_at'],
    )
    eddb_session.add(interdicted)
    eddb_session.flush()

    return interdicted


def parse_location(eddb_session, data):
    """
    Parse the Location messages in the log file.
    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    try:
        found, _ = cogdb.eddb.get_all_systems_named(eddb_session, [data['StarSystem']])
        location = pvp.schema.PVPLocation(
            cmdr_id=data.get('cmdr_id'),
            system_id=found[0].id,
            event_at=data['event_at'],
        )
        eddb_session.add(location)
        eddb_session.flush()
    except IndexError:
        location = None

    return location


def datetime_to_tstamp(date_string):
    """
    Convert a tiemstamp string in a log line to a simpler integer timestamp
    for storage in database.

    Args:
        date_string: The string containing a date of format specified in cogdb.eddn.TIME_STRP

    Returns:
        A float representation of the datetime parsed.

    Raises:
        ValueError: The datetime did not conform to expected format.
    """
    try:
        return datetime.datetime.strptime(date_string, TIME_STRP).timestamp()
    except ValueError:
        logging.getLogger(__name__).error("Malformed timestamp on log line: %s", date_string)
        raise


def parse_event(eddb_session, data):
    """
    Parse the event that has been passed in.

    Args:
        eddb_session: A session onto the db.
        data: The JSON data object.
    """
    event = data['event']
    if event in EVENT_TO_PARSER:
        return EVENT_TO_PARSER[event](eddb_session, data)

    logging.getLogger(__name__).error("Failed to parse event: %s", event)
    raise ValueError(f"No parser configured for: {event}")


def load_journal_possible(fname, cmdr_id):
    """
    Load an existing json file on server and then parse the lines to validate them.
    Any lines that fail to be validated will be ignored.

    Args:
        fname: The filename of the partial log.
        cmdr_id: The id of the commander who submitted the log.

    Returns: A list of parsed json objects ready to further process.
    """
    to_return = []
    with open(fname, 'r', encoding='utf-8') as fin, cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        lines = [x for x in fin.read().split('\n') if x]

        for line in lines:
            try:
                loaded = json.loads(line)
                loaded['event_at'] = datetime_to_tstamp(loaded['timestamp'])
                loaded['cmdr_id'] = cmdr_id
                to_return += [parse_event(eddb_session, loaded)]
            except json.decoder.JSONDecodeError:
                logging.getLogger(__name__).error("Failed to JSON decode line: %s", line)
            except ValueError as exc:
                logging.getLogger(__name__).warning(str(exc))
            except sqla.exc.IntegrityError:
                eddb_session.rollback()
                logging.getLogger(__name__).warning("Duplicate Event: %s", line)

    return to_return


def clean_cmdr_name(name):
    """
    Clean cmdr name for storage in db.
        - Strip off any leading 'cmdr'
        - Remove any extra whitespace
        - Limit to width of db field

    Args:
        name: The name of the cmdr to clean

    Returns: Cleaned cmdr name.
    """
    if name.lower().startswith('cmdr'):
        name = name[4:].strip()

    return name[:EDDB_LEN['pvp_name']]


def ship_name_map():
    """
    Generate a map of possibly seen names to IDs in the db.

    Returns: A dictionary of names onto ids.
    """
    ship_map = ship_type_to_id_map(traffic_text=True)
    ship_map.update(ship_type_to_id_map(traffic_text=False))
    ship_map.update({key.lower(): value for key, value in ship_map.items() if key[0].isupper()})

    return ship_map


EVENT_TO_PARSER = {
    "Died": parse_died,
    "FSDJump": parse_location,
    "Interdicted": parse_pvpinterdicted,
    "Interdiction": parse_pvpinterdiction,
    "Location": parse_location,
    "PVPKill": parse_pvpkill,
}
