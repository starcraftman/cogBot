"""
Simple parser for Frontier's Player Journal written out while playing Elite: Dangerous.

Provide ability to read, parse and insert events into the database.

Reference:
    https://edcodex.info/?m=doc
"""
import datetime
import json
import logging

import aiofiles
import sqlalchemy as sqla

import cogdb
import cogdb.eddb
from cogdb.eddb import LEN as EDDB_LEN
from cogdb.spy_squirrel import ship_type_to_id_map
import cog.inara
from cog.util import TIME_STRP
import pvp.schema

COMBAT_RANK_TO_VALUE = {x: ind for ind, x in enumerate(cog.inara.COMBAT_RANKS)}
VALUE_TO_COMBAT_RANK = {y: x for x, y in COMBAT_RANK_TO_VALUE.items()}


class ParserError(Exception):
    """
    Simple exception to denote unsupported parsing.
    """


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

    try:
        death = eddb_session.query(pvp.schema.PVPDeath).\
            filter(pvp.schema.PVPDeath.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPDeath.system_id == data['system_id'],
                   pvp.schema.PVPDeath.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        death = pvp.schema.PVPDeath(
            cmdr_id=data['cmdr_id'],
            system_id=data['system_id'],
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
    try:
        kill = eddb_session.query(pvp.schema.PVPKill).\
            filter(pvp.schema.PVPKill.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPKill.system_id == data['system_id'],
                   pvp.schema.PVPKill.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        kill = pvp.schema.PVPKill(
            cmdr_id=data.get('cmdr_id'),
            system_id=data['system_id'],
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

    try:
        interdiction = eddb_session.query(pvp.schema.PVPInterdiction).\
            filter(pvp.schema.PVPInterdiction.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPInterdiction.system_id == data['system_id'],
                   pvp.schema.PVPInterdiction.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        interdiction = pvp.schema.PVPInterdiction(
            cmdr_id=data.get('cmdr_id'),
            system_id=data['system_id'],
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

    try:
        interdicted = eddb_session.query(pvp.schema.PVPInterdicted).\
            filter(pvp.schema.PVPInterdicted.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPInterdicted.system_id == data['system_id'],
                   pvp.schema.PVPInterdicted.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        interdicted = pvp.schema.PVPInterdicted(
            cmdr_id=data.get('cmdr_id'),
            system_id=data['system_id'],
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
        location = eddb_session.query(pvp.schema.PVPLocation).\
            filter(pvp.schema.PVPLocation.cmdr_id == data['cmdr_id'],
                   pvp.schema.PVPLocation.event_at == data['event_at']).\
            one()
    except sqla.exc.NoResultFound:
        try:
            found, _ = cogdb.eddb.get_all_systems_named(eddb_session, [data['StarSystem']])
            location = pvp.schema.PVPLocation(
                cmdr_id=data['cmdr_id'],
                system_id=found[0].id,
                event_at=data['event_at'],
            )
            eddb_session.add(location)
            eddb_session.flush()
        except IndexError:
            location = None

    return location


def parse_cmdr_name(data):
    """
    Scan a line of log looking for possible commander name.

    Args:
        data: The JSON data.

    Returns: The CMDR name if found, otherwise None.
    """
    cmdr = None

    if data['event'] == 'LoadGame':
        cmdr = data['Commander']
    elif data['event'] == 'Commander':
        cmdr = data['Name']

    return cmdr


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


def get_parser(data):
    """
    Lookup the required parser for a given event in the data.

    Args:
        eddb_session: A session onto the db.
        data: The JSON data object.
    """
    event = data['event']
    if event in EVENT_TO_PARSER:
        return event, EVENT_TO_PARSER[event]

    raise ParserError(f"No parser configured for: {event}")


class Parser():
    """
    Parse a given journal fragment.
    """
    def __init__(self, *, fname, cmdr_id, eddb_session):
        self.fname = fname
        self.cmdr_id = cmdr_id
        self.eddb_session = eddb_session
        self.lines = None
        self.data = {}  # Scratch space for objects

    def load(self):
        """
        Load and cleanup the lines from the journal fragment.
        """
        with open(self.fname, 'r', encoding='utf-8') as fin:
            self.lines = fin.readlines()

    def parse_line(self, line):
        """
        Parse a single line event and make required changes to both the database
        and the tracked data that assists in parsing forward.

        Args:
            line: The line from the journal uploaded.

        Returns: None if no parsing was possible. If success, returns the parsed event object in database.
        """
        result = None
        try:
            for loaded in load_journal_events(line):
                loaded.update({
                    'event_at': datetime_to_tstamp(loaded['timestamp']),
                    'cmdr_id': self.cmdr_id,
                    'system_id': self.data['Location'].system_id if self.data.get('Location') else None,
                })
                event, parser = get_parser(loaded)

                # If a CMDR supercruises reset events tracking, still same location
                if event in ['SupercruiseEntry', 'SupercruiseExit']:
                    self.data = {'Location': self.data.get('Location')}
                    return result

                result = parser(self.eddb_session, loaded)
                self.post_parsing(event, result)
        except json.decoder.JSONDecodeError:
            logging.getLogger(__name__).error("Failed to JSON decode line: %s", line)
        except (ParserError, ValueError) as exc:
            logging.getLogger(__name__).warning(str(exc))
        except sqla.exc.IntegrityError:
            self.eddb_session.rollback()
            logging.getLogger(__name__).warning("Duplicate Event: %s", line)

        return result

    def post_parsing(self, event, result):
        """
        By tracking previous events in db between jump events, link events together.
        For instance, if a CMDR interdicts another ship then kills it, that is a PVPInterdictedKill, that will
        link to the individual PVPInterdiction and PVPKill records.

        Args:
            event: The event that triggered the log.
            result: The parsed database object.
        """
        # Link events that were connected for later statistics
        if event == 'PVPKill' and self.data.get('Interdiction') and\
                result.victim_name == self.data['Interdiction'].victim_name and\
                result.event_at >= self.data['Interdiction'].event_at:
            self.eddb_session.add(pvp.schema.PVPInterdictionKill(
                cmdr_id=result.cmdr_id,
                pvp_interdiction_id=self.data['Interdiction'].id,
                pvp_kill_id=result.id,
                event_at=result.event_at,
            ))
            self.eddb_session.flush()

        elif event == 'PVPKill' and self.data.get('Interdicted') and\
                result.victim_name == self.data['Interdicted'].interdictor_name and\
                result.event_at >= self.data['Interdicted'].event_at:
            self.eddb_session.add(pvp.schema.PVPInterdictedKill(
                cmdr_id=result.cmdr_id,
                pvp_interdicted_id=self.data['Interdicted'].id,
                pvp_kill_id=result.id,
                event_at=result.event_at,
            ))
            self.eddb_session.flush()

        elif event == 'Died' and self.data.get('Interdiction') and\
                result.killed_by(self.data['Interdiction'].victim_name) and\
                result.event_at >= self.data['Interdiction'].event_at:
            self.eddb_session.add(pvp.schema.PVPInterdictionDeath(
                cmdr_id=result.cmdr_id,
                pvp_interdiction_id=self.data['Interdiction'].id,
                pvp_death_id=result.id,
                event_at=result.event_at,
            ))
            self.eddb_session.flush()

        elif event == 'Died' and self.data.get('Interdicted') and\
                result.killed_by(self.data['Interdicted'].interdictor_name) and\
                result.event_at >= self.data['Interdicted'].event_at:
            self.eddb_session.add(pvp.schema.PVPInterdictedDeath(
                cmdr_id=result.cmdr_id,
                pvp_interdicted_id=self.data['Interdicted'].id,
                pvp_death_id=result.id,
                event_at=result.event_at,
            ))
            self.eddb_session.flush()

        # Always store event in data for later use
        if event in ['Interdiction', 'Interdicted', 'PVPKill']:
            self.data[event] = result

        # When changing location reset data and set Locaation
        elif event in ['Location', 'FSDJump']:
            self.data.clear()
            self.data['Location'] = result

        # When dying all connections reset
        elif event == 'Died':
            self.data.clear()

    def parse(self):
        """
        Parse all possible events from the journal fragment.

        Returns: All parsed main events.
        """
        to_return = []
        for line in self.lines:
            returned = self.parse_line(line)
            if returned:
                to_return += [returned]

        pvp.schema.update_pvp_stats(self.eddb_session, self.cmdr_id)

        return to_return


async def find_cmdr_name(fname):
    """
    Given a journal file, scan until you can identify the CMDR name.

    Args:
        fname: The filename of the journal.

    Returns: The CMDR name if found printed in log, otherwise None if not found.
    """
    async with aiofiles.open(fname, 'r', encoding='utf-8') as fin:
        async for line in fin:
            try:
                for event in load_journal_events(line):
                    cmdr_name = parse_cmdr_name(event)
                    if cmdr_name:
                        return cmdr_name
            except json.decoder.JSONDecodeError:
                pass

    return None


def load_journal_events(json_line):
    """
    Best effort attempt to load JSON objects from a line.
    Assume initially that it is one object per line.
    On failure, assume Frontier put multiple JSONs and add list around them.

    Args:
        json_line: A line that is assumed to have some JSON on it, one or more.

    Returns: A list of JSON objects, at least 1.

    Raises: json.decoder.JSONDecodeError - Failed to load any JSON on line.
    """
    try:
        events = [json.loads(json_line)]
    except json.decoder.JSONDecodeError:
        try:
            events = json.loads(f'[ {json_line} ] ')
        except json.decoder.JSONDecodeError:
            logging.getLogger(__name__).error("Impossible to parse: %s", json_line)
            raise

    return events


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
    "SupercruiseEntry": parse_location,
    "SupercruiseExit": parse_location,
    "Location": parse_location,
    "PVPKill": parse_pvpkill,
}
