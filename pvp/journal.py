"""
Simple parser for Frontier's Player Journal written out while playing Elite: Dangerous.

Provide ability to read, parse and insert events into the database.

Reference:
    https://edcodex.info/?m=doc
"""
import datetime
import json
import logging

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
    __import__('pprint').pprint(data)

    is_wing_kill = "Killers" in data
    if 'KillerName' in data:  # Reformat object so single case same as wing
        data['Killers'] = [{
            "Name": data["KillerName"],
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

    for killer in data["Killers"]:
        eddb_session.add(
            pvp.schema.PVPDeathKiller(
                cmdr_id=data['cmdr_id'],
                pvp_death_id=death.id,
                killer_name=killer['Name'][:25],
                # TODO: Might need another mapping of name to id
                #  killer_ship=SHIP_NAME_TO_ID[killer['Ship']],k
                killer_ship=1,
                killer_rank=COMBAT_RANK_TO_VALUE[killer['Rank']],
                event_at=data['event_at'],
            )
        )

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
        victim_name=data['Victim'],
        victim_rank=data['CombatRank'],
        event_at=data['event_at'],
    )
    eddb_session.add(kill)

    return kill


def parse_pvpinterdiction(eddb_session, data):
    """
    Parse Interdiction messages in log file.

    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    interdiction = pvp.schema.PVPInterdiction(
        cmdr_id=data.get('cmdr_id'),
        victim_name=data['Interdicted'],
        is_player=data['IsPlayer'],
        is_success=data['Success'],
        victim_rank=data['CombatRank'],
        event_at=data['event_at'],
    )
    eddb_session.add(interdiction)

    return interdiction


def parse_pvpinterdicted(eddb_session, data):
    """
    Parse Interdiction messages in log file.

    Args:
        eddb_session: A session onto the db.
        data: A JSON object with the data to parse.

    Returns: The added object.
    """
    interdicted = pvp.schema.PVPInterdicted(
        cmdr_id=data.get('cmdr_id'),
        did_submit=data['Submitted'],
        is_player=data['IsPlayer'],
        interdictor_name=data['Interdictor'],
        interdictor_rank=data['CombatRank'],
        event_at=data['event_at'],
    )
    eddb_session.add(interdicted)

    return interdicted


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


def load_journal_possible(fname, cmdr_id=None):
    """
    Load an existing json file on server and then parse the lines to validate them.
    Any lines that fail to be validated will be ignored.

    Returns JSON objects to easily use.
    """
    json_objs = []
    with open(fname, 'r', encoding='utf-8') as fin:
        lines = [x for x in fin.read().split('\n') if x]

        for line in lines:
            try:
                loaded = json.loads(line)
                loaded['event_at'] = datetime_to_tstamp(loaded['timestamp'])
                if cmdr_id:
                    loaded['cmdr_id'] = cmdr_id
                json_objs += [loaded]
            except json.decoder.JSONDecodeError:
                logging.getLogger(__name__).error("Failed to parse player journal line: {line}")

    return json_objs


def main():
    pass


EVENT_TO_PARSER = {
    "Died": parse_died,
}
SHIP_NAME_TO_ID = ship_type_to_id_map()

if __name__ == "__main__":
    main()
