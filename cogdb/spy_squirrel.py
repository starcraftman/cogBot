"""
Module to parse and import data from spying squirrel.
"""
import json
import logging
import os
import pathlib
import re
import subprocess as sub
import tempfile
import time

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm
import sqlalchemy.exc as sqla_e
from sqlalchemy.ext.hybrid import hybrid_property

import cog.util
import cogdb.eddb
from cog.util import ReprMixin, TimestampMixin
from cogdb.eddb import Base, Power, System, Faction, Influence
from cogdb.schema import FortSystem, UMSystem, EFortType, EUMType, EUMSheet


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
}
MAX_SPY_MERITS = 99999
# Remove entries older than this relative current timestamp
TWO_WEEK_SECONDS = 14 * 24 * 60 * 60
EVENT_SPY_TRAFFIC = f"""
CREATE EVENT IF NOT EXISTS clean_spy_traffic
ON SCHEDULE
    EVERY 1 DAY
COMMENT "Check daily for SpyTraffic entries older than 14 days."
DO
    DELETE FROM eddb.spy_traffic
    WHERE updated_at < (unix_timestamp() - {TWO_WEEK_SECONDS});
"""
EVENT_SPY_TOP5 = """
CREATE EVENT IF NOT EXISTS clean_spy_top5
ON SCHEDULE
    EVERY 1 DAY
COMMENT "Check daily for SpyBounty entries older than 14 days."
DO
    DELETE FROM eddb.spy_top5
    WHERE updated_at < (unix_timestamp() - {TWO_WEEK_SECONDS});
"""
BOUNTY_CATEGORY_MAP = {
    'faction': 1,  # Local faction crimes
    'power': 2,  # The power, i.e. Hudson
    'super': 3,  # The super, i.e. Fed / Imp / Alliance
}


class SpyShip(ReprMixin, Base):
    """
    Constants for ship type for SpyTraffic.
    """
    __tablename__ = 'spy_ships'
    _repr_keys = ['id', 'text', 'traffic_text']

    id = sqla.Column(sqla.Integer, primary_key=True)
    text = sqla.Column(sqla.String(cogdb.eddb.LEN["ship"]))
    traffic_text = sqla.Column(sqla.String(cogdb.eddb.LEN["ship"]))

    def __str__(self):
        """ A pretty one line to give all information. """
        return f"Ship: {self.text}"

    def __eq__(self, other):
        return isinstance(other, SpyShip) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.id}")


# These entries will be stored for SPY_LIMIT days.
class SpyBounty(ReprMixin, TimestampMixin, Base):
    """
    Track the bounties active in a system.
    """
    __tablename__ = 'spy_top5'
    _repr_keys = [
        'id', 'category', 'pos', 'cmdr_name', 'ship_name', 'last_seen_system', 'last_seen_station',
        'bounty', 'ship_id', 'updated_at'
    ]

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    ship_id = sqla.Column(sqla.Integer, sqla.ForeignKey('spy_ships.id'))
    power_id = sqla.Column(sqla.Integer, nullable=True)

    pos = sqla.Column(sqla.Integer, primary_key=True, default=1)  # Should only be [1, 5]
    cmdr_name = sqla.Column(sqla.String(cogdb.eddb.LEN["cmdr_name"]), nullable=False, default="")
    ship_name = sqla.Column(sqla.String(cogdb.eddb.LEN["ship_name"]), nullable=False, default="")
    last_seen_system = sqla.Column(sqla.String(cogdb.eddb.LEN["system"]), nullable=False, default="")
    last_seen_station = sqla.Column(sqla.String(cogdb.eddb.LEN["station"]), nullable=False, default="")
    bounty = sqla.Column(sqla.BigInteger, default=0)
    category = sqla.Column(sqla.Integer, default=BOUNTY_CATEGORY_MAP['power'])
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    ship = sqla.orm.relationship(
        'SpyShip', uselist=False, lazy='joined', viewonly=True,
    )
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(Power.id) == SpyBounty.power_id',
    )

    def __str__(self):
        """ A pretty one line to give all information. """
        ship_text = "{}".format(self.ship.text if self.ship else self.ship_id)
        return f"""#{self.pos} {self.cmdr_name} last seen in {self.last_seen_system}/{self.last_seen_station} ({ship_text})
Has {self.bounty:,} in bounty, updated at {self.utc_date}"""

    def __eq__(self, other):
        return isinstance(other, SpyVote) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.id}_{self.pos}")

    @staticmethod
    def from_bounty_post(post, *, power_id=None, ship_map=None):
        """
        Generate a SpyBounty object from an existing bounty object.
        For empty entries (i.e. commanderId 0) will generate an effectively empty object

        Args:
            post: A dictionary with information from bounty post
            power_id: The power id of the system. If none, this is local
            ship_map: The map of ship names to SpyShip.ids

        Returns: A SpyBounty object.
        """
        name, ship_name, station, system, ship_id = '', '', '', '', None
        if not ship_map:
            ship_map = ship_type_to_id_map()

        if post['commanderId'] != 0:
            system = post['lastLocation']
            if ' - ' in system:
                system, station = system.split(' - ')

            name = post['name']
            mat = re.match(r'CMDR (.*) \((.*)\)', name)
            if mat:
                name = mat.group(1)
                parts = mat.group(2).split('"')
                if len(parts) == 3:
                    ship_name = parts[1]
                    try:
                        ship_id = ship_map[parts[0].strip()]
                    except KeyError:
                        pass

        kwargs = {
            'cmdr_name': name,
            'ship_name': ship_name,
            'ship_id': ship_id,
            'power_id': power_id,
            'last_seen_system': system,
            'last_seen_station': station,
            'pos': post['pos'],
            'bounty': post['value'],
            'category': post['category'],
        }
        if 'updated_at' in post:  # Mainly for testing, unsure if useful in production
            kwargs['updated_at'] = post['updated_at']

        return SpyBounty(**kwargs)

    @hybrid_property
    def is_faction(self):
        """
        Is the bounty for faction?
        """
        return self.category == BOUNTY_CATEGORY_MAP['faction']

    @hybrid_property
    def is_power(self):
        """
        Is the bounty for power (Hudson)?
        """
        return self.category == BOUNTY_CATEGORY_MAP['power']

    @hybrid_property
    def is_super(self):
        """
        Is the bounty for superpower (Federal)?
        """
        return self.category == BOUNTY_CATEGORY_MAP['super']


# These entries will be stored for SPY_LIMIT days.
class SpyTraffic(ReprMixin, Base):
    """
    Monitor traffic of different ships in the system.
    """
    __tablename__ = 'spy_traffic'
    _repr_keys = ['id', 'system', 'ship_id', 'cnt', 'updated_at']

    id = sqla.Column(sqla.BigInteger, primary_key=True)
    ship_id = sqla.Column(sqla.Integer, sqla.ForeignKey('spy_ships.id'))
    cnt = sqla.Column(sqla.Integer)
    system = sqla.Column(sqla.String(cogdb.eddb.LEN["system"]), nullable=False, default="")
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    ship = sqla.orm.relationship(
        'SpyShip', uselist=False, lazy='joined', viewonly=True,
    )

    def __str__(self):
        """ A pretty one line to give all information. """
        ship_text = "{}".format(self.ship.text if self.ship else self.ship_id)
        return f"{self.system} {ship_text}: {self.cnt}"

    def __eq__(self, other):
        return isinstance(other, SpyTraffic) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.id}")


class SpyVote(ReprMixin, TimestampMixin, Base):
    """
    Record current vote by power.
    """
    __tablename__ = 'spy_votes'
    _repr_keys = ['power_id', 'vote', 'updated_at']

    power_id = sqla.Column(sqla.Integer, primary_key=True)
    vote = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(Power.id) == SpyVote.power_id',
    )

    def __str__(self):
        """ A pretty one line to give all information. """
        return f"{self.power.text}: {self.vote}%, updated at {self.utc_date}"

    def __eq__(self, other):
        return isinstance(other, SpyVote) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}")


class SpyPrep(ReprMixin, TimestampMixin, Base):
    """
    Store Prep triggers by systems.
    """
    __tablename__ = 'spy_preps'
    _repr_keys = ['id', 'power_id', 'ed_system_id', 'merits', 'updated_at']

    __table_args__ = (
        sqla.UniqueConstraint('ed_system_id', 'power_id', name='system_power_constraint'),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    ed_system_id = sqla.Column(sqla.BigInteger, index=True, nullable=False)
    power_id = sqla.Column(sqla.Integer, nullable=False)

    system_name = sqla.Column(sqla.String(cogdb.eddb.LEN["system"]), index=True)  # Intentional caching for QoL
    merits = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    system = sqla.orm.relationship(
        'System', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(System.ed_system_id) == SpyPrep.ed_system_id',
    )
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(Power.id) == SpyPrep.power_id',
    )

    def __str__(self):
        """ A pretty one line to give all information. """
        power_text = "{}".format(self.power.text if self.power else self.power_id)
        system_text = "{}".format(self.system.name if self.system else self.ed_system_id)
        return f"{power_text} {system_text}: {self.merits}, updated at {self.utc_date}"

    def __eq__(self, other):
        return isinstance(other, SpyPrep) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}_{self.ed_system_id}")


class SpySystem(ReprMixin, TimestampMixin, Base):
    """
    Store the current important information of the system.
    """
    __tablename__ = 'spy_systems'
    _repr_keys = [
        'id', 'ed_system_id', 'power_id', 'power_state_id',
        'income', 'upkeep_current', 'upkeep_default',
        'fort', 'fort_trigger', 'um', 'um_trigger', 'updated_at'
    ]

    __table_args__ = (
        sqla.UniqueConstraint('ed_system_id', 'power_id', name='system_power_constraint'),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    ed_system_id = sqla.Column(sqla.BigInteger, index=True, nullable=False)
    power_id = sqla.Column(sqla.Integer, nullable=False)
    power_state_id = sqla.Column(sqla.Integer, nullable=False, default=0)

    system_name = sqla.Column(sqla.String(cogdb.eddb.LEN["system"]), index=True)  # Intentional caching for QoL
    income = sqla.Column(sqla.Integer, default=0)
    upkeep_current = sqla.Column(sqla.Integer, default=0)
    upkeep_default = sqla.Column(sqla.Integer, default=0)
    fort = sqla.Column(sqla.Integer, default=0)
    fort_trigger = sqla.Column(sqla.Integer, default=0)
    um = sqla.Column(sqla.Integer, default=0)
    um_trigger = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, default=time.time)
    held_merits = sqla.Column(sqla.Integer, default=0)
    stolen_forts = sqla.Column(sqla.Integer, default=0)
    held_updated_at = sqla.Column(sqla.Integer, default=time.time)

    # Relationships
    system = sqla.orm.relationship(
        'System', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(System.ed_system_id) == SpySystem.ed_system_id',
    )
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(Power.id) == SpySystem.power_id',
    )
    power_state = sqla.orm.relationship(
        'PowerState', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(PowerState.id) == SpySystem.power_state_id',
    )

    def __str__(self):
        """ A pretty one line to give all information. """
        status_text = f"{self.fort}/{self.fort_trigger} | {self.um}/{self.um_trigger}, updated at {self.utc_date}"
        power_text = "{}".format(self.power.text if self.power else self.power_id)
        system_text = "{}".format(self.system.name if self.system else self.ed_system_id)
        if self.is_expansion:
            description = f"Expansion for {power_text} to {system_text}: {status_text}"
        else:
            description = f"{power_text} {system_text}: {status_text}"

        return description

    def __eq__(self, other):
        return isinstance(other, SpySystem) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}_{self.ed_system_id}")

    @hybrid_property
    def is_expansion(self):
        """ Is this an expansion system? """
        return self.power_state_id != 16

    def update(self, **kwargs):
        """
        Simple kwargs update to this object.
        Any key will be set against this db object with the value associated.
        """
        for key, val in kwargs.items():
            setattr(self, key, val)


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


def ship_type_to_id_map(traffic_text=False):
    """
    Returns a simple map from ship type to the id in SpyShip table.
    """
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        ships = eddb_session.query(SpyShip).\
            all()
        attrname = 'traffic_text' if traffic_text else 'text'
        mapped = {getattr(ship, attrname): ship.id for ship in ships}

        try:
            del mapped[None]
        except KeyError:
            pass

        return mapped


def fetch_json_secret(secrets_path, name):
    """
    Check if required secrets available, if not fetch them.
    Executing this function requires local install of secrethub + credentials to read.

    Args:
        secrets_path: Path to a directory to put secrets.
        name: Name of the json secret to fetch
    """
    pat = pathlib.Path(os.path.join(secrets_path, f'{name}.json'))
    cmd = ['secrethub', 'read', '-o', str(pat), f'starcraftman/cogbot/tests/json/{name}']
    if not pat.exists():
        print(f"fetching: {pat}")
        sub.check_call(cmd)


def load_json_secret(fname):
    """
    Load a json file example for API testing.

    Args:
        fname: The filename to load or fetch from secrethub.
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
    db_systems = []
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
                    'power_state_id': JSON_POWER_STATE_TO_EDDB[bundle['state']],
                    'fort_trigger': data['thrFor'],
                    'um_trigger': data['thrAgainst'],
                    'income': data['income'],
                    'upkeep_current': data['upkeepCurrent'],
                    'upkeep_default': data['upkeepDefault'],
                }
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
                db_systems += [SpySystem(**kwargs)]


def load_refined_json(refined):
    """
    Load the refined json and parse all information from it.

    Args:
        refined: The refined json to load.

    Returns:
        A dictionary mapping powers by name onto the systems they control and their status.
    """
    updated_at = int(refined["lastModified"])
    json_powers_to_eddb_id = json_powers_to_eddb_map()

    db_objs = []
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
            db_objs += [spyvote]

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
                db_objs += [spyprep]
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
                db_objs += [system]


def parse_params(input):
    """
    Generically parse the params object of JSON messages.

    Where needed provide decoding and casting as needed.

    Args:
        input: A JSON object, with fields as expected.

    Returns: A simplified object with information from params.
    """
    flat = {}
    for ent in input:
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


def parse_response_news_summary(input):
    """
    Capabale of parsing the faction news summary.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(input['params'])

    parts = list(info["list"].split(':'))
    info["influence"] = float(parts[1].split('=')[1])
    info["happiness"] = int(re.match(r'.*HappinessBand(\d)', parts[2]).group(1))
    info["name"] = info["factionName"]
    del info["list"]
    del info["factionName"]

    return info


def parse_response_trade_goods(input):
    """
    Capabale of parsing the trade goods available.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(input['params'])
    return [val for key, val in info.items()]


def parse_response_bounties_claimed(input):
    """
    Capabale of parsing claimed and given bounties.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    return parse_params(input['params'])


def parse_response_top5_bounties(input):
    """
    Capabale of parsing the top 5 bounties.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(input['params'])

    # Transform params information into better structure
    return {
        i: {
            'pos': i,
            'value': info[f"bountyValue{i}"],
            'commanderId': info[f"commanderId{i}"],
            'lastLocation': info[f"lastLocation{i}"],
            'name': info[f"name{i}"],
            'category': info['type']
        } for i in range(1, 6)
    }


def parse_response_traffic_totals(input):
    """
    Capabale of parsing the top 5 bounties.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(input['params'])

    result = {
        'total': info['total'],
        'by_ship': {}
    }
    del info['total']
    for val in info.values():
        name, num = val.split('; - ')
        result['by_ship'][name.replace('_NAME', '')[1:].lower()] = int(num)

    return result


def parse_response_power_update(input):
    """
    Capabale of parsing the power update information.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    params = input['params']
    return {
        "power": params[0]["value"],
        "stolen_forts": int(params[1]["value"]),
        "held_merits": int(params[2]["value"]),
    }


def load_response_json(response):
    """
    Capable of fully parsing and processing information in a response JSON.

    Args:
        response: A large JSON object returned from POST.
    """
    results = {}
    for news_info in response.values():
        result = {}
        for entry in news_info['news']:
            parser = PARSER_MAP[entry['type']]
            try:
                result[parser['name']] += [parser['func'](entry)]
            except KeyError:
                result[parser['name']] = [parser['func'](entry)]

        # Prune any lists of 1 element, to not be lists.
        for key, value in result.items():
            if isinstance(value, list) and len(value) == 1:
                result[key] = value[0]

        # Separate top5s if both present
        if len(result['top5']) > 1:
            for group in result['top5']:
                cat = list(group.values())[0]["category"]
                result[f'top5_{cat}'] = group
            del result['top5']

        sys_name = result['factions'][0]['system']
        results[sys_name] = result

    update_based_response_info(results)

    return results


def update_based_response_info(info):
    """
    Based on fully parsed response to a POST, update the database.

    Args:
        info: The large info dict containing information returned and parsed from POST.
    """
    ship_map = ship_type_to_id_map(traffic_text=False)
    ship_map_traffic = ship_type_to_id_map(traffic_text=True)
    log = logging.getLogger(__name__)

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for sys_name, sys_info in info.items():
            # Handle updating data for influence of factions
            for faction in sys_info['factions']:
                try:
                    found = eddb_session.query(cogdb.eddb.Influence).\
                        join(cogdb.eddb.System, Influence.system_id == System.id).\
                        join(cogdb.eddb.Faction, Influence.faction_id == Faction.id).\
                        filter(
                            cogdb.eddb.System.name == sys_name,
                            cogdb.eddb.Faction.name == faction['name']).\
                        one()
                except sqla.orm.exc.NoResultFound:
                    # Somehow influence not there, faction must be new to system
                    try:
                        system_id = eddb_session.query(System.id).\
                            filter(System.name == sys_name).\
                            one()[0]
                        faction_id = eddb_session.query(Faction).\
                            filter(Faction.name == faction['name']).\
                            one()[0]
                        found = Influence(system_id=system_id, faction_id=faction_id)
                        eddb_session.add(found)
                    except sqla_orm.exc.NoResultFound:
                        log.warning("Failed to find combination of: %s | %s", sys_name, sys_info['factions'])

                found.happiness_id = faction['happiness']
                found.influence = faction['influence']
                # FIXME: Enable at end.
                #  cogdb.eddb.add_history_influence(eddb_session, influence)

            # Handle held merits
            system = None
            try:
                system = eddb_session.query(SpySystem).\
                    filter(SpySystem.system_name == sys_name).\
                    one()
                system.held_merits = sys_info['power']['held_merits']
                system.stolen_forts = sys_info['power']['stolen_forts']
            except sqla.orm.exc.NoResultFound:
                log.warning("Adding SpySystem for: %s", sys_name)
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

            if 'top5_power' in sys_info:
                for b_info in sys_info['top5_power'].values():
                    bounty = SpyBounty.from_bounty_post(b_info, power_id=system.power_id, ship_map=ship_map)
                    print(repr(bounty))

            for ship_name, cnt in sys_info['traffic']['by_ship'].items():
                try:
                    traffic = SpyTraffic(
                        cnt=cnt,
                        ship_id=ship_map_traffic[ship_name],
                        system=system.system_name,
                    )
                    print(repr(traffic))
                except KeyError:
                    print("Not found", ship_name)


def compare_sheet_fort_systems_to_spy(session, eddb_session):
    """Compare the fort systems to the spy systems and determine the
       intersection, then take the SpySystem.fort and SpySystem.um values if they are greater.

    Args:
        session: A session onto the db.
        eddb_session: A session onto the EDDB db.
    """
    fort_targets = session.query(FortSystem).\
        filter(FortSystem.type == EFortType.fort).\
        all()
    fort_names = [x.name for x in fort_targets]
    fort_dict = {x.name: x for x in fort_targets}

    systems = {}
    for system in fort_targets:
        systems.update({
            system.name: {
                'sheet_col': system.sheet_col,
                'sheet_order': system.sheet_order,
                'fort': system.fort_status,
                'um': system.um_status,
            }
        })

    spy_systems = eddb_session.query(SpySystem).\
        join(System, System.ed_system_id == SpySystem.ed_system_id).\
        filter(System.name.in_(fort_names)).\
        all()
    for spy_sys in spy_systems:
        systems[spy_sys.system.name]['fort'] = spy_sys.fort
        fort_dict[spy_sys.system.name].fort_status = spy_sys.fort

        systems[spy_sys.system.name]['um'] = spy_sys.um + spy_sys.held_merits
        fort_dict[spy_sys.system.name].um_status = spy_sys.um + spy_sys.held_merits

    return list(sorted(systems.values(), key=lambda x: x['sheet_order']))


def compare_sheet_um_systems_to_spy(session, eddb_session):
    """Compare the um systems to the spy systems and determine the
       intersection, then find the new progress_us and progress_them values.

    Args:
        session: A session onto the db.
        eddb_session: A session onto the EDDB db.
    """
    um_targets = session.query(UMSystem).\
        filter(
            UMSystem.type == EUMType.control,
            UMSystem.sheet_src == EUMSheet.main).\
        all()
    um_names = [x.name for x in um_targets]
    um_dict = {x.name: x for x in um_targets}

    systems = {}
    for system in um_targets:
        systems.update({
            system.name: {
                'sheet_col': system.sheet_col,
                'progress_us': system.progress_us,
                'progress_them': system.progress_them,
                'map_offset': system.map_offset,
            }
        })

    spy_systems = eddb_session.query(SpySystem).\
        join(System, System.ed_system_id == SpySystem.ed_system_id).\
        filter(System.name.in_(um_names)).\
        all()
    for spy_sys in spy_systems:
        if spy_sys.um > systems[spy_sys.system.name]['progress_us']:
            systems[spy_sys.system.name]['progress_us'] = spy_sys.um + spy_sys.held_merits
            um_dict[spy_sys.system.name].progress_us = spy_sys.um + spy_sys.held_merits

        spy_progress_them = spy_sys.fort / spy_sys.fort_trigger
        if spy_progress_them > systems[spy_sys.system.name]['progress_them']:
            systems[spy_sys.system.name]['progress_them'] = spy_progress_them
            um_dict[spy_sys.system.name].progress_them = spy_progress_them

    return list(sorted(systems.values(), key=lambda x: x['sheet_col']))


def preload_spy_tables(eddb_session):
    eddb_session.add_all([
        SpyShip(id=1, text="Adder", traffic_text="adder"),
        SpyShip(id=2, text="Alliance Challenger"),
        SpyShip(id=3, text="Alliance Chieftain"),
        SpyShip(id=4, text="Alliance Crusader"),
        SpyShip(id=5, text="Anaconda", traffic_text="anaconda"),
        SpyShip(id=6, text="Asp Explorer"),
        SpyShip(id=7, text="Asp Scout", traffic_text="asp"),
        SpyShip(id=8, text="Beluga Liner", traffic_text="belugaliner"),
        SpyShip(id=9, text="Cobra MK IV"),
        SpyShip(id=10, text="Cobra Mk. III", traffic_text="cobramkiii"),
        SpyShip(id=11, text="Diamondback Explorer", traffic_text="diamondbackxl"),
        SpyShip(id=12, text="Diamondback Scout", traffic_text="diamondback"),
        SpyShip(id=13, text="Dolphin", traffic_text="dolphin"),
        SpyShip(id=14, text="Eagle Mk. II", traffic_text="eagle"),
        SpyShip(id=15, text="Federal Assault Ship"),
        SpyShip(id=16, text="Federal Corvette", traffic_text="federation_corvette"),
        SpyShip(id=17, text="Federal Dropship", traffic_text="federation_dropship_mkii"),
        SpyShip(id=18, text="Federal Gunship"),
        SpyShip(id=19, text="Fer-de-Lance", traffic_text="ferdelance"),
        SpyShip(id=20, text="Hauler", traffic_text="hauler"),
        SpyShip(id=21, text="Imperial Clipper"),
        SpyShip(id=22, text="Imperial Courier"),
        SpyShip(id=23, text="Imperial Cutter", traffic_text="cutter"),
        SpyShip(id=24, text="Imperial Eagle", traffic_text="empire_eagle"),
        SpyShip(id=25, text="Keelback"),
        SpyShip(id=26, text="Krait MkII", traffic_text="krait_mkii"),
        SpyShip(id=27, text="Krait Phantom", traffic_text="krait_light"),
        SpyShip(id=28, text="Mamba", traffic_text="mamba"),
        SpyShip(id=29, text="Orca", traffic_text="orca"),
        SpyShip(id=30, text="Python", traffic_text="python"),
        SpyShip(id=31, text="Sidewinder Mk. I"),
        SpyShip(id=32, text="Type-10 Defender"),
        SpyShip(id=33, text="Type-6 Transporter", traffic_text="type6"),
        SpyShip(id=34, text="Type-7 Transporter", traffic_text="type7"),
        SpyShip(id=35, text="Type-9 Heavy"),
        SpyShip(id=36, text="Viper MK IV", traffic_text="viper_mkiv"),
        SpyShip(id=37, text="Viper Mk III", traffic_text="viper"),
        SpyShip(id=38, text="Vulture", traffic_text="vulture"),
    ])
    eddb_session.commit()


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
    """
    sqla.orm.session.close_all_sessions()
    drop_tables()
    Base.metadata.create_all(cogdb.eddb_engine)


def main():  # pragma: no cover | destructive to test
    """
    Main function to load the test data during development.
    """
    recreate_tables()

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        preload_spy_tables(eddb_session)
        load_base_json(load_json_secret('base.json'))
        load_refined_json(load_json_secret('refined.json'))
        load_response_json(load_json_secret('response.json'))


SPY_TABLES = [SpyPrep, SpyVote, SpySystem, SpyTraffic, SpyBounty, SpyShip]
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
if cogdb.TEST_DB:
    recreate_tables()
else:
    Base.metadata.create_all(cogdb.eddb_engine)


if __name__ == "__main__":
    main()
