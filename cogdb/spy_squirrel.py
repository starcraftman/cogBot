"""
Module to parse and import data from spying squirrel.
"""
import datetime

import sqlalchemy as sqla

import cogdb.eddb
from cogdb.eddb import Base, Power


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


class SpyVote(Base):
    """
    Record current vote by power.
    """
    __tablename__ = 'spy_votes'

    header = ["Power", "Consolidation"]

    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'), primary_key=True)
    vote = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, onupdate=sqla.func.unix_timestamp())

    # Relationships
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(Power.id) == SpyVote.power_id',
    )

    def __repr__(self):
        keys = ['power_id', 'vote', 'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        """ A pretty one line to give all information. """
        return "{power}: {vote}%, updated at {date}".format(
            vote=self.vote, power=self.power.text, date=self.updated_at)

    def __eq__(self, other):
        return isinstance(other, SpyVote) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}")


class SpyPrep(Base):
    """
    Store Prep triggers by systems.
    """
    __tablename__ = 'spy_preps'

    header = ["ID", "Power", "System", "Merits"]

    id = sqla.Column(sqla.Integer, primary_key=True)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'))
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'))
    merits = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, onupdate=sqla.func.unix_timestamp())

    # Relationships
    system = sqla.orm.relationship(
        'System', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(System.id) == SpyPrep.system_id',
    )
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(Power.id) == SpyPrep.power_id',
    )

    def __repr__(self):
        keys = ['id', 'power_id', 'system_id', 'merits', 'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        """ A pretty one line to give all information. """
        return "{power} {system}: {merits}, updated at {date}".format(
            merits=self.merits, power=self.power.text, system=self.system.name, date=self.updated_at)

    def __eq__(self, other):
        return isinstance(other, SpyPrep) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}_{self.system_id}")


class SpySystem(Base):
    """
    Store the current important information of the system.
    """
    __tablename__ = 'spy_systems'

    # ids
    id = sqla.Column(sqla.Integer, primary_key=True)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'))
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'))
    power_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), default=0)

    # info
    is_expansion = sqla.Column(sqla.Boolean, default=False)
    income = sqla.Column(sqla.Integer, default=0)
    upkeep_current = sqla.Column(sqla.Integer, default=0)
    upkeep_default = sqla.Column(sqla.Integer, default=0)
    fort = sqla.Column(sqla.Integer, default=0)
    fort_trigger = sqla.Column(sqla.Integer, default=0)
    um = sqla.Column(sqla.Integer, default=0)
    um_trigger = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, onupdate=sqla.func.unix_timestamp())

    # Relationships
    system = sqla.orm.relationship(
        'System', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(System.id) == SpySystem.system_id',
    )
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(Power.id) == SpySystem.power_id',
    )
    power_state = sqla.orm.relationship(
        'PowerState', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(PowerState.id) == SpySystem.power_state_id',
    )

    def __repr__(self):
        keys = ['id', 'system_id', 'power_id', 'power_state_id',
                'is_expansion', 'income', 'upkeep_current', 'upkeep_default',
                'fort', 'fort_trigger', 'um', 'um_trigger', 'updated_at']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        """ A pretty one line to give all information. """
        updated_at = datetime.datetime.utcfromtimestamp(self.updated_at)
        status_text = f"{self.fort} | {self.um}, updated at {updated_at}"
        if self.is_expansion:
            description = f"Expansion for {self.power.text} to {self.system.name}: {status_text}"
        else:
            description = f"{self.power.text} {self.system.name}: {status_text}"

        return description

    def __eq__(self, other):
        return isinstance(other, SpySystem) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}_{self.system_id}")


def json_powers_to_eddb_map():
    """
    Returns a simple map FROM power_id in JSON messages TO Power.id in EDDB.
    """
    with cogdb.session_scope(cogdb.EDDBSession) as session:
        eddb_power_names_to_id = {power.text: power.id for power in session.query(Power).all()}
        json_powers_to_eddb_id = {
            power_id: eddb_power_names_to_id[power_name]
            for power_id, power_name in POWER_ID_MAP.items()
        }

    return json_powers_to_eddb_id


def load_base_json(base):
    """ Load the base json and parse all information from it.

    Args:
        base: The base json to load.

    Returns:
        A dictionary mapping powers by name onto the systems they control and their status.
    """
    db_systems = []
    json_powers_to_eddb_id = json_powers_to_eddb_map()

    for bundle in base['powers']:
        power_id = json_powers_to_eddb_id[bundle['powerId']]
        #  power_name = POWER_ID_MAP[bundle['powerId']]

        for sys_addr, data in bundle['systemAddr'].items():
            kwargs = {
                'system_id': sys_addr,
                'power_id': power_id,
                'power_state_id': JSON_POWER_STATE_TO_EDDB[bundle['state']],
                'is_expansion': bundle['state'] == 'takingControl',
                'fort_trigger': data['thrFor'],
                'um_trigger': data['thrAgainst'],
                'income': data['income'],
                'upkeep_current': data['upkeepCurrent'],
                'upkeep_default': data['upkeepDefault'],
            }

            db_systems += [SpySystem(**kwargs)]

    return db_systems


def load_refined_json(refined):
    """ Load the refined json and parse all information from it.

    Args:
        refined: The refined json to load.

    Returns:
        A dictionary mapping powers by name onto the systems they control and their status.
    """
    updated_at = refined["lastModified"]
    json_powers_to_eddb_id = json_powers_to_eddb_map()

    db_preps, db_votes, db_sys = [], [], []
    for bundle in refined["preparation"]:
        power_id = json_powers_to_eddb_id[bundle['power_id']]
        db_votes += [SpyVote(power_id=power_id, vote=bundle['consolidation']['rank'], updated_at=updated_at)]
        db_preps += [
            SpyPrep(power_id=power_id, system_id=system_id, merits=merits, updated_at=updated_at)
            for system_id, merits in bundle['rankedSystems']
        ]

    for bundle in refined["gainControl"]:
        db_sys += [SpySystem(
            system_id=bundle['systemAddr'],
            power_id=json_powers_to_eddb_id[bundle['power_id']],
            power_state_id=64,
            fort=bundle['qtyFor'],
            um=bundle['qtyAgainst'],
            is_expansion=True,
            updated_at=updated_at,
        )]
    for bundle in refined["fortifyUndermine"]:
        db_sys += [SpySystem(
            system_id=bundle['systemAddr'],
            power_id=json_powers_to_eddb_id[bundle['power_id']],
            power_state_id=16,
            fort=bundle['qtyFor'],
            um=bundle['qtyAgainst'],
            updated_at=updated_at,
        )]

    return db_preps, db_votes, db_sys
