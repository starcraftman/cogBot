"""
Module to parse and import data from spying squirrel.
"""
import datetime
import json
import os
import pathlib
import time

import sqlalchemy as sqla
import sqlalchemy.exc as sqla_e
from sqlalchemy.ext.hybrid import hybrid_property

import cog.util
import cogdb.eddb
from cogdb.eddb import Base, Power, System


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

    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'), primary_key=True)
    vote = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(Power.id) == SpyVote.power_id',
    )

    def __repr__(self):
        keys = ['power_id', 'vote', 'updated_at']
        kwargs = [f'{key}={getattr(self, key)!r}' for key in keys]

        return f"{self.__class__.__name__}({', '.join(kwargs)})"

    def __str__(self):
        """ A pretty one line to give all information. """
        date = datetime.datetime.utcfromtimestamp(self.updated_at)
        return f"{self.power.text}: {self.vote}%, updated at {date}"

    def __eq__(self, other):
        return isinstance(other, SpyVote) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}")


class SpyPrep(Base):
    """
    Store Prep triggers by systems.
    """
    __tablename__ = 'spy_preps'

    __table_args__ = (
        sqla.UniqueConstraint('ed_system_id', 'power_id', name='system_power_constraint'),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    ed_system_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('systems.ed_system_id'))
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'))
    merits = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    system = sqla.orm.relationship(
        'System', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(System.ed_system_id) == SpyPrep.ed_system_id',
    )
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(Power.id) == SpyPrep.power_id',
    )

    def __repr__(self):
        keys = ['id', 'power_id', 'ed_system_id', 'merits', 'updated_at']
        kwargs = [f'{key}={getattr(self, key)!r}' for key in keys]

        return f"{self.__class__.__name__}({', '.join(kwargs)})"

    def __str__(self):
        """ A pretty one line to give all information. """
        date = datetime.datetime.utcfromtimestamp(self.updated_at)
        return f"{self.power.text} {self.system.name}: {self.merits}, updated at {date}"

    def __eq__(self, other):
        return isinstance(other, SpyPrep) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}_{self.ed_system_id}")


class SpySystem(Base):
    """
    Store the current important information of the system.
    """
    __tablename__ = 'spy_systems'

    __table_args__ = (
        sqla.UniqueConstraint('ed_system_id', 'power_id', name='system_power_constraint'),
    )

    # ids
    id = sqla.Column(sqla.Integer, primary_key=True)
    ed_system_id = sqla.Column(sqla.BigInteger, sqla.ForeignKey('systems.ed_system_id'))
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'))
    power_state_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), default=0)

    # info
    income = sqla.Column(sqla.Integer, default=0)
    upkeep_current = sqla.Column(sqla.Integer, default=0)
    upkeep_default = sqla.Column(sqla.Integer, default=0)
    fort = sqla.Column(sqla.Integer, default=0)
    fort_trigger = sqla.Column(sqla.Integer, default=0)
    um = sqla.Column(sqla.Integer, default=0)
    um_trigger = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, default=time.time)
    held_merits = sqla.Column(sqla.Integer, default=0)
    held_updated_at = sqla.Column(sqla.Integer, default=time.time)

    # Relationships
    system = sqla.orm.relationship(
        'System', uselist=False, lazy='select', viewonly=True,
        primaryjoin='foreign(System.ed_system_id) == SpySystem.ed_system_id',
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
        keys = ['id', 'ed_system_id', 'power_id', 'power_state_id',
                'income', 'upkeep_current', 'upkeep_default',
                'fort', 'fort_trigger', 'um', 'um_trigger', 'updated_at']
        kwargs = [f'{key}={getattr(self, key)!r}' for key in keys]

        return f"{self.__class__.__name__}({', '.join(kwargs)})"

    def __str__(self):
        """ A pretty one line to give all information. """
        updated_at = datetime.datetime.utcfromtimestamp(self.updated_at)
        status_text = f"{self.fort}/{self.fort_trigger} | {self.um}/{self.um_trigger}, updated at {updated_at}"
        if self.is_expansion:
            description = f"Expansion for {self.power.text} to {self.system.name}: {status_text}"
        else:
            description = f"{self.power.text} {self.system.name}: {status_text}"

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
    with cogdb.session_scope(cogdb.EDDBSession) as session:
        eddb_power_names_to_id = {power.text: power.id for power in session.query(Power).all()}
        json_powers_to_eddb_id = {
            power_id: eddb_power_names_to_id[power_name]
            for power_id, power_name in POWER_ID_MAP.items()
        }

    return json_powers_to_eddb_id


def load_base_json(base, eddb_session):
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

        for sys_addr, data in bundle['systemAddr'].items():
            kwargs = {
                'ed_system_id': sys_addr,
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
                    filter(SpySystem.ed_system_id == sys_addr,
                           SpySystem.power_id == power_id).\
                    one()
                system.update(**kwargs)
            except sqla.orm.exc.NoResultFound:
                system = SpySystem(**kwargs)
                eddb_session.add(system)
            db_systems += [SpySystem(**kwargs)]

    return db_systems


def load_refined_json(refined, eddb_session):
    """ Load the refined json and parse all information from it.

    Args:
        refined: The refined json to load.
        systems: A map of ed_system_id -> SpySystem objects to be updated with info.

    Returns:
        A dictionary mapping powers by name onto the systems they control and their status.
    """
    updated_at = int(refined["lastModified"])
    json_powers_to_eddb_id = json_powers_to_eddb_map()

    db_objs = []
    for bundle in refined["preparation"]:
        power_id = json_powers_to_eddb_id[bundle['power_id']]
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
            try:
                spyprep = eddb_session.query(SpySystem).\
                    filter(SpySystem.ed_system_id == ed_system_id,
                           SpySystem.power_id == power_id).\
                    one()
                spyprep.merits = merits
                spyprep.updated_at = updated_at
            except sqla.orm.exc.NoResultFound:
                spyprep = SpyPrep(
                    power_id=power_id,
                    ed_system_id=ed_system_id,
                    merits=merits,
                    updated_at=updated_at
                )
                eddb_session.add(spyprep)
            db_objs += [spyprep]

    for bundle, pstate_id in [[refined["gainControl"][0], 64], [refined["fortifyUndermine"][0], 16]]:
        power_id = json_powers_to_eddb_id[bundle['power_id']]
        ed_system_id = bundle['systemAddr']
        kwargs = {
            'power_id': power_id,
            'ed_system_id': ed_system_id,
            'power_state_id': pstate_id,
            'fort': bundle['qtyFor'],
            'um': bundle['qtyAgainst'],
            'updated_at': updated_at,
        }
        try:
            system = eddb_session.query(SpySystem).\
                filter(SpySystem.ed_system_id == ed_system_id,
                       SpySystem.power_id == power_id).\
                one()
            system.update(**kwargs)
        except sqla.orm.exc.NoResultFound:
            system = SpySystem(**kwargs)
            eddb_session.add(system)
        db_objs += [system]

    return db_objs


def process_scrape_data(data_json):
    """Process the scrape data and put it into the db.

    This includes dumping existing tables first before loading.

    Args:
        data_json: The JSON object with all the data.
    """
    empty_tables()

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for powerplay_leader, systems in data_json.items():
            power_id = eddb_session.query(Power).\
                filter(Power.text == powerplay_leader).\
                one().id
            for system_name, system_info in systems.items():
                eddb_system = eddb_session.query(System).\
                    filter(System.name == system_name).\
                    one()
                del system_info['name']
                system_info.update({
                    'ed_system_id': eddb_system.ed_system_id,
                    'power_id': power_id,
                    'power_state_id': 16,  # Assuming controls
                })
                eddb_session.add(SpySystem(**system_info))

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


def main():
    """
    Main function to load the test data during development.
    """
    recreate_tables()
    base_f = pathlib.Path(os.path.join(cog.util.ROOT_DIR, 'tests', 'base.json'))
    refined_f = pathlib.Path(os.path.join(cog.util.ROOT_DIR, 'tests', 'refined.json'))

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        with open(base_f, encoding='utf-8') as fin:
            load_base_json(json.load(fin), eddb_session)

        with open(refined_f, encoding='utf-8') as fin:
            load_refined_json(json.load(fin), eddb_session)

        eddb_session.commit()


SPY_TABLES = [SpyPrep, SpyVote, SpySystem]
# Ensure the tables are created before use when this imported
if cogdb.TEST_DB:
    recreate_tables()
else:
    Base.metadata.create_all(cogdb.engine)


if __name__ == "__main__":
    main()
