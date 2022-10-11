"""
Module to parse and import data from spying squirrel.
"""
import datetime
import json
import os
import pathlib
import re
import time

import sqlalchemy as sqla
import sqlalchemy.exc as sqla_e
from sqlalchemy.ext.hybrid import hybrid_property

import cog.util
import cogdb.eddb
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


class SpyVote(cog.util.TimestampMixin, Base):
    """
    Record current vote by power.
    """
    __tablename__ = 'spy_votes'

    power_id = sqla.Column(sqla.Integer, primary_key=True)
    vote = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, default=time.time, onupdate=time.time)

    # Relationships
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='joined', viewonly=True,
        primaryjoin='foreign(Power.id) == SpyVote.power_id',
    )

    def __repr__(self):
        keys = ['power_id', 'vote', 'updated_at']
        kwargs = [f'{key}={getattr(self, key)!r}' for key in keys]

        return f"{self.__class__.__name__}({', '.join(kwargs)})"

    def __str__(self):
        """ A pretty one line to give all information. """
        return f"{self.power.text}: {self.vote}%, updated at {self.utc_date}"

    def __eq__(self, other):
        return isinstance(other, SpyVote) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}")


class SpyPrep(cog.util.TimestampMixin, Base):
    """
    Store Prep triggers by systems.
    """
    __tablename__ = 'spy_preps'

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

    def __repr__(self):
        keys = ['id', 'power_id', 'ed_system_id', 'merits', 'updated_at']
        kwargs = [f'{key}={getattr(self, key)!r}' for key in keys]

        return f"{self.__class__.__name__}({', '.join(kwargs)})"

    def __str__(self):
        """ A pretty one line to give all information. """
        power_text = "{}".format(self.power.text if self.power else self.power_id)
        system_text = "{}".format(self.system.name if self.system else self.ed_system_id)
        return f"{power_text} {system_text}: {self.merits}, updated at {self.utc_date}"

    def __eq__(self, other):
        return isinstance(other, SpyPrep) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}_{self.ed_system_id}")


class SpySystem(cog.util.TimestampMixin, Base):
    """
    Store the current important information of the system.
    """
    __tablename__ = 'spy_systems'

    __table_args__ = (
        sqla.UniqueConstraint('ed_system_id', 'power_id', name='system_power_constraint'),
    )

    # ids
    id = sqla.Column(sqla.Integer, primary_key=True)
    ed_system_id = sqla.Column(sqla.BigInteger, index=True, nullable=False)
    power_id = sqla.Column(sqla.Integer, nullable=False)
    power_state_id = sqla.Column(sqla.Integer, nullable=False, default=0)

    # info
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

    def __repr__(self):
        keys = ['id', 'ed_system_id', 'power_id', 'power_state_id',
                'income', 'upkeep_current', 'upkeep_default',
                'fort', 'fort_trigger', 'um', 'um_trigger', 'updated_at']
        kwargs = [f'{key}={getattr(self, key)!r}' for key in keys]

        return f"{self.__class__.__name__}({', '.join(kwargs)})"

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
            eddb_system = eddb_session.query(System).\
                filter(System.ed_system_id == ed_system_id).\
                one()
            try:
                spyprep = eddb_session.query(SpyPrep).\
                    filter(SpyPrep.ed_system_id == ed_system_id,
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

    for bundle, pstate_id in [[refined["gainControl"][0], 64], [refined["fortifyUndermine"][0], 16]]:
        power_id = json_powers_to_eddb_id[bundle['power_id']]
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
                filter(SpySystem.ed_system_id == ed_system_id,
                       SpySystem.power_id == power_id).\
                one()
            system.update(**kwargs)
        except sqla.orm.exc.NoResultFound:
            system = SpySystem(**kwargs)
            eddb_session.add(system)
        db_objs += [system]

    return db_objs


def parse_params(input):
    """Generically parse the params object of JSON messages.

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
    """Capabale of parsing the faction news summary.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(input['params'])

    parts = [x for x in info["list"].split(':')]
    info["influence"] = float(parts[1].split('=')[1])
    info["happiness"] = int(re.match(r'.*HappinessBand(\d)', parts[2]).group(1))
    del info["list"]

    return info


def parse_response_trade_goods(input):
    """Capabale of parsing the trade goods available.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(input['params'])
    return [val for key, val in info.items()]


def parse_response_bounties_claimed(input):
    """Capabale of parsing claimed and given bounties.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    return parse_params(input['params'])


def parse_response_top5_bounties(input):
    """Capabale of parsing the top 5 bounties.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    info = parse_params(input['params'])

    # Transform params information into better structure
    result = {
        i: {
            'bountyValue': info[f"bountyValue{i}"],
            'commanderId': info[f"commanderId{i}"],
            'lastLocation': info[f"lastLocation{i}"],
            'name': info[f"name{i}"],
        } for i in range(1, 6)
    }
    result['type'] = info['type']

    return result


def parse_response_traffic_totals(input):
    """Capabale of parsing the top 5 bounties.

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
    for key, val in info.items():
        name, num = val.split('; - ')
        result['by_ship'][name.replace('_NAME', '')[1:].lower()] = int(num)

    return result


def parse_response_power_update(input):
    """Capabale of parsing the power update information.

    Args:
        input: A JSON object to parse.

    Returns: A dictionary of information contained within.
    """
    params = input['params']
    return {
        "power": params[0]["value"],
        "fort": int(params[1]["value"]),
        "um": int(params[2]["value"]),
    }


def load_response_json(response, eddb_session):
    """Capable of fully parsing and processing information in a response JSON.

    Args:
        response: A large JSON object returned from POST.
        eddb_session: A session onto the EDDB database.
    """
    result = {}
    for sys_name, news_info in response.items():
        for entry in news_info['news']:
            type = entry['type']
            parser = PARSER_MAP[type]
            try:
                result[parser['name']] += [parser['func'](entry)]
            except KeyError:
                result[parser['name']] = [parser['func'](entry)]
    result['system'] = result['factions'][0]['system']

    # Prune any lists of 1 element, to not be lists.
    for key in result:
        if isinstance(result[key], list) and len(result[key]) == 1:
            result[key] = result[key][0]

    return result


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
                system_info.update({
                    'ed_system_id': eddb_system.ed_system_id,
                    'power_id': power_id,
                    'power_state_id': 16,  # Assuming controls
                })
                eddb_session.add(SpySystem(**system_info))

    eddb_session.commit()


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
        filter(UMSystem.type == EUMType.control,
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


def update_eddb_factions(eddb_session, fact_info):
    """Bulk update influence values for existing found system faction pairs.
    Changes will be committed

    Args:
        eddb_session: Session onto the EDDB database.
        fact_info: A dictionary with required information, see cogdb.scrape.scrape_all_bgs for format.

    Returns: A list of Influence objects updated or added to db.
    """
    infs = []
    for system_name, info in fact_info.items():
        for faction_name in info['factions']:
            try:
                found = eddb_session.query(Influence).\
                    join(System).\
                    join(Faction, Influence.faction_id == Faction.id).\
                    filter(System.name == system_name,
                        Faction.name == faction_name).\
                    one()
            except sqla_orm.exc.NoResultFound:  # Handle case of not existing record
                try:
                    system_id = eddb_session.query(System.id).\
                        filter(System.name == system_name).\
                        one()[0]
                    faction_id = eddb_session.query(Faction).\
                        filter(Faction.name == faction_name).\
                        one()[0]
                    found = Influence(system_id=system_id, faction_id=faction_id)
                    eddb_session.add(found)
                except sqla_orm.exc.NoResultFound:
                    logging.getLogger(__name__).error("update_eddb_factions: MISSING DB INFO for system or faction: %s, %s" % system_name, faction_name)
            found.influence = info['factions'][faction_name]
            found.updated_at = info['updated_at']
            cogdb.eddb.add_history_influence(eddb_session, found)
            infs += [found]

    eddb_session.commit()
    return infs


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
    base_f = pathlib.Path(os.path.join(cog.util.ROOT_DIR, 'tests', 'cogdb', 'base.json'))
    refined_f = pathlib.Path(os.path.join(cog.util.ROOT_DIR, 'tests', 'cogdb', 'refined.json'))

    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        with open(base_f, encoding='utf-8') as fin:
            load_base_json(json.load(fin), eddb_session)

        with open(refined_f, encoding='utf-8') as fin:
            load_refined_json(json.load(fin), eddb_session)

        eddb_session.commit()

    # FIXME: For testing. Can delete soon.
    #
    #  with cogdb.session_scope(cogdb.Session) as session, cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        #  fall = compare_sheet_fort_systems_to_spy(session, eddb_session)
        #  __import__('pprint').pprint(fall)
        #  fall = compare_sheet_um_systems_to_spy(session, eddb_session)
        #  __import__('pprint').pprint(fall)


SPY_TABLES = [SpyPrep, SpyVote, SpySystem]
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
    Base.metadata.create_all(cogdb.engine)


if __name__ == "__main__":
    main()
