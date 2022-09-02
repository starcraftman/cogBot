"""
Module to parse and import data from new json source.
"""
import datetime
import time

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm

import cog.exc
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
MAX_SPY_MERITS = 99999


class SpyPrep(Base):
    """
    Store Prep triggers by systems.
    """
    __tablename__ = 'spy_preps'

    header = ["ID", "Power", "System", "Merits"]

    id = sqla.Column(sqla.Integer, primary_key=True)
    system_id = sqla.Column(sqla.Integer, sqla.ForeignKey('systems.id'), primary_key=True)
    power_id = sqla.Column(sqla.Integer, sqla.ForeignKey('powers.id'), primary_key=True)
    merits = sqla.Column(sqla.Integer, default=0)
    updated_at = sqla.Column(sqla.Integer, onupdate=sqla.func.unix_timestamp())

    # Relationships
    system = sqla.orm.relationship(
        'System', uselist=False, lazy='select',
        primaryjoin='foreign(System.id) == SpyPrep.system_id',
    )
    power = sqla.orm.relationship(
        'Power', uselist=False, lazy='select',
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

    def update(self, **kwargs):
        """
        Update the object with expected kwargs.

        kwargs:
            merits: The current merits for the system.
            updated_at: The new date time to set for this update. (Required)

        Raises:
            ValidationFail - The kwargs did not contain updated_at or it was not suitable.
        """
        if 'updated_at' not in kwargs:
            raise cog.exc.ValidationFail("Expected key 'updated_at' is missing.")

        self.updated_at = kwargs['updated_at']
        try:
            self.merits = kwargs['merits']
        except (KeyError, cog.exc.ValidationFail):
            pass

    #  @sqla_orm.validates('merits')
    #  def validate_merits(self, key, value):
        #  try:
            #  if value < 0 or value > MAX_SPY_MERITS:
                #  raise cog.exc.ValidationFail("Bounds check failed for: {} with value {}".format(key, value))
        #  except TypeError:
            #  pass

        #  return value

    #  @sqla_orm.validates('updated_at')
    #  def validate_updated_at(self, key, value):
        #  if not value or not isinstance(value, datetime.datetime) or (self.updated_at and value < self.updated_at):
            #  raise cog.exc.ValidationFail("Date invalid or was older than current value.")

        #  return value


def load_base_json(base):
    """ Load the base json and parse all information from it.

    Args:
        base: The base json to load.

    Returns:
        A dictionary mapping powers by name onto the systems they control and their status.
    """
    systems_by_power = {}
    powers = base['powers']
    for bundle in powers:
        power_name = POWER_ID_MAP[bundle['powerId']]
        sys_state = bundle['state']

        # TODO: Design db objects to hook
        for sys_addr, data in bundle['systemAddr'].items():
            system = {
                'id': sys_addr,
                'state': sys_state,
                'income': data['income'],
                'tAgainst': data['thrAgainst'],
                'tFor': data['thrFor'],
                'upkeep': data['upkeepCurrent'],
                'upkeep_default': data['upkeepDefault'],
            }

            try:
                systems_by_power[power_name] += [system]
            except KeyError:
                systems_by_power[power_name] = [system]


    return systems_by_power


def load_refined_json(refined):
    """ Load the refined json and parse all information from it.

    Args:
        refined: The refined json to load.

    Returns:
        A dictionary mapping powers by name onto the systems they control and their status.
    """
    updated_at = time.time()

    with cogdb.session_scope(cogdb.EDDBSession) as session:
        eddb_power_names_to_id = {power.text: power.id for power in session.query(Power).all()}
        json_powers_to_eddb_id = {
            power_id: eddb_power_names_to_id[power_name]
            for power_id, power_name in POWER_ID_MAP.items()
        }

    db_preps = []
    for bundle in refined["preparation"]:
        power_id = json_powers_to_eddb_id[bundle['power_id']]
        preps = [
            {'power_id': power_id, 'system_id': id, 'merits': total, 'updated_at': updated_at}
            for id, total in bundle['rankedSystems']
        ]
        db_preps += [SpyPrep(**kwargs) for kwargs in preps]

    return db_preps
