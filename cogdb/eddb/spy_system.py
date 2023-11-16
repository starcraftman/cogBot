"""
Store information about the current live information of fort systems and preps of
the powers in the game.
"""
import datetime
import time

import sqlalchemy as sqla
from sqlalchemy.ext.hybrid import hybrid_property

from cogdb.eddb.common import Base, LEN
from cog.util import ReprMixin, TimestampMixin


class SpyPrep(ReprMixin, TimestampMixin, Base):
    """
    Store the live information about a Power's system under preparation for expansion.
    """
    __tablename__ = 'spy_preps'
    _repr_keys = ['id', 'power_id', 'ed_system_id', 'merits', 'updated_at']

    __table_args__ = (
        sqla.UniqueConstraint('ed_system_id', 'power_id', name='system_power_constraint'),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    ed_system_id = sqla.Column(sqla.BigInteger, index=True, nullable=False)
    power_id = sqla.Column(sqla.Integer, nullable=False)

    system_name = sqla.Column(sqla.String(LEN["system"]), index=True)  # Intentional caching for QoL
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
        power_text = self.power.text if self.power else str(self.power_id)
        system_text = self.system.name if self.system else str(self.ed_system_id)
        return f"{power_text} {system_text}: {self.merits}, updated at {self.updated_date}"

    def __eq__(self, other):
        return isinstance(other, SpyPrep) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}_{self.ed_system_id}")


class SpySystem(ReprMixin, TimestampMixin, Base):
    """
    Store the live information about a Power's standard fortification system.
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
    # State ids: 16 control, 32 exploited, 48 contested

    system_name = sqla.Column(sqla.String(LEN["system"]), index=True)  # Intentional caching for QoL
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
    held_updated_at = sqla.Column(sqla.Integer, default=10)  # Intentionally old, force held if queried

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
        status_text = f"{self.fort}/{self.fort_trigger} | {self.um}/{self.um_trigger}, updated at {self.updated_date}"
        power_text = self.power.text if self.power else str(self.power_id)
        system_text = self.system.name if self.system else str(self.ed_system_id)
        if self.is_expansion:
            description = f"Expansion for {power_text} to {system_text}: {status_text}"
        else:
            description = f"{power_text} {system_text}: {status_text}"

        return description

    def __eq__(self, other):
        return isinstance(other, SpySystem) and hash(self) == hash(other)

    def __hash__(self):
        return hash(f"{self.power_id}_{self.ed_system_id}")

    @property
    def held_updated_date(self):
        """The update at date as a naive datetime object."""
        return datetime.datetime.utcfromtimestamp(self.held_updated_at)

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
