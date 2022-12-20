"""
Tests for pvp.schema
"""
import pvp.schema


def test_pvpschema_rank_maps():
    __import__('pprint').pprint(pvp.schema.COMBAT_RANK_TO_VALUE)
    __import__('pprint').pprint(pvp.schema.VALUE_TO_COMBAT_RANK)
