"""
Tests for local eddb copy
"""
from __future__ import absolute_import, print_function

import cogdb.eddb


# def test_get_shipyard_stations(session, centre_name, sys_dist=15, arrival=1000):
def test_get_shipyard_stations(eddb_session):
    actual = cogdb.eddb.get_shipyard_stations(eddb_session, "Rana")
    assert actual[0] == ['Rana', 0.0, 'Ali Hub', 672]
    assert len(actual) == 9

    actual = cogdb.eddb.get_shipyard_stations(eddb_session, "Rana", 30)
    assert actual[0] == ['Rana', 0.0, 'Ali Hub', 672]
    assert len(actual) == 116

    actual = cogdb.eddb.get_shipyard_stations(eddb_session, "Rana", 15, 50000)
    assert actual[0] == ['Rana', 0.0, 'Ali Hub', 672]
    assert len(actual) == 18
