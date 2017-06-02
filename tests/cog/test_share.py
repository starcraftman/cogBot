"""
Test any shared logic
"""
from __future__ import absolute_import, print_function

import pytest

import cog.share


@pytest.fixture()
def fort_table():
    """
    Yield fixture returns fort sheet.
    """
    import cogdb.query
    sheet_id = cog.share.get_config('hudson', 'cattle', 'id')
    secrets = cog.share.get_config('secrets', 'sheets')
    sheet = cog.sheets.GSheet(sheet_id, secrets['json'], secrets['token'])

    yield cogdb.query.FortTable(sheet)


def test_get_config():
    assert cog.share.get_config('secrets', 'sheets', 'json') == '.secrets/sheets.json'


def test_make_parser_throws(fort_table):
    parser = cog.share.make_parser(fort_table)
    with pytest.raises(cog.share.ArgumentParseError):
        parser.parse_args(['--help'])


def test_make_parser(fort_table):
    """
    Simply verify it works, not all parser paths.
    """
    parser = cog.share.make_parser(fort_table)
    args = parser.parse_args('fort --next --long'.split())
    assert args.long is True
    assert args.next is True
