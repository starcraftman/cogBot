"""
Test any shared logic
"""
from __future__ import absolute_import, print_function

import pytest

import cog.share


def test_get_config():
    assert cog.share.get_config('secrets', 'sheets', 'json') == '.secrets/sheets.json'


def test_make_parser_throws():
    parser = cog.share.make_parser()
    with pytest.raises(cog.share.ArgumentParseError):
        parser.parse_args(['--help'])


def test_make_parser():
    """
    Simply verify it works, not all parser paths.
    """
    parser = cog.share.make_parser()
    args = parser.parse_args('fort --next --long'.split())
    assert args.long is True
    assert args.next is True
