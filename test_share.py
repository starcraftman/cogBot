"""
Test any shared logic
"""
from __future__ import absolute_import, print_function

import pytest

import share


def test_get_config():
    assert share.get_config('secrets', 'sheets', 'json') == '.secrets/sheets.json'


def test_make_parser_throws():
    parser = share.make_parser()
    with pytest.raises(share.ArgumentParseError):
        parser.parse_args(['--help'])


def test_make_parser():
    """
    Simply verify it works, not all parser paths.
    """
    parser = share.make_parser()
    args = parser.parse_args('fort --next --long'.split())
    assert args.long is True
    assert args.next is True
    assert args.func == share.parse_fort
