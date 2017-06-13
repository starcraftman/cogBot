"""
Test any shared logic
"""
from __future__ import absolute_import, print_function

import pytest

import cog.exc
import cog.share


def test_dict_to_columns():
    data = {
        'first': [1, 2, 3],
        'more': [100],
        'second': [10, 30, 50],
        'three': [22, 19, 26, 23],
    }
    expect = [
        ['first (3)', 'more (1)', 'second (3)', 'three (4)'],
        [1, 100, 10, 22],
        [2, '', 30, 19],
        [3, '', 50, 26],
        ['', '', '', 23]
    ]
    assert cog.share.dict_to_columns(data) == expect


def test_get_config():
    assert cog.share.get_config('paths', 'log_conf') == 'data/log.yml'


def test_make_parser_throws():
    parser = cog.share.make_parser('!')
    with pytest.raises(cog.exc.ArgumentParseError):
        parser.parse_args(['!not_cmd'])
    with pytest.raises(cog.exc.ArgumentHelpError):
        parser.parse_args('!fort --help'.split())
    with pytest.raises(cog.exc.ArgumentParseError):
        parser.parse_args('!fort --invalidflag'.split())


def test_make_parser():
    """
    Simply verify it works, not all parser paths.
    """
    parser = cog.share.make_parser('!')
    args = parser.parse_args('!fort --next --long'.split())
    assert args.long is True
    assert args.next is True
