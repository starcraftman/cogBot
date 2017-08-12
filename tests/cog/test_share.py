"""
Test any shared logic
"""
from __future__ import absolute_import, print_function
import os

import mock
import pytest

import cog.exc
import cog.share


def test_throw_argument_parser():
    parser = cog.share.ThrowArggumentParser()
    with pytest.raises(cog.exc.ArgumentHelpError):
        parser.print_help()
    with pytest.raises(cog.exc.ArgumentParseError):
        parser.error('blank')
    with pytest.raises(cog.exc.ArgumentParseError):
        parser.exit()


def test_modformatter_record():
    record = mock.Mock()
    record.__dict__['pathname'] = cog.share.rel_to_abs('cog', 'share.py')
    with pytest.raises(TypeError):
        cog.share.ModFormatter().format(record)
    assert record.__dict__['relmod'] == 'cog/share'


def test_rel_to_abs():
    expect = os.path.join(cog.share.ROOT_DIR, 'data', 'log.yml')
    assert cog.share.rel_to_abs('data', 'log.yml') == expect


def test_get_config():
    assert cog.share.get_config('paths', 'log_conf') == 'data/log.yml'


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
    args = parser.parse_args('!fort --next 5'.split())
    assert args.next == 5


# def test_extract_emoji():
    # message = 'This is a fort message: :Fortifying: do :not touch this.\n:Word:'
    # assert set(cog.share.extract_emoji(message)) == set([':Fortifying:', ':Word:'])
