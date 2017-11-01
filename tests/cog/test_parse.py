"""
Test any shared logic
"""
from __future__ import absolute_import, print_function

import pytest

import cog.exc
import cog.parse
import cog.util


def test_throw_argument_parser():
    parser = cog.parse.ThrowArggumentParser()
    with pytest.raises(cog.exc.ArgumentHelpError):
        parser.print_help()
    with pytest.raises(cog.exc.ArgumentParseError):
        parser.error('blank')
    with pytest.raises(cog.exc.ArgumentParseError):
        parser.exit()


def test_make_parser_throws():
    parser = cog.parse.make_parser('!')
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
    parser = cog.parse.make_parser('!')
    args = parser.parse_args('!fort --next 5'.split())
    assert args.next == 5
