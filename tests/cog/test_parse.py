"""
Test any shared logic
"""
import pytest

import cog.exc
import cog.parse
import cog.util
from cogdb.schema import EVoteType


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


def test_parse_vote_tuple():
    """
    Multiple cases here due to simplicity of function.
    """
    with pytest.raises(cog.parse.ThrowArggumentParser):
        cog.parse.parse_vote_tuple(['wrong', 'wrong', 'wrong'])

    with pytest.raises(cog.parse.ThrowArggumentParser):
        cog.parse.parse_vote_tuple(['wrong', 'wrong'])

    with pytest.raises(cog.parse.ThrowArggumentParser):
        cog.parse.parse_vote_tuple(['wrong', 'prep'])

    with pytest.raises(cog.parse.ThrowArggumentParser):
        cog.parse.parse_vote_tuple(['cons', 'wrong'])

    with pytest.raises(cog.parse.ThrowArggumentParser):
        cog.parse.parse_vote_tuple(['cons', '-5'])

    assert cog.parse.parse_vote_tuple(['cons', '5']) == (EVoteType.cons, 5)
    assert cog.parse.parse_vote_tuple(['1', 'prep']) == (EVoteType.prep, 1)
