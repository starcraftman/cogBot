# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Tests for cog.exc
"""
import mock

import cog.exc
import cogdb.schema

from tests.conftest import fake_msg_gears, Channel


def test_cog_exception_reply():
    error = cog.exc.CogException("An exception happened :(", lvl='info')
    assert str(error) == "An exception happened :("


def test_cog_exception_write_log():
    error = cog.exc.CogException("An exception happened :(", lvl='info')

    log = mock.Mock()
    log.info.return_value = None
    msg = fake_msg_gears("I don't like exceptions")
    error.write_log(log, content=msg.content, author=msg.author, channel=msg.channel)
    expect = """
CogException: An exception happened :(
====================
User1 sent I don't like exceptions from Channel: live_hudson/Guild: Gears' Hideout
    Discord ID: 1
    Username: User1#12345
    Hudson on Gears' Hideout"""
    log.info.assert_called_with(expect)


def test_more_one_match():
    error = cog.exc.MoreThanOneMatch('LHS', ['LHS 1', 'LHS 2', 'LHS 3'], 'System')
    expect = """Unable to match exactly one result. Refine the search.

Looked for __LHS__ in Systems. Potentially matched the following:

    - __LHS__ 1
    - __LHS__ 2
    - __LHS__ 3"""
    assert str(error) == expect

    error = cog.exc.MoreThanOneMatch('Channel',
                                     [Channel('Channel 1'), Channel('Channel 2'), Channel('Channel 3')],
                                     'Channel', obj_attr='name')
    expect = """Unable to match exactly one result. Refine the search.

Looked for __Channel__ in Channels. Potentially matched the following:

    - __Channel__ 1
    - __Channel__ 2
    - __Channel__ 3"""
    assert str(error) == expect


def test_no_match():
    error = cog.exc.NoMatch('Cubeo', cogdb.schema.FortSystem.__name__)
    assert str(error) == """No match when one was required. Refine the search.

Looked for for __Cubeo__ in FortSystems."""
    error = cog.exc.NoMatch('Person1', 'person')
    assert str(error) == """No match when one was required. Refine the search.

Looked for for __Person1__ in persons."""


def test_name_collision_error():
    expect = """**Critical Error**
----------------
CMDR "Gears" found in rows [20, 33] of the Cattle Sheet

To Resolve:
    Delete or rename the cmdr in one of these rows
    Then execute `admin scan` to reload the db"""
    error = cog.exc.NameCollisionError('Cattle', 'Gears', [20, 33])
    assert str(error) == expect


def test_emphasize_match():
    result = cog.exc.emphasize_match('match', 'A line that should match somewhere')

    assert result == 'A line that should __match__ somewhere'


def test_log_format():
    msg = fake_msg_gears('Hello world!')
    expect = """User1 sent Hello world! from Channel: live_hudson/Guild: Gears' Hideout
    Discord ID: 1
    Username: User1#12345
    Hudson on Gears' Hideout"""

    log_msg = cog.exc.log_format(content=msg.content, author=msg.author, channel=msg.channel)
    assert log_msg == expect
