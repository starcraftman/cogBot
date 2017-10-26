"""
Tests for cog.inara
"""
from __future__ import absolute_import, print_function
import pytest

import cog.inara
import cog.util

from tests.cog.test_actions import Message

SEARCH_EXACT = cog.util.rel_to_abs('tests', 'inara_search.exact')
SEARCH_MANY = cog.util.rel_to_abs('tests', 'inara_search.more_than_one')
CMDR_INFO = cog.util.rel_to_abs('tests', 'inara_cmdr.exact')


@pytest.fixture
def inara_cmdr():
    with open(CMDR_INFO) as fin:
        yield fin.read()


# def check_reply(msg, prefix='!'):
    # """
    # When user responds, validate his response.

    # Response should be form: cmdr x, where x in [1, n)

    # Raises:
        # AbortWhois - Timeout reached or user requested abort.
        # ValueError - Bad message.

    # Returns: Parsed index of cmdrs dict.
    # """
    # # If msg is None, the wait_for_message timed out.
    # if not msg or re.match(r'\s*stop', msg.content):
        # raise AbortWhois('Timeout or user aborted command.')

    # match = re.search(r'\s*cmdr\s+(\d+)', msg.content)
    # if msg.content.startswith(prefix) or not match:
        # raise ValueError('Bad response.\n\nPlease choose with **cmdr x** or **stop**')

    # return match.group(1)


def test_check_reply():
    with pytest.raises(cog.inara.AbortWhois):
        cog.inara.check_reply(None)

    with pytest.raises(cog.inara.AbortWhois):
        cog.inara.check_reply(Message('stop', None, None, None, None))

    with pytest.raises(ValueError):
        cog.inara.check_reply(Message('22', None, None, None, None))
    
    assert cog.inara.check_reply(Message('cmdr 5', None, None, None, None)) == 5

def test_parse_allegiance(inara_cmdr):
    data = {}
    cog.inara.parse_allegiance(inara_cmdr, data)
    assert data['allegiance'] == 'Federation'


def test_parse_assets(inara_cmdr):
    data = {}
    cog.inara.parse_assets(inara_cmdr, data)
    assert data['assets'] == '769,047,359 Cr'


def test_parse_balance(inara_cmdr):
    data = {}
    cog.inara.parse_balance(inara_cmdr, data)
    assert data['balance'] == '762,386,209 Cr'


def test_parse_name(inara_cmdr):
    data = {}
    cog.inara.parse_name(inara_cmdr, data)
    assert data['name'] == 'GearsandCogs'


def test_parse_power(inara_cmdr):
    data = {}
    cog.inara.parse_power(inara_cmdr, data)
    assert data['power'] == 'Zachary Hudson'


def test_parse_profile_picture(inara_cmdr):
    data = {}
    cog.inara.parse_profile_picture(inara_cmdr, data)
    assert data['profile_picture'] == 'https://inara.cz/data/users/64/64860x1235.jpg'


def test_parse_rank(inara_cmdr):
    data = {}
    cog.inara.parse_rank(inara_cmdr, data)
    assert data['rank'] == 'Elite'


def test_parse_role(inara_cmdr):
    data = {}
    cog.inara.parse_role(inara_cmdr, data)
    assert data['role'] == 'Scientist / Space cowboy'


def test_parse_wing(inara_cmdr):
    data = {}
    cog.inara.parse_wing(inara_cmdr, data)
    assert data['wing'] == 'Federal Republican Command'


def test_parse_wing_url(inara_cmdr):
    data = {}
    cog.inara.parse_wing_url(inara_cmdr, data)
    assert data['wing_url'] == 'https://inara.cz/wing/722/'
