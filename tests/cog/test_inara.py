"""
Tests for cog.inara
"""
from __future__ import absolute_import, print_function
import aiomock
import mock
import pytest

import cog.inara
import cog.util

from tests.conftest import Message, fake_msg_gears

SEARCH_EXACT = cog.util.rel_to_abs('tests', 'inara_search.exact')
SEARCH_MANY = cog.util.rel_to_abs('tests', 'inara_search.more_than_one')
CMDR_INFO = cog.util.rel_to_abs('tests', 'inara_cmdr.exact')


@pytest.fixture
def inara_cmdr():
    with open(CMDR_INFO) as fin:
        yield fin.read()


@pytest.mark.asyncio
async def test_login_inara_bad_url():
    try:
        old_site = cog.inara.SITE_LOGIN
        cog.inara.SITE_LOGIN = 'https://www.google.com'
        with pytest.raises(cog.exc.RemoteError):
            api = cog.inara.InaraApi()
            await api.login_to_inara()
    finally:
        cog.inara.SITE_LOGIN = old_site
        api.http.close()


@pytest.mark.asyncio
async def test_login_inara_bad_credentials():
    # I expect this to never be valid
    bad_creds = {'formact': 'ENT_LOGIN', 'location': 'intro', 'loginid': 'invalid_user', 'loginpass': 'invalid_pass!'}
    try:
        with mock.patch('cog.util.get_config') as mock_config:
            mock_config.return_value = bad_creds
            with pytest.raises(cog.exc.RemoteError):
                api = cog.inara.InaraApi()
                await api.login_to_inara()
    finally:
        api.http.close()


@pytest.mark.asyncio
async def test_search_bad_response(f_bot):
    fake_http = aiomock.AIOMock()
    fake_http.get.async_return_value = mock.Mock(status=404)
    with pytest.raises(cog.exc.RemoteError):
        api = cog.inara.InaraApi()
        api.bot = f_bot
        api.http.close()
        api.http = fake_http
        await api.login_to_inara()
        await api.search_in_inara('gearsandcogs', fake_msg_gears('!whois gearsandcogs'))


@pytest.mark.asyncio
async def test_search_response_no_login(f_bot):
    try:
        api = cog.inara.InaraApi()
        api.bot = f_bot
        cmdr = await api.search_in_inara('gearsandcogs', fake_msg_gears('!whois gearsandcogs'))
        assert cmdr == {'name': 'GearsandCogs', 'req_id': 1, 'url': 'https://inara.cz/cmdr/64860/'}
    finally:
        api.http.close()


@pytest.mark.asyncio
async def test_search_response_logged_in(f_bot):
    try:
        api = cog.inara.InaraApi()
        api.bot = f_bot
        await api.login_to_inara()
        cmdr = await api.search_in_inara('gearsandcogs', fake_msg_gears('!whois gearsandcogs'))
        assert cmdr == {'name': 'GearsandCogs', 'req_id': 0, 'url': 'https://inara.cz/cmdr/64860/'}
    finally:
        api.http.close()


@pytest.mark.asyncio
async def test_select_from_multiple_exact(f_bot):
    try:
        api = cog.inara.InaraApi()
        f_bot.wait_for_message.async_return_value = fake_msg_gears('cmdr 2')
        api.bot = f_bot
        await api.login_to_inara()
        cmdr = await api.search_in_inara('gears', fake_msg_gears('!whois gears'))
        assert cmdr == {'name': 'GearsandCogs', 'req_id': 0, 'url': 'https://inara.cz/cmdr/64860/'}
    finally:
        api.http.close()


@pytest.mark.asyncio
async def test_select_from_multiple_stop(f_bot):
    try:
        api = cog.inara.InaraApi()
        f_bot.wait_for_message.async_return_value = fake_msg_gears('stop')
        api.bot = f_bot
        await api.login_to_inara()
        with pytest.raises(cog.exc.CmdAborted):
            await api.search_in_inara('gears', fake_msg_gears('!whois gears'))
    finally:
        api.http.close()


@pytest.mark.asyncio
async def test_fetch_from_cmdr_page(f_bot):
    try:
        api = cog.inara.InaraApi()
        f_bot.wait_for_message.async_return_value = fake_msg_gears('stop')
        api.bot = f_bot
        await api.login_to_inara()
        cmdr = {'name': 'GearsandCogs', 'req_id': 0, 'url': 'https://inara.cz/cmdr/64860/'}
        await api.fetch_from_cmdr_page(cmdr, fake_msg_gears('!whois gears'))
        # TODO: Very lazy check :/
        assert 'embed=<discord.embeds.Embed' in str(f_bot.send_message.call_args)
    finally:
        api.http.close()


def test_check_reply():
    with pytest.raises(cog.exc.CmdAborted):
        cog.inara.check_reply(None)

    with pytest.raises(cog.exc.CmdAborted):
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
