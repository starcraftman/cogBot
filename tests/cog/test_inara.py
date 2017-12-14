"""
Tests for cog.inara
"""
from __future__ import absolute_import, print_function
try:
    import simplejson as json
except ImportError:
    import json
import pytest

import cog.inara
import cog.util

from tests.conftest import Message, fake_msg_gears


def test_inara_api_input():
    api_input = cog.inara.InaraApiInput()
    api_input.add_event("fakeEvent", {"fakeKey": "fakeData"})

    actual = json.loads(api_input.serialize())
    events = actual['events'][0]
    del events['eventTimestamp']

    expect = {
        'eventData': {'fakeKey': 'fakeData'},
        'eventName': 'fakeEvent',
    }
    assert events == expect
    assert actual["header"]["appName"] == "CogBot"


@pytest.mark.asyncio
async def test_inara_api_key_unset(f_bot):
    try:
        old_key = cog.inara.API_KEY
        cog.inara.API_KEY = None

        api = cog.inara.InaraApi()
        cog.util.BOT = f_bot
        await api.search_with_api('gearsandcogs',
                                  fake_msg_gears('!whois gearsandcogs'))
        assert "!whois is currently disabled." in str(f_bot.send_message.call_args)
    finally:
        cog.inara.API_KEY = old_key


@pytest.mark.asyncio
async def test_search_with_api(f_bot):
    api = cog.inara.InaraApi()
    f_bot.wait_for_message.async_return_value = fake_msg_gears('stop')
    cog.util.BOT = f_bot
    f_msg = fake_msg_gears('!whois gearsandcogs')
    cmdr = await api.search_with_api('gearsandcogs', f_msg)

    assert cmdr['name'] == "GearsandCogs"
    assert cmdr["req_id"] == 0


@pytest.mark.asyncio
async def test_reply_with_api_result(f_bot):
    api = cog.inara.InaraApi()
    f_bot.wait_for_message.async_return_value = fake_msg_gears('stop')
    cog.util.BOT = f_bot
    f_msg = fake_msg_gears('!whois gearsandcogs')
    cmdr = await api.search_with_api('gearsandcogs', f_msg)
    await api.reply_with_api_result(cmdr["req_id"], cmdr["event_data"], f_msg, False)

    # TODO: Very lazy check :/
    assert 'embed=<discord.embeds.Embed' in str(f_bot.send_message.call_args)


@pytest.mark.asyncio
async def test_select_from_multiple_exact(f_bot):
    api = cog.inara.InaraApi()
    f_bot.wait_for_message.async_return_value = fake_msg_gears('cmdr 2')
    cog.util.BOT = f_bot
    cmdr = await api.search_with_api('gears', fake_msg_gears('!whois gears'))
    assert cmdr["name"] == "GearsandCogs"
    with pytest.raises(KeyError):
        cmdr["otherNamesFound"]


@pytest.mark.asyncio
async def test_select_from_multiple_stop(f_bot):
    api = cog.inara.InaraApi()
    f_bot.wait_for_message.async_return_value = fake_msg_gears('stop')
    cog.util.BOT = f_bot
    with pytest.raises(cog.exc.CmdAborted):
        await api.search_with_api('gears', fake_msg_gears('!whois gears'))


def test_check_reply():
    with pytest.raises(cog.exc.CmdAborted):
        cog.inara.check_reply(None)

    with pytest.raises(cog.exc.CmdAborted):
        cog.inara.check_reply(Message('stop', None, None, None, None))

    with pytest.raises(ValueError):
        cog.inara.check_reply(Message('22', None, None, None, None))

    assert cog.inara.check_reply(Message('cmdr 5', None, None, None, None)) == 5
