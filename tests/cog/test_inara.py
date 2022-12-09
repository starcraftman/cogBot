# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Tests for cog.inara
"""
import asyncio
import os

import aiomock
import discord
try:
    import rapidjson as json
except ImportError:
    import json
import pytest

import cog.inara
import cog.util

from tests.conftest import Interaction, fake_msg_gears, fake_msg_newuser

REASON_INARA = 'Prevent temp inara ban due flooding. To enable, ensure os.environ ALL_TESTS=True'
INARA_TEST = pytest.mark.skipif(not os.environ.get('ALL_TESTS'), reason=REASON_INARA)


def mock_inter(values):
    inter = aiomock.AIOMock(values=values, sent=[])
    inter.component.label = inter.values[0]
    inter.sent = []

    async def send_(resp):
        inter.sent += [resp]
    inter.send = send_

    return inter


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
    assert actual["header"]["APIkey"].startswith("3")


@pytest.mark.asyncio
async def test_rate_limiter_increment(f_bot):
    msg = fake_msg_gears("!fort")
    rate_limit = cog.inara.RateLimiter(max_rate=12, resume_rate=9)

    await rate_limit.increment(f_bot, msg.channel)
    assert rate_limit.rate == 1


@pytest.mark.asyncio
async def test_rate_limiter_increment_wait(f_bot):
    msg = fake_msg_gears("!fort")
    rate_limit = cog.inara.RateLimiter(max_rate=2, resume_rate=1)

    await rate_limit.increment(f_bot, msg.channel)
    assert rate_limit.rate == 1
    asyncio.ensure_future(rate_limit.increment(f_bot, msg.channel))
    await asyncio.sleep(1)
    assert rate_limit.rate == 2


@pytest.mark.asyncio
async def test_rate_limiter_decrement(f_bot):
    msg = fake_msg_gears("!fort")
    rate_limit = cog.inara.RateLimiter(max_rate=12, resume_rate=9)
    await rate_limit.increment(f_bot, msg.channel)
    assert rate_limit.rate == 1

    await rate_limit.decrement(delay=1)
    assert rate_limit.rate == 0


@pytest.mark.asyncio
async def test_rate_limiter_decrement_wait(f_bot):
    rate_limit = cog.inara.RateLimiter(max_rate=2, resume_rate=1)
    msg = fake_msg_gears("!fort")

    async def run_test():
        await rate_limit.increment(f_bot, msg.channel)
        await rate_limit.decrement(delay=2)

    await asyncio.gather(
        run_test(),
        run_test(),
    )
    assert rate_limit.rate == 0


@pytest.mark.asyncio
async def test_inara_api_key_unset(f_bot):
    old_key = cog.inara.HEADER_PROTO
    try:
        cog.inara.HEADER_PROTO = None

        api = cog.inara.InaraApi()
        cog.util.BOT = f_bot
        await api.search_with_api('gearsandcogs',
                                  fake_msg_gears('!whois gearsandcogs'))
        assert "!whois is currently disabled." in str(f_bot.send_message.call_args)
    finally:
        cog.inara.HEADER_PROTO = old_key


@INARA_TEST
@pytest.mark.asyncio
async def test_search_inara_and_kos_match_exact(f_bot):
    api = cog.inara.InaraApi()
    cog.util.BOT = f_bot
    f_bot.wait_for.async_side_effect = [mock_inter([cog.inara.BUT_CANCEL])]

    msg = fake_msg_gears('!whois gearsandcogs')
    result = await api.search_inara_and_kos('gearsandcogs', msg)

    expect = {
        'add': False,
        'cmdr': 'GearsandCogs',
        'is_friendly': False,
        'reason': 'unknown',
        'squad': 'Federal Republic Command - Hudson Powerplay'
    }
    assert result == expect


@INARA_TEST
@pytest.mark.asyncio
async def test_search_inara_and_kos_match_select(f_bot):
    api = cog.inara.InaraApi()
    cog.util.BOT = f_bot
    f_bot.wait_for.async_side_effect = [mock_inter(['Gearsprud']), mock_inter([cog.inara.BUT_FRIENDLY])]

    msg = fake_msg_gears('!whois gearsandcogs')
    result = await api.search_inara_and_kos('gears', msg)

    expect = {
        'add': True,
        'cmdr': 'Gearsprud',
        'is_friendly': True,
        'reason': 'Manual report after a !whois in Channel: live_hudson by cmdr Member: User1',
        'squad': 'unknown'
    }
    assert result == expect


@INARA_TEST
@pytest.mark.asyncio
async def test_search_inara_and_kos_no_match(f_bot):
    api = cog.inara.InaraApi()
    cog.util.BOT = f_bot
    f_bot.wait_for.async_side_effect = [mock_inter([cog.inara.BUT_HOSTILE])]

    msg = fake_msg_gears('!whois gearsandcogs')
    result = await api.search_inara_and_kos('notInInaraForSure', msg)

    expect = {
        'add': True,
        'cmdr': 'notInInaraForSure',
        'is_friendly': False,
        'reason': 'Manual report after a !whois in Channel: live_hudson by cmdr Member: User1',
        'squad': 'unknown'
    }
    assert result == expect


@INARA_TEST
@pytest.mark.asyncio
async def test_search_with_api(f_bot):
    api = cog.inara.InaraApi()
    f_bot.wait_for.async_return_value = fake_msg_gears('stop')
    cog.util.BOT = f_bot
    f_msg = fake_msg_gears('!whois gearsandcogs')
    cmdr = await api.search_with_api('gearsandcogs', f_msg)

    assert cmdr['name'] == "GearsandCogs"
    assert cmdr["req_id"] == 0


@INARA_TEST
@pytest.mark.asyncio
async def test_reply_with_api_result(f_bot):
    api = cog.inara.InaraApi()
    msg = fake_msg_gears('stop')
    comp = aiomock.Mock()
    comp.label = cog.inara.BUT_CANCEL
    f_bot.wait_for.async_return_value = Interaction('reply_api', message=msg, user=msg.author, component=comp)

    f_bot.send_message.async_return_value = fake_msg_gears("A message to send.")
    cog.util.BOT = f_bot
    f_msg = fake_msg_gears('!whois gearsandcogs')
    cmdr = await api.search_with_api('gearsandcogs', f_msg)
    await api.reply_with_api_result(cmdr["req_id"], cmdr["event_data"], f_msg)

    # TODO: Very lazy check :/
    assert 'Should the CMDR GearsandCogs be added as friendly or hostile?' in str(f_bot.send_message.call_args)


@INARA_TEST
@pytest.mark.asyncio
async def test_select_from_multiple_exact(f_bot):
    api = cog.inara.InaraApi()
    msg = fake_msg_gears('stop')
    f_bot.wait_for.async_side_effect = [
        Interaction('reply_api', message=msg, user=msg.author, comp_label='GearsandCogs'),
        Interaction('reply_api', message=msg, user=msg.author, comp_label=cog.inara.BUT_DENY),
    ]
    cog.util.BOT = f_bot
    cmdr = await api.search_with_api('gears', fake_msg_gears('!whois gears'))
    assert cmdr["name"] == "GearsandCogs"
    with pytest.raises(KeyError):
        cmdr["otherNamesFound"]  # pylint: disable=pointless-statement


@INARA_TEST
@pytest.mark.asyncio
async def test_select_from_multiple_stop(f_bot):
    api = cog.inara.InaraApi()
    msg = fake_msg_gears(cog.inara.BUT_CANCEL)
    f_bot.wait_for.async_return_value = Interaction('multiple_stop', message=msg, user=msg.author, comp_label=cog.inara.BUT_CANCEL)
    cog.util.BOT = f_bot
    with pytest.raises(cog.exc.CmdAborted):
        await api.search_with_api('gears', fake_msg_gears('!whois gears'))


# TODO Failing test only on github CI, cause unknown. Low priority.
@INARA_TEST
@pytest.mark.asyncio
async def test_inara_squad_details(f_bot):
    expect = [
        {
            'name': 'Squad Leader',
            'value': '[St Michael](https://inara.cz/elite/cmdr/138041/)',
            'inline': True
        },
        {'name': 'Allegiance', 'value': 'Empire', 'inline': True},
        {'name': 'Power', 'value': 'Arissa Lavigny-Duval', 'inline': True},
        {
            'name': 'Headquarters',
            'value': '[Carthage [Marker Depot]](https://inara.cz/elite/starsystem/18799/)',
            'inline': True
        },
        {
            'name': 'Minor Faction',
            'value': "[Lavigny's Legion](https://inara.cz/elite/minorfaction/19129/)",
            'inline': True
        },
        {'name': 'Language', 'value': 'English', 'inline': True}
    ]

    result = [x for x in await cog.inara.inara_squad_parse('https://inara.cz/elite/squadron/85/') if
              x['name'] != 'Squad Age']

    assert result == expect


def test_check_interaction_response(f_dusers, f_admins):
    message = fake_msg_gears('hello')
    message2 = fake_msg_newuser('goodbye')
    inter = Interaction('inter1', user=message2.author, message=message2)

    # Same everything
    assert cog.inara.check_interaction_response(message2.author, message2, inter)

    # Different messages, always reject
    assert not cog.inara.check_interaction_response(message.author, message, inter)

    # Same message, author is admin
    inter = Interaction('inter1', user=message.author, message=message2)
    assert cog.inara.check_interaction_response(message2.author, message2, inter)


def test_extract_inara_systems():
    msg = """
CRITICAL Systems
:CombatMission: :CombatBond: :Bounties: (keep killing the enemy ships in the Conflict Zones until the battle is won)
Bragpura <:Small-1:3210582> :Planetary: fight for Lords of Totjob'al (Control War, Day 1 -*Close Victory* 1-0)
V590 Lyrae <:Small-1:3210582> :Planetary: fight for Tai Qing Alliance Bond (Control War, Day 1 -*Close Victory* 1-0) :FC:

:Mission: :Exploration: :Bounties: :Commodity:
Tamba <:Large-1:3521029> for HR 7012 Noblement :FC:

Priority Systems
:Mission: :Exploration: :Bounties: :Commodity:
Juma <:Small-1:3210582> Moseley Settlement for Juma Aristocrats
Slavanibo <:Large-1:3521029> :Planetary: for Marquis du Slavanibo
Ticushpakhi <:Small-1:3210582> :Planetary: Any Station for Dukes of Biaris
1. Wardal <:Small-1:3210582> Any Station for Noblemen of Nohock Ek
Alone <:Small-1:3210582> Any Station
    """
    expect = ([
        ('Bragpura', 'https://inara.cz/galaxy-starsystem/?search=Bragpura'),
        ('V590 Lyrae', 'https://inara.cz/galaxy-starsystem/?search=V590%20Lyrae'),
        ('Tamba', 'https://inara.cz/galaxy-starsystem/?search=Tamba'),
        ('Juma', 'https://inara.cz/galaxy-starsystem/?search=Juma'),
        ('Slavanibo', 'https://inara.cz/galaxy-starsystem/?search=Slavanibo'),
        ('Ticushpakhi', 'https://inara.cz/galaxy-starsystem/?search=Ticushpakhi'),
        ('Wardal', 'https://inara.cz/galaxy-starsystem/?search=Wardal'),
        ('Alone', 'https://inara.cz/galaxy-starsystem/?search=Alone'),
    ], [
        ("Lords of Totjob'al",
            "https://inara.cz/galaxy-minorfaction/?search=Lords%20of%20Totjob'al"),
        ('Tai Qing Alliance Bond',
            'https://inara.cz/galaxy-minorfaction/?search=Tai%20Qing%20Alliance%20Bond'),
        ('HR 7012 Noblement',
            'https://inara.cz/galaxy-minorfaction/?search=HR%207012%20Noblement'),
        ('Juma Aristocrats',
            'https://inara.cz/galaxy-minorfaction/?search=Juma%20Aristocrats'),
        ('Marquis du Slavanibo',
            'https://inara.cz/galaxy-minorfaction/?search=Marquis%20du%20Slavanibo'),
        ('Dukes of Biaris',
            'https://inara.cz/galaxy-minorfaction/?search=Dukes%20of%20Biaris'),
        ('Noblemen of Nohock Ek',
            'https://inara.cz/galaxy-minorfaction/?search=Noblemen%20of%20Nohock%20Ek'),
    ])

    assert cog.inara.extract_inara_systems(msg) == expect


def test_generate_bgs_embed():
    systems = [('System1', 'System1'), ('System2', 'System2')]
    factions = [('Faction1', 'Faction1')]
    embed = cog.inara.generate_bgs_embed(systems, factions)

    field1 = embed.fields[0]
    assert field1.value == "[System1](System1)"


def test_kos_lookup_cmdr_embeds(session, f_kos):
    results = cog.inara.kos_lookup_cmdr_embeds(session, "good_guy")
    assert len(results) == 2
    assert isinstance(results[0], discord.Embed)
    assert results[0].fields[0].value == 'good_guy'


def test_wrap_json_loads():
    with pytest.raises(cog.exc.RemoteError):
        cog.inara.wrap_json_loads(None)
