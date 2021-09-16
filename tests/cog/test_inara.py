"""
Tests for cog.inara
"""
import os

import discord
try:
    import rapidjson as json
except ImportError:
    import json
import pytest

import cog.inara
import cog.util

from tests.conftest import Message, fake_msg_gears

REASON_INARA = 'Prevent temp inara ban due flooding. To enable, ensure os.environ ALL_TESTS=True'
INARA_TEST = pytest.mark.skipif(not os.environ.get('ALL_TESTS'), reason=REASON_INARA)


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
async def test_inara_api_key_unset(f_bot):
    try:
        old_key = cog.inara.HEADER_PROTO
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
    f_bot.wait_for.async_return_value = (fake_msg_gears('stop'), None)
    f_bot.send_message.async_return_value = fake_msg_gears("A message to send.")
    cog.util.BOT = f_bot
    f_msg = fake_msg_gears('!whois gearsandcogs')
    cmdr = await api.search_with_api('gearsandcogs', f_msg)
    await api.reply_with_api_result(cmdr["req_id"], cmdr["event_data"], f_msg)

    # TODO: Very lazy check :/
    assert "CMDR has been reported to Leadership." in str(f_bot.send_message.call_args)


@pytest.mark.asyncio
async def test_friendly_detector_already_in_kos(f_bot):
    api = cog.inara.InaraApi()
    f_msg = fake_msg_gears('!whois Prozer')
    f_bot.wait_for.async_return_value = True, None
    is_friendly_returned, squad_returned = await api.friendly_detector(None, False, ["test embed"], f_msg)

    assert is_friendly_returned is None and squad_returned is None


@pytest.mark.asyncio
async def test_friendly_detector_canceled(f_bot):
    api = cog.inara.InaraApi()
    f_msg = fake_msg_gears('!whois Prozer')
    emoji_cancel = cog.util.CONF.emojis._no
    f_bot.wait_for.async_return_value = emoji_cancel, None
    cmdr = {"name": "Prozer", "allegiance": "Federation"}
    is_friendly_returned, squad_returned = await api.friendly_detector(cmdr, False, [], f_msg)

    assert is_friendly_returned is None and squad_returned is None


@pytest.mark.asyncio
async def test_friendly_detector_friendly_no_squad(f_bot):
    api = cog.inara.InaraApi()
    f_msg = fake_msg_gears('!whois Prozer')
    emoji_friendly = cog.util.CONF.emojis._friendly
    f_bot.wait_for.async_return_value = emoji_friendly, None
    cmdr = {"name": "Prozer", "allegiance": "Federation"}
    is_friendly_returned, squad_returned = await api.friendly_detector(cmdr, False, [], f_msg)

    assert is_friendly_returned
    assert squad_returned == "Unknown"


@pytest.mark.asyncio
async def test_friendly_detector_hostile_with_squad(f_bot):
    api = cog.inara.InaraApi()
    emoji_hostile = cog.util.CONF.emojis._hostile
    f_bot.wait_for.async_return_value = emoji_hostile, None
    f_msg = fake_msg_gears('!whois Akeno')
    cmdr = {"name": "Akeno", "allegiance": "Empire", "squad": "Test"}
    is_friendly_returned, squad_returned = await api.friendly_detector(cmdr, True, [], f_msg)

    assert not is_friendly_returned
    assert squad_returned == "Test"


@INARA_TEST
@pytest.mark.asyncio
async def test_select_from_multiple_exact(f_bot):
    api = cog.inara.InaraApi()
    # 7 is not guaranteed, based on external inara cmdr names order in results
    f_bot.wait_for.async_return_value = fake_msg_gears('cmdr 9')
    cog.util.BOT = f_bot
    cmdr = await api.search_with_api('gears', fake_msg_gears('!whois gears'))
    assert cmdr["name"] == "GearsandCogs"
    with pytest.raises(KeyError):
        cmdr["otherNamesFound"]  # pylint: disable=pointless-statement


@INARA_TEST
@pytest.mark.asyncio
async def test_select_from_multiple_stop(f_bot):
    api = cog.inara.InaraApi()
    f_bot.wait_for.async_return_value = fake_msg_gears('stop')
    cog.util.BOT = f_bot
    with pytest.raises(cog.exc.CmdAborted):
        await api.search_with_api('gears', fake_msg_gears('!whois gears'))


@pytest.mark.asyncio
async def test_inara_squad_details(f_bot):
    expect = [
        {
            'name': 'Squad Leader',
            'value': '[Extremofire](https://inara.cz/cmdr/12997/)',
            'inline': True
        },
        {'name': 'Allegiance', 'value': 'Empire', 'inline': True},
        {'name': 'Power', 'value': 'Arissa Lavigny-Duval', 'inline': True},
        {
            'name': 'Headquarters',
            'value': '[Carthage [Marker Depot]](https://inara.cz/starsystem/18799/)',
            'inline': True
        },
        {
            'name': 'Minor Faction',
            'value': "unknown",
            'inline': True
        },
        {'name': 'Language', 'value': 'English', 'inline': True}
    ]

    result = [x for x in await cog.inara.inara_squad_parse('https://inara.cz/squadron/85/') if
              x['name'] != 'Squad Age']

    assert result == expect


def test_check_reply():
    with pytest.raises(cog.exc.CmdAborted):
        cog.inara.check_reply(None)

    with pytest.raises(cog.exc.CmdAborted):
        cog.inara.check_reply(Message('!status', None, None, None))

    with pytest.raises(cog.exc.CmdAborted):
        cog.inara.check_reply(Message('stop', None, None, None))

    assert cog.inara.check_reply(Message('2', None, None, None)) == 2

    assert cog.inara.check_reply(Message('cmdr 5', None, None, None)) == 5


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


def test_kos_lookup_cmdr_embeds(session, f_kos):
    results = cog.inara.kos_lookup_cmdr_embeds(session, "good_guy")
    assert len(results) == 2
    assert isinstance(results[0], discord.Embed)
    assert results[0].fields[0].value == 'good_guy'
