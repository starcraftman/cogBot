'''
Provide ability to search commanders on Inara.cz
Lookup can be exact or loose, responds with all relevant CMDR info.

Thanks to CMDR shotwn for the contribution.
Contributed: 20/10/2017
Inara API version: 01/12/2017 - v1

Information on api: https://inara.cz/inara-api-devguide/
IMPORTANT: Request rate hard limited to 25/min, temp ban after 1-2 hours.

HEADER_PROTO contains key and identifying info for each request.
Example of how header should look. Now pulled directly from config.
#  HEADER_PROTO = {
    #  "appName": 'AppName',
    #  "appVersion": AppVersion,
    #  "APIkey": APIKey,
    #  "isDeveloped": True  # Just leae this true.
#  }
TODO: Implement rate limiting here or upstream in actions.
'''
import asyncio
import datetime
import logging
import math
import re
try:
    import rapidjson as json
except ImportError:
    import json

import aiohttp
import bs4
import discord

import cog
import cog.exc
import cog.util
import cogdb

# Disable line too long, pylint: disable=C0301

try:
    HEADER_PROTO = cog.util.get_config('inara', 'proto_header')
except KeyError:
    HEADER_PROTO = None
    logging.getLogger(__name__).\
        warning("!whois inara search disabled. No inara field or api_key in config.yml")
    print("!whois inara search disabled. No inara field or api_key in config.yml")

SITE = 'https://inara.cz'
API_ENDPOINT = SITE + '/inapi/v1/'
API_RESPONSE_CODES = {
    'ok': 200,
    'multiple results': 202,
    'no result': 204,
    'error': 400
}
API_HEADERS = {'content-type': 'application/json'}

PP_COLORS = {
    'Alliance': 0x008000,
    'Empire': 0x3232FF,
    'Federation': 0xB20000,
    'default': 0xDEADBF,
}

KOS_COLORS = {
    'KILL': 0x3232FF,
    'FRIENDLY': 0xB20000,
    'default': 0xDEADBF,
}

COMBAT_RANKS = [
    'Harmless',
    'Mostly Harmless',
    'Novice',
    'Competent',
    'Expert',
    'Master',
    'Dangerous',
    'Deadly',
    'Elite'
]
EMPTY_IMG = "https://upload.wikimedia.org/wikipedia/commons/5/59/Empty.png"
EMPTY_INARA = 'unknown'
INARA_SYSTEM_SEARCH = "https://inara.cz/galaxy-starsystem/?search={}"
INARA_STATION_SEARCH = "https://inara.cz/galaxy-station/?search={}%20[{}]"  # system, station_name
INARA_FACTION_SEARCH = "https://inara.cz/galaxy-minorfaction/?search={}"


class InaraApiInput():
    """
    Inara API input prototype for easily generating requested JSON by Inara.
    """
    def __init__(self):
        self.events = []

    def add_event(self, event_name, event_data):
        """ Add an event to send """
        new_event = {
            "eventName": event_name,
            "eventData": event_data,
            "eventTimestamp": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        self.events.append(new_event)

    def serialize(self):
        """
        Return JSON string to send to API

        Raises:
            InternalException: JSON serialization failed.
        Returns:
            String: API request serialized as JSON.
        """
        send = {
            "header": HEADER_PROTO,
            "events": self.events
        }
        try:
            # do not use aiohttp to dump json for handling exception a bit better.
            return json.dumps(send)
        except TypeError:
            raise cog.exc.InternalException('Inara API input JSON serialization failed.',
                                            lvl='exception')


class InaraApi():
    """
    Inara CMDR lookups done with aiohttp module.
    Each request tracked separately, allows for back and forth with bot on loose match.
    """
    def __init__(self):
        self.req_counter = 0  # count how many searches done with search_in_inara
        self.waiting_messages = {}  # 'Searching in inara.cz' messages. keys are req_id.

    async def delete_waiting_message(self, req_id):  # pragma: no cover
        """ Delete the message which informs user about start of search """
        if req_id in self.waiting_messages:
            await self.waiting_messages[req_id].delete()
            del self.waiting_messages[req_id]

    async def search_with_api(self, cmdr_name, msg, ignore_multiple_match=False):
        """
        Search for a commander on Inara.

        Raises:
            CmdAborted - User let timeout occur or cancelled loose match.
            RemoteError - If response code invalid or remote unreachable.
            InternalException - JSON serialization failed.

        Returns:
            Dictionary in full success.
            None if disabled or not found.
        """
        # keep search disabled if there is no API_KEY
        if not HEADER_PROTO:
            await cog.util.BOT.send_message(msg.channel,
                                            "!whois is currently disabled. Inara API key is not set.")
            return None

        # request id
        req_id = self.req_counter
        self.req_counter += 1 % 1000

        try:
            # inform user about initiating the search.
            self.waiting_messages[req_id] = await cog.util.BOT.send_message(msg.channel,
                                                                            "Searching inara.cz ...")

            api_input = InaraApiInput()
            api_input.add_event("getCommanderProfile", {"searchName": cmdr_name})

            # search for commander
            async with aiohttp.ClientSession() as http:
                async with http.post(API_ENDPOINT, data=api_input.serialize(),
                                     headers=API_HEADERS) as resp:
                    if resp.status != 200:
                        raise cog.exc.RemoteError("Inara search failed. HTTP Response code bad: %s"
                                                  % str(resp.status))

                    response_json = await resp.json(loads=wrap_json_loads)

            # after here many things are unorthodox due api structure.
            # check if api accepted our request.
            r_code = response_json["header"]["eventStatus"]

            # handle rejection.
            if r_code == API_RESPONSE_CODES["error"] or r_code not in API_RESPONSE_CODES.values():
                logging.getLogger(__name__).error("INARA Response Failure: \n%s", response_json)
                raise cog.exc.RemoteError("Inara search failed. See log for details. API Response code bad: %s"
                                          % str(r_code))

            event = response_json["events"][0]
            if event["eventStatus"] == API_RESPONSE_CODES["no result"]:
                await cog.util.BOT.send_message(msg.channel,
                                                "Could not find CMDR **{}**".format(cmdr_name))
                return None

            event_data = event["eventData"]

            # fetch commander name, use userName if there is no commanderName set
            commander_name = event_data.get("commanderName", event_data["userName"])

            # other possible cmdr matches
            cmdrs = event_data.get("otherNamesFound", [])

            # return if exact match and no alternatives or selected
            if commander_name.lower() == cmdr_name.lower() and (
                    not cmdrs or ignore_multiple_match):

                return {
                    "req_id": req_id,
                    "inara_cmdr_url": event_data["inaraURL"],
                    "name": commander_name,
                    "event_data": event_data
                }

            # not an exact match or multiple matches passing, will prompt user a list for selection.
            # list will come up, delete waiting message
            await self.delete_waiting_message(req_id)
            cmdrs.insert(0, commander_name)
            cmdr = await self.select_from_choices(cmdr_name, cmdrs, msg)

            if cmdr == commander_name:
                return {
                    "req_id": req_id,
                    "imara_cmdr_url": event_data["inaraURL"],
                    "name": commander_name,
                    "event_data": event_data
                }

            # selected from otherNamesFound, run it again for selected commander.
            # it will search using returned names, so ignore multiple match this time.
            return await self.search_with_api(cmdr, msg, ignore_multiple_match=True)
        finally:
            # delete waiting message on exception.
            await self.delete_waiting_message(req_id)

        return None

    async def select_from_choices(self, name, cmdrs, msg):
        """
        Present the loosely matched choices and wait for user selection.

        Returns:
            Present choices and await a valid numeric reply.
            None if any of the following true:
                1) Timesout waiting for user response.
                2) Invalid response from user (i.e. text, invalid number).

        Raises:
            CmdAborted - Cmdr either requested abort or failed to respond.
        """
        fmt = '{:' + str(math.ceil(len(cmdrs) / 10)) + '}) {}'
        cmdr_list = [fmt.format(ind, cmdr) for ind, cmdr in enumerate(cmdrs, 1)]

        reply = '\n'.join([
            'No exact match for CMDR **{}**'.format(name),
            'Possible matches:',
            '    ' + '\n    '.join(cmdr_list),
            '\nTo select choice 2 reply with: **2**',
            'To abort, reply: **stop**',
            '\n__This message will delete itself on success or 30s timeout.__',
        ])

        user_select = None
        while True:
            try:
                responses = [await cog.util.BOT.send_message(msg.channel, reply)]
                try:
                    user_select = await cog.util.BOT.wait_for(
                        'message',
                        check=lambda m: m.author == msg.author and m.channel == msg.channel,
                        timeout=30)
                except asyncio.TimeoutError:  # TODO: Temp hack
                    user_select = None
                if user_select:
                    responses += [user_select]

                cmdrs_dict = dict(enumerate(cmdrs, 1))
                key = check_reply(user_select)
                return cmdrs_dict[key]
            except (KeyError, ValueError) as exc:
                if user_select:
                    responses += [await cog.util.BOT.send_message(msg.channel, str(exc))]
            finally:
                asyncio.ensure_future(responses[0].channel.delete_messages(responses))

    async def squad_details(self, event_data, cmdr):
        """
        Fill in wing details when requested.

        Returns:
            A Discord Embed for CMDR's squadron details.
        """
        squad_data = event_data["commanderSquadron"]
        cmdr["squad_rank"] = squad_data.get("squadronMemberRank", cmdr["squad_rank"])
        cmdr["squad_count"] = squad_data.get("squadronMembersCount", cmdr["squad_count"])

        extra = await inara_squad_parse(squad_data['inaraURL'])
        return discord.Embed.from_dict({
            'color': PP_COLORS.get(extra[2]["value"], PP_COLORS['default']),
            'author': {
                'name': "{}'s Squadron".format(cmdr["name"]),
                'icon_url': cmdr["profile_picture"],
            },
            'provider': {
                'name': 'Inara',
                'url': SITE,
            },
            'thumbnail': {
                'url': EMPTY_IMG,
            },
            'title': squad_data['squadronName'],
            'url': squad_data["inaraURL"],
            'footer': {
                'text': "Any unknown fields weren't present or failed to parse. See squad link.",
            },
            "fields": [
                {'name': 'Squad Rank', 'value': cmdr["squad_rank"], 'inline': True},
                {'name': 'Squad Count', 'value': cmdr["squad_count"], 'inline': True},
            ] + extra,
        })

    async def reply_with_api_result(self, req_id, event_data, msg):
        """
        Reply using event_data from Inara API getCommanderProfile.

        """
        # cmdr prototype, only name guaranteed. Others will display if not found.
        # keeping original prototype from regex method.
        # balance and assets are not given from api.
        cmdr = {
            'name': 'ERROR',
            'profile_picture': 'https://inara.cz/images/userportraitback.png',
            'role': EMPTY_INARA,
            'allegiance': EMPTY_INARA,
            'rank': EMPTY_INARA,
            'power': EMPTY_INARA,
            'squad': EMPTY_INARA,
            'squad_rank': EMPTY_INARA,
            'squad_count': EMPTY_INARA,
        }

        map_event = [
            ["name", "userName"],
            ["name", "commanderName"],  # commanderName not always set, fallback to userName
            ["profile_picture", "avatarImageURL"],
            ["role", "preferredGameRole"],
            ["allegiance", "preferredAllegianceName"],
            ["power", "preferredPowerName"],
        ]
        for slot, data_name in map_event:
            received = event_data.get(data_name, cmdr[slot])
            if received:
                cmdr[slot] = received

        # rank, ranks are a List of Dictionaries. try to get combat rank
        if "commanderRanksPilot" in event_data:
            match = next((rank for rank in event_data["commanderRanksPilot"]
                          if rank["rankName"] == "combat"), None)
            if match:
                try:
                    cmdr["rank"] = COMBAT_RANKS[int(match["rankValue"])]
                except (ValueError, KeyError):
                    cmdr["rank"] = 'Unknown Rank'

        embeds = []

        try:
            cmdr["squad"] = event_data["commanderSquadron"].get("squadronName", cmdr["squad"])
            embeds += [await self.squad_details(event_data, cmdr)]
        except KeyError:
            pass

        cmdr_embed = discord.Embed.from_dict({
            'color': PP_COLORS.get(cmdr["allegiance"], PP_COLORS['default']),
            'author': {
                'name': cmdr["name"],
                'icon_url': cmdr["profile_picture"],
            },
            'provider': {
                'name': 'Inara',
                'url': SITE,
            },
            'thumbnail': {
                'url': cmdr['profile_picture']
            },
            'title': "Commander Profile",
            'url': event_data["inaraURL"],
            "fields": [
                {'name': 'Allegiance', 'value': cmdr["allegiance"], 'inline': True},
                {'name': 'Role', 'value': cmdr["role"], 'inline': True},
                {'name': 'Combat Rank', 'value': cmdr["rank"], 'inline': True},
                {'name': 'Squadron', 'value': cmdr["squad"], 'inline': True},
            ],
        })

        embeds = [cmdr_embed] + embeds

        kos_cmdrs = cogdb.query.kos_search_cmdr(cogdb.Session(), cmdr['name'])
        for kos in kos_cmdrs[:3]:
            embeds += [discord.Embed.from_dict({
                'color': KOS_COLORS.get(kos.friendly, KOS_COLORS['default']),
                'author': {
                    'name': "KOS Finder",
                    'icon_url': cmdr["profile_picture"],
                },
                "fields": [
                    {'name': 'Name', 'value': kos.cmdr, 'inline': True},
                    {'name': 'Reg Squadron', 'value': kos.faction, 'inline': True},
                    {'name': 'Is Friendly ?', 'value': kos.friendly, 'inline': True},
                ],
            })]

        futs = [cog.util.BOT.send_message(msg.channel, embed=embed) for embed in embeds]
        futs += [self.delete_waiting_message(req_id)]
        for fut in futs:
            await fut


def check_reply(msg, prefix='!'):
    """
    When user responds, validate his response.

    Response should be form: cmdr x, where x in [1, n)

    Raises:
        CmdAborted - Timeout reached or user requested abort.
        ValueError - Bad message.

    Returns: Parsed index of cmdrs dict.
    """
    # If msg is None, the wait_for_message timed out.
    if not msg or re.match(r'\s*stop\s*', msg.content.lower()) or msg.content.startswith(prefix):
        raise cog.exc.CmdAborted('Timeout or user aborted command.')

    match = re.search(r'\s*(\d+)\s*', msg.content.lower())
    if not match:
        raise ValueError('Bad response.\n\nPlease choose a **number** or **stop**')

    return int(match.group(1))


def wrap_json_loads(string):
    """ Loads JSON. Make aiohttp use this function for custom exceptions. """
    try:
        return json.loads(string)
    except TypeError:
        raise cog.exc.RemoteError('Inara API responded with bad JSON.')


api = InaraApi()  # use as module, needs "bot" to be set. pylint: disable=C0103


async def inara_squad_parse(url):
    """
    Fetch information directly from the squadron page to supplement the missing info
    from the inara api.

    Args:
        url: The link to a squardron's page.

    Returns: A dict with information that can be added to squad details.
    """
    squad_data = {
        "allegiance": EMPTY_INARA,
        "power": EMPTY_INARA,
        "language": EMPTY_INARA,
        "age": EMPTY_INARA,
        "hq": EMPTY_INARA,
        "leader": EMPTY_INARA,
        "minor": EMPTY_INARA,
    }
    async with aiohttp.ClientSession() as http:
        async with http.get(url) as resp:
            soup = bs4.BeautifulSoup(await resp.text(), 'html.parser')

    for ele in soup.find('div', 'sidecontent2').find('div', 'mainblock'):
        ele_str = str(ele)
        if not ele_str:
            continue

        if 'Allegiance:</span>' in ele_str and ele.nextSibling.strip():
            squad_data['allegiance'] = ele.nextSibling.strip()
        elif 'Power:</span>' in ele_str and ele.nextSibling.strip():
            squad_data['power'] = ele.nextSibling.string.strip()
        elif 'Language:</span>' in ele_str and ele.nextSibling.strip():
            squad_data['language'] = ele.nextSibling.strip()
        elif 'Squadron age:</span>' in ele_str and ele.nextSibling.strip():
            squad_data['age'] = ele.nextSibling.strip()
        elif 'Headquarters:</span>' in ele_str and ele.nextSibling.nextSibling.string:
            node = ele.nextSibling.nextSibling
            squad_data['hq'] = "[{}]({})".format(node.string.strip(), SITE + node['href'])
        elif 'Squadron commander:</span>' in ele_str and ele.nextSibling.nextSibling.string:
            node = ele.nextSibling.nextSibling
            squad_data['leader'] = "[{}]({})".format(node.string.strip(), SITE + node['href'])
        elif 'Minor faction:</span>' in ele_str and ele.nextSibling.nextSibling.string:
            node = ele.nextSibling.nextSibling
            squad_data['minor'] = "[{}]({})".format(node.string.strip(), SITE + node['href'])

    return [
        {'name': 'Squad Leader', 'value': squad_data["leader"], 'inline': True},
        {'name': 'Squad Age', 'value': squad_data["age"], 'inline': True},
        {'name': 'Allegiance', 'value': squad_data["allegiance"], 'inline': True},
        {'name': 'Power', 'value': squad_data["power"], 'inline': True},
        {'name': 'Headquarters', 'value': squad_data["hq"], 'inline': True},
        {'name': 'Minor Faction', 'value': squad_data["minor"], 'inline': True},
        {'name': 'Language', 'value': squad_data["language"], 'inline': True},
    ]


def extract_inara_systems(message):
    """
    Take a message (str or discord.Message object) and extract all possible
    Inara systems references based on an expected format.

    Returns: (sys_list, faction_list)
        sys_list: A list of form: ((system_name, inara_url), ...)
        faction_list: A list of form: ((faction_name, inara_url), ...)
    """
    text = message
    if isinstance(message, discord.Message):
        text = message.content

    faction_list, sys_list = [], []
    for mat in re.finditer(r'([0-9].\s+)?(?P<sys>.+) (<:(Small|Large).*?:[0-9]*>)(.*for\s+(?P<fact>[a-zA-z0-9\' -]+))?', text):
        sys_name = mat.group('sys').strip()
        link = INARA_SYSTEM_SEARCH.format(sys_name.replace(' ', '%20'))
        sys_list += [(sys_name, link)]

        if mat.group('fact'):
            fact_name = mat.group('fact').strip()
            link = INARA_FACTION_SEARCH.format(fact_name.replace(' ', '%20'))
            faction_list += [(fact_name, link)]

    return sys_list, faction_list


def generate_bgs_embed(sys_list, faction_list):
    """
    Generate an embed with links required based on input lists.

    Returns: A discord Embed.
    """
    fields = []
    for sys, sys_link in sys_list:
        fields += [{'name': "System", 'value': "[{}]({})".format(sys, sys_link), "inline": True}]
    for fact, fact_link in faction_list:
        fields += [{'name': "Faction", 'value': "[{}]({})".format(fact, fact_link), "inline": True}]

    return discord.Embed.from_dict({
        'color': PP_COLORS.get("Federation"),
        'author': {
            'name': "Cog"
        },
        'provider': {
            'name': 'Inara',
            'url': SITE,
        },
        'thumbnail': {
            'url': EMPTY_IMG,
        },
        'title': "Inara System & Faction Links",
        'url': SITE,
        'footer': {
            'text': "Please report any broken or missing links."
        },
        "fields": fields
    })


def kos_report_cmdr_embed(reporter, cmdr, faction, reason, is_friendly=False):
    """
    Return an embed that be used to inform of a report.

    Returns: A discord embed.
    """
    kill = "FRIENDLY" if is_friendly else "KILL"

    return discord.Embed.from_dict({
        'color': KOS_COLORS[kill],
        'author': {
            'name': reporter,
        },
        'provider': {
            'name': 'Cog',
        },
        'thumbnail': {
            'url': EMPTY_IMG,
        },
        'title': "KOS Report",
        'footer': {
            'text': "Review this information and use thumbs to decide if allowed.",
        },
        "fields": [
            {'name': 'cmdr', 'value': cmdr, 'inline': True},
            {'name': 'faction', 'value': faction, 'inline': True},
            {'name': 'kill', 'value': kill, 'inline': True},
            {'name': 'reason', 'value': reason, 'inline': False},
        ],
    })


def main():
    import sys
    loop = asyncio.new_event_loop()
    for n in sys.argv[1:]:
        loop.run_until_complete(inara_squad_parse('https://inara.cz/squadron/{}/'.format(n)))


if __name__ == "__main__":
    main()
