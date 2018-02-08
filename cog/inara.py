'''
Provide ability to search commanders on Inara.cz
Lookup can be exact or loose, responds with all relevant CMDR info.

Thanks to CMDR shotwn for the contribution.
Contributed: 20/10/2017
Inara API version: 01/12/2017 - v1
'''
import asyncio
import datetime
import logging
import math
import re
try:
    import simplejson as json
except ImportError:
    import json

import aiohttp
import discord

import cog
import cog.exc
import cog.util

# Disable line too long, pylint: disable=C0301

try:
    API_KEY = cog.util.get_config('inara', 'api_key')
except KeyError:
    logging.getLogger('cog.inara').\
        warning("!whois inara search disabled. No inara field or api_key in config.yml")
    print("!whois inara search disabled. No inara field or api_key in config.yml")
    API_KEY = None

SITE = 'https://inara.cz'
API_ENDPOINT = SITE + '/inapi/v1/'
API_RESPONSE_CODES = {
    'ok': 200,
    'multiple results': 202,
    'no result': 204,
    'error': 400
}
API_HEADERS = {'content-type': 'application/json'}
# Prototype for all json header sections
HEADER_PROTO = {
    "appName": 'CogBot',
    "appVersion": cog.__version__,
    "APIkey": API_KEY,
    "isDeveloped": True
}

PP_COLORS = {
    'Alliance': 0x008000,
    'Empire': 0x3232FF,
    'Federation': 0xB20000,
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
            "eventTimestamp": datetime.datetime.now().isoformat(timespec='seconds') + "Z"
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
            await cog.util.BOT.delete_message(self.waiting_messages[req_id])
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
        if not API_KEY:
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
                        raise cog.exc.RemoteError("Inara search failed. HTTP Response code bad: " +
                                                  str(resp.status))

                    response_json = await resp.json(loads=wrap_json_loads)

            # after here many things are unorthodox due api structure.
            # check if api accepted our request.
            r_code = response_json["header"]["eventStatus"]

            # handle rejection.
            if r_code == API_RESPONSE_CODES["error"] or r_code not in API_RESPONSE_CODES.values():
                raise cog.exc.RemoteError("Inara search failed. API Response code bad: " +
                                          str(r_code))

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
                user_select = await cog.util.BOT.wait_for_message(timeout=30, author=msg.author,
                                                                  channel=msg.channel)
                if user_select:
                    responses += [user_select]

                cmdrs_dict = dict(enumerate(cmdrs, 1))
                key = check_reply(user_select)
                return cmdrs_dict[key]
            except (KeyError, ValueError) as exc:
                if user_select:
                    responses += [await cog.util.BOT.send_message(msg.channel, str(exc))]
            finally:
                asyncio.ensure_future(asyncio.gather(
                    *[cog.util.BOT.delete_message(response) for response in responses]))

    async def wing_details(self, event_data, cmdr):
        """
        Fill in wing details when requested.

        Returns:
            A Discord Embed for CMDR's wing details.
        """
        # wing details
        cmdr["wing_cmdr_rank"] = event_data["commanderWing"].get("wingMemberRank",
                                                                 cmdr["wing_cmdr_rank"])
        cmdr["wing_members_count"] = event_data["commanderWing"].get("wingMembersCount",
                                                                     cmdr["wing_members_count"])

        # wing details embed
        wing_embed = discord.Embed()
        wing_embed.set_thumbnail(url=EMPTY_IMG)  # just a blank to keep embed full size
        wing_embed.set_author(name=cmdr["name"] + "'s Wing")
        wing_embed.url = event_data["commanderWing"]["inaraURL"]
        wing_embed.provider.name = SITE

        wing_embed.add_field(name="Wing Name", value=cmdr["wing"], inline=True)
        wing_embed.add_field(name=cmdr["name"] + "'s Rank", value=cmdr["wing_cmdr_rank"],
                             inline=True)
        wing_embed.add_field(name="Head Count", value=cmdr["wing_members_count"], inline=True)

        return wing_embed

    async def reply_with_api_result(self, req_id, event_data, msg, with_wing_details):
        """
        Reply using event_data from Inara API getCommanderProfile.

        """
        # cmdr prototype, only name guaranteed. Others will display if not found.
        # keeping original prototype from regex method.
        # balance and assets are not given from api.
        cmdr = {
            'name': 'ERROR',
            'profile_picture': 'https://inara.cz/images/userportraitback.png',
            'role': 'unknown',
            'allegiance': 'none',
            'rank': 'unknown',
            'power': 'none',
            'wing': 'none',
            'wing_cmdr_rank': 'unknown',
            'wing_members_count': 'unknown',
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
            cmdr[slot] = event_data.get(data_name, cmdr[slot])

        # rank, ranks are a List of Dictionaries. try to get combat rank
        if "commanderRanksPilot" in event_data:
            match = next((rank for rank in event_data["commanderRanksPilot"]
                          if rank["rankName"] == "combat"), None)
            if match:
                try:
                    cmdr["rank"] = COMBAT_RANKS[match["rankValue"]]
                except KeyError:
                    cmdr["rank"] = 'Unknown Rank'

        embeds = []

        try:
            cmdr["wing"] = event_data["commanderWing"].get("wingName", cmdr["wing"])
            if with_wing_details:
                embeds += [await self.wing_details(event_data, cmdr)]
        except KeyError:
            pass

        # Build Embed
        cmdr_embed = discord.Embed(colour=PP_COLORS.get(cmdr["allegiance"], PP_COLORS['default']))
        cmdr_embed.set_author(name=cmdr["name"], icon_url=cmdr["profile_picture"])
        cmdr_embed.set_thumbnail(url=cmdr["profile_picture"])
        cmdr_embed.url = event_data["inaraURL"]
        cmdr_embed.provider.name = SITE

        cmdr_embed.add_field(name='Wing', value=cmdr["wing"], inline=True)
        cmdr_embed.add_field(name='Allegiance', value=cmdr["allegiance"], inline=True)
        cmdr_embed.add_field(name='Role', value=cmdr["role"], inline=True)
        cmdr_embed.add_field(name='Power', value=cmdr["power"], inline=True)
        cmdr_embed.add_field(name='Combat Rank', value=cmdr["rank"], inline=True)
        embeds = [cmdr_embed] + embeds

        # TODO: KOS HOOK WILL BE HERE !
        # crosscheck who-is with KOS list, then append information to embed

        futs = [cog.util.BOT.send_message(msg.channel, embed=embed) for embed in embeds]
        futs += [self.delete_waiting_message(req_id)]
        await asyncio.gather(*futs)


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
