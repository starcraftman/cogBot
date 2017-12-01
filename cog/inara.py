'''
Provide ability to search commanders on Inara.cz
Lookup can be exact or loose, responds with all relevant CMDR info.

Thanks to CMDR shotwn for the contribution.
Contributed: 20/10/2017
Inara API version: 01/12/2017 - v1

TODO: Edit tests.

'''
import asyncio
import atexit
import math
import re
import json
import datetime

import aiohttp
import discord

import cog.exc
import cog.util

SITE = 'https://inara.cz'
API_ENDPOINT = SITE + '/inapi/v1/'

# test over example response
# SITE = 'http://themainreceivers.com'
# API_ENDPOINT = SITE + '/inara_api_test.php'

# Disable line too long, pylint: disable=C0301

try:
    CONFIG = cog.util.get_config('inara')

    API_KEY = CONFIG["api_key"]
except KeyError:
    # raise cog.exc.MissingConfigFile("!whois inara search disabled. No inara field or api_key in config.yml", lvl='info')
    # logging.info("!whois inara search disabled. No inara field or api_key in config.yml")
    print("!whois inara search disabled. No inara field or api_key in config.yml")

API_ON_DEVELOPMENT = True
API_APP_NAME = 'CogBot'
API_APP_VERSION = '0.1.0'
API_HEADERS = {'content-type': 'application/json'}
API_RESPONSE_CODES = {
    'ok' : 200,
    'multiple results' : 202,
    'no result' : 204,
    'error' : 400
}

PARSERS = []
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

class InaraApiInput():
    """
    Inara API input prototype for easily generating requested JSON by Inara.

    """
    def __init__(self):
        self.header = {
            "appName" : API_APP_NAME,
            "appVersion" : API_APP_VERSION,
            "APIkey" : API_KEY,
            "isDeveloped" : True
        }
        self.events = []

    async def add_event(self, event_name, event_data):
        """ Add an event to send """
        new_event = {
            "eventName" : event_name,
            "eventData" : event_data,
            "eventTimestamp" : datetime.datetime.now().isoformat(timespec='seconds') + "Z"
        }
        self.events.append(new_event)

    async def serialize(self):
        """
        Return JSON string to send to API

        Raises:
            InternalException: JSON serialization failed.
        Returns:
            String: API request serialized as JSON.
        """
        send = {
            "header": self.header,
            "events": self.events
        }

        try:
            # do not use aiohttp to dump json for handling exception a bit better.
            return json.dumps(send)
        except TypeError:
            raise cog.exc.InternalException('Inara API input JSON serialization failed.', lvl='exception')


class InaraApi():
    """
    Inara CMDR lookups done with aiohttp module.
    Each request tracked separately, allows for back and forth with bot on loose match.

    N.B. To prevent shutdown warnings ensure self.http.close() called atexit.
    """
    def __init__(self, bot=None):
        self.bot = bot
        self.http = aiohttp.ClientSession()
        self.req_counter = 0  # count how many searches done with search_in_inara
        self.waiting_messages = {}  # 'Searching in inara.cz' messages. keys are req_id.

    async def delete_waiting_message(self, req_id):  # pragma: no cover
        """ Delete the message which informs user about start of search """
        if req_id in self.waiting_messages:
            await self.bot.delete_message(self.waiting_messages[req_id])
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
        try:
            API_KEY
        except NameError:
            await self.bot.send_message(msg.channel, "!whois is currently disabled. Inara API key is not set.")
            return None

        # request id
        req_id = self.req_counter
        # add one, loops between 0 - 1000
        self.req_counter += 1 % 1000

        try:
            # inform user about initiating the search.
            self.waiting_messages[req_id] = await self.bot.send_message(msg.channel, "Searching inara.cz ...")

            # prepare api input
            api_input = InaraApiInput()
            await api_input.add_event("getCommanderProfile", {"searchName" : cmdr_name})

            # serialize api input
            api_json = await api_input.serialize()

            # search for commander
            async with self.http.post(API_ENDPOINT, data=api_json, headers=API_HEADERS) as resp:
                if resp.status != 200:
                    raise cog.exc.RemoteError("Inara search failed. HTTP Response code bad: {}".format(resp.status))

                response_json = await resp.json(loads=wrap_json_loads)

            # after here many things are unorthodox due api structure.
            # check if api accepted our request.
            api_response_code = response_json["header"]["eventStatus"]

            # handle rejection.
            if api_response_code == API_RESPONSE_CODES["error"] or api_response_code not in API_RESPONSE_CODES.values():
                raise cog.exc.RemoteError("Inara search failed. API Response code bad: {}".format(api_response_code))

            # only one event have been send, only one event should return.
            event = response_json["events"][0]

            # check if there is no match
            if event["eventStatus"] == API_RESPONSE_CODES["no result"]:
                await self.bot.send_message(msg.channel, "Could not find CMDR **{}**".format(cmdr_name))
                return None

            event_data = event["eventData"]

            # fetch commander name, use userName if there is no commanderName set
            commander_name = event_data.get("commanderName", event_data["userName"])

            # create cmdrs list with otherNamesFound
            cmdrs = event_data.get("otherNamesFound", [])

            # check if it is an exact match
            if commander_name.lower() == cmdr_name.lower():

                # exact match, consider other matches if it is not stated otherwise.
                if not cmdrs or ignore_multiple_match:
                    return {
                        "req_id": req_id,
                        "inara_cmdr_url": event_data["inaraURL"],
                        "name": commander_name,
                        "event_data" : event_data
                    }

            # not an exact match or multiple matches passing, will prompt user a list for selection.

            # insert the one found from inara to the top
            cmdrs.insert(0, commander_name)

            # list will come up, delete waiting message
            await self.delete_waiting_message(req_id)

            # show up the list
            cmdr = await self.select_from_choices(cmdr_name, cmdrs, msg)

            # if already returned one is selected, continue with that.
            if cmdr == commander_name:
                return {
                    "req_id": req_id,
                    "imara_cmdr_url": event_data["inaraURL"],
                    "name": commander_name,
                    "event_data" : event_data
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
            '\nTo select choice 2 reply with: **cmdr 2**',
            'To abort, reply: **stop**',
            '\n__This message will delete itself on success or 30s timeout.__',
        ])

        user_select = None
        while True:
            try:
                responses = [await self.bot.send_message(msg.channel, reply)]
                user_select = await self.bot.wait_for_message(timeout=30, author=msg.author,
                                                              channel=msg.channel)
                if user_select:
                    responses += [user_select]

                cmdrs_dict = dict(enumerate(cmdrs, 1))
                key = check_reply(user_select)
                return cmdrs_dict[key]
            except (KeyError, ValueError) as exc:
                if user_select:
                    responses += [await self.bot.send_message(msg.channel, str(exc))]
            finally:
                asyncio.ensure_future(asyncio.gather(
                    *[self.bot.delete_message(response) for response in responses]))

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
            #'balance': 'unknown',
            'wing': 'none',
            'wing_cmdr_rank': 'unknown',
            'wing_members_count' : 'unknown'
            #'assets': 'unknown'
        }

        # get userName first since commanderName is not always there.
        cmdr["name"] = event_data.get("userName", cmdr["name"])

        # now replace it with commanderName if key exists.
        cmdr["name"] = event_data.get("commanderName", cmdr["name"])

        # profile picture
        cmdr["profile_picture"] = event_data.get("avatarImageURL", cmdr["profile_picture"])

        # role
        cmdr["role"] = event_data.get("preferredGameRole", cmdr["role"])

        # allegiance
        cmdr["allegiance"] = event_data.get("preferredAllegianceName", cmdr["allegiance"])

        # rank, ranks are a List of Dictionaries. try to get combat rank
        if "commanderRanksPilot" in event_data:
            match = next((rank for rank in event_data["commanderRanksPilot"] if rank["rankName"] == "combat"), None)

            if match:
                try:
                    cmdr["rank"] = COMBAT_RANKS[match["rankValue"]]
                except KeyError:
                    cmdr["rank"] = 'Unknown Rank'

        # power
        cmdr["power"] = event_data.get("preferredPowerName", cmdr["power"])

        # balance is not given from api

        # assets is not given from api

        # wing
        wing_embed = None
        if "commanderWing" in event_data:
            cmdr["wing"] = event_data["commanderWing"].get("wingName", cmdr["wing"])
            
            if with_wing_details:
                # wing details
                cmdr["wing_cmdr_rank"] = event_data["commanderWing"].get("wingMemberRank", cmdr["wing_cmdr_rank"])
                cmdr["wing_members_count"] = event_data["commanderWing"].get("wingMembersCount", cmdr["wing_members_count"])

                # wing details embed
                wing_embed = discord.Embed()

                # just a blank to keep embed full size.
                wing_embed.set_thumbnail(url="https://upload.wikimedia.org/wikipedia/commons/5/59/Empty.png")
                wing_embed.set_author(name=cmdr["name"]+"'s Wing")

                wing_embed.provider.name = SITE

                wing_embed.url = event_data["commanderWing"]["inaraURL"]
                wing_embed.add_field(name="Wing Name", value=cmdr["wing"], inline=True)
                wing_embed.add_field(name=cmdr["name"]+"'s Rank", value=cmdr["wing_cmdr_rank"], inline=True)
                wing_embed.add_field(name="Head Count", value=cmdr["wing_members_count"], inline=True)


        # TODO: KOS HOOK WILL BE HERE !
        # crosscheck who-is with KOS list, then append information to embed

        # Build Embed
        embed = discord.Embed(colour=PP_COLORS.get(cmdr["allegiance"], PP_COLORS['default']))
        embed.set_author(name=cmdr["name"], icon_url=cmdr["profile_picture"])
        embed.set_thumbnail(url=cmdr["profile_picture"])
        embed.url = event_data["inaraURL"]
        embed.provider.name = SITE
        embed.add_field(name='Wing', value=cmdr["wing"], inline=True)
        embed.add_field(name='Allegiance', value=cmdr["allegiance"], inline=True)
        embed.add_field(name='Role', value=cmdr["role"], inline=True)
        embed.add_field(name='Power', value=cmdr["power"], inline=True)
        embed.add_field(name='Combat Rank', value=cmdr["rank"], inline=True)
        #embed.add_field(name='Overall Assets', value=cmdr["assets"], inline=True)
        #embed.add_field(name='Credit Balance', value=cmdr["balance"], inline=True)

        await self.bot.send_message(msg.channel, embed=embed)

        if wing_embed:
            await self.bot.send_message(msg.channel, embed=wing_embed)

        await self.delete_waiting_message(req_id)

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
    if not msg or re.match(r'\s*stop', msg.content):
        raise cog.exc.CmdAborted('Timeout or user aborted command.')

    match = re.search(r'\s*cmdr\s+(\d+)', msg.content.lower())
    if msg.content.startswith(prefix) or not match:
        raise ValueError('Bad response.\n\nPlease choose with **cmdr x** or **stop**')

    return int(match.group(1))

def wrap_json_loads(string):
    """ Loads JSON. Make aiohttp use this function for custom exceptions. """
    try:
        return json.loads(string)
    except TypeError:
        raise cog.exc.RemoteError('Inara API responded with bad JSON.')


# TODO: Alternatively, just unroll the class to be static data and module functions?

api = InaraApi()  # use as module, needs "bot" to be set. pylint: disable=C0103
atexit.register(api.http.close)  # Ensure proper close, move to cog.bot later
