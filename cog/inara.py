'''
Provide ability to search commanders on Inara.cz
Lookup can be exact or loose, responds with all relevant CMDR info.

Thanks to CMDR shotwn for the contribution.
Contributed: 20/10/2017
Search using Inara, API version: 25/11/2017

TODO: Edit tests.

TODO: Consider removing fetch_from_cmdr_page.

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

# TODO: Consider KeyError
CONFIG = cog.util.get_config('inara')

API_KEY = CONFIG["api_key"]
API_ON_DEVELOPMENT = True
API_APP_NAME = 'CogBot'
API_APP_VERSION = '0.1.0'
API_HEADERS = {'content-type': 'application/json'}
API_RESPONSE_CODES = {
    "ok" : 200,
    "multiple results" : 202,
    "no result" : 204,
    "error" : 400
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
        """ Return JSON string to send to API """
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
        self.waiting_messages = {}  # Searching in inara.cz messages. keys are req_id.

    async def delete_waiting_message(self, req_id):  # pragma: no cover
        """ Delete the message which informs user about start of search """
        if req_id in self.waiting_messages:
            await self.bot.delete_message(self.waiting_messages[req_id])
            del self.waiting_messages[req_id]

    async def search_with_api(self, cmdr_name, msg, ignore_multiple_match = False):
        """
        Search for a commander on Inara.

        Raises:
            CmdAborted - User let timeout occur or cancelled loose match.
            RemoteError - If response code invalid or remote unreachable.
            InternalException - JSON serialization failed.

        Returns: Dictionary.
        """

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

    async def reply_with_api_result(self, req_id, eventData, msg):
        """
        Reply using eventData from Inara API getCommanderProfile.

        """
        # cmdr prototype, only name guaranteed. Others will display if not found.
        # keeping original prototype from regex method.
        # balance and assets are not given from api.
        cmdr = {
            'name': 'ERROR',
            'profile_picture': '/images/userportraitback.png',
            'role': 'unknown',
            'allegiance': 'none',
            'rank': 'unknown',
            'power': 'none',
            #'balance': 'unknown',
            'wing': 'none',
            #'assets': 'unknown'
        }

        # get userName first since commanderName is not always there.
        cmdr["name"] = eventData.get("userName", cmdr["name"])

        # now replace it with commanderName if key exists.
        cmdr["name"] = eventData.get("commanderName", cmdr["name"])

        # profile picture
        cmdr["profile_picture"] = eventData.get("avatarImageURL", cmdr["profile_picture"])

        # role
        cmdr["role"] = eventData.get("preferredGameRole", cmdr["role"])

        # allegiance
        cmdr["allegiance"] = eventData.get("preferredAllegianceName", cmdr["allegiance"])

        # rank, ranks are a List of Dictionaries. try to get combat rank
        if "commanderRanksPilot" in eventData:
            match = next((rank for rank in eventData["commanderRanksPilot"] if rank["rankName"] == "combat"), None)

            if match:
                try:
                    cmdr["rank"] = COMBAT_RANKS[match["rankValue"]]
                except KeyError:
                    cmdr["rank"] = 'Unknown Rank'

        # power
        cmdr["power"] = eventData.get("preferredPowerName")

        # balance is not given from api

        # wing
        if "commanderWing" in eventData:
            cmdr["wing"] = eventData["commanderWing"].get("wingName", cmdr["wing"])

        # assets is not given from api

        # TODO: KOS HOOK WILL BE HERE !
        # crosscheck who-is with KOS list, then append information to embed

        # Build Embed
        em = discord.Embed(colour=PP_COLORS.get(cmdr["allegiance"], PP_COLORS['default']))
        em.set_author(name=cmdr["name"], icon_url=cmdr["profile_picture"])
        em.set_thumbnail(url=cmdr["profile_picture"])
        em.url = eventData["inaraURL"]
        em.provider.name = SITE
        em.add_field(name='Wing', value=cmdr["wing"], inline=True)
        em.add_field(name='Allegiance', value=cmdr["allegiance"], inline=True)
        em.add_field(name='Role', value=cmdr["role"], inline=True)
        em.add_field(name='Power', value=cmdr["power"], inline=True)
        em.add_field(name='Combat Rank', value=cmdr["rank"], inline=True)
        #em.add_field(name='Overall Assets', value=cmdr["assets"], inline=True)
        #em.add_field(name='Credit Balance', value=cmdr["balance"], inline=True)

        await self.bot.send_message(msg.channel, embed=em)
        await self.delete_waiting_message(req_id)

    async def fetch_from_cmdr_page(self, found_commander, msg):
        """
        Fetch cmdr page, parse information, setup embed and send response.

        Raises:
            RemoteError - Failed response from Inara.
        """
        async with self.http.get(found_commander["inara_cmdr_url"]) as resp:
            if resp.status != 200:
                raise cog.exc.RemoteError("Inara CMDR page: Bad response code: " + str(resp.status))

            response_text = await resp.text()

        # cmdr prototype, only name guaranteed. Others will display if not found.
        cmdr = {
            'name': 'ERROR',
            'profile_picture': '/images/userportraitback.png',
            'role': 'unknown',
            'allegiance': 'none',
            'rank': 'unknown',
            'power': 'none',
            'balance': 'unknown',
            'wing': 'none',
            'assets': 'unknown'
        }

        for func in PARSERS:
            func(response_text, cmdr)

        # TODO: KOS HOOK WILL BE HERE !
        # crosscheck who-is with KOS list, then append information to embed

        # Build Embed
        em = discord.Embed(colour=PP_COLORS.get(cmdr["allegiance"], PP_COLORS['default']))
        em.set_author(name=cmdr["name"], icon_url=cmdr["profile_picture"])
        em.set_thumbnail(url=cmdr["profile_picture"])
        em.url = found_commander["url"]
        em.provider.name = SITE
        em.add_field(name='Wing', value=cmdr["wing"], inline=True)
        em.add_field(name='Allegiance', value=cmdr["allegiance"], inline=True)
        em.add_field(name='Role', value=cmdr["role"], inline=True)
        em.add_field(name='Power', value=cmdr["power"], inline=True)
        em.add_field(name='Rank', value=cmdr["rank"], inline=True)
        em.add_field(name='Overall Assets', value=cmdr["assets"], inline=True)
        em.add_field(name='Credit Balance', value=cmdr["balance"], inline=True)

        await self.bot.send_message(msg.channel, embed=em)
        await self.delete_waiting_message(found_commander["req_id"])


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


def register_parser(func):  # pragma: no cover
    """ Simply register parsers for later use. """
    PARSERS.append(func)
    return func

def wrap_json_loads(string):
    """ Make aiohttp use this function for custom exceptions. """
    try:
        return json.loads(string)
    except TypeError:
        raise cog.exc.RemoteError('Inara API responded with bad JSON.')


@register_parser
def parse_allegiance(text, cmdr):
    """ Parse allegiance of CMDR from Inara page. """
    match = re.search(r'Allegiance</span><br>([^\<]+)</td>', text)
    if match and match.group(1) != "&nbsp;":
        cmdr["allegiance"] = match.group(1)


@register_parser
def parse_assets(text, cmdr):
    """ Parse assets of CMDR from Inara page. """
    match = re.search(r'Overall assets</span><br>([^\<]+)</td>', text)
    if match and match.group(1) != "&nbsp;":
        cmdr["assets"] = match.group(1)


@register_parser
def parse_balance(text, cmdr):
    """ Parse balance of CMDR from Inara page. """
    match = re.search(r'Credit Balance</span><br>([^\<]+)</td>', text)
    if match and match.group(1) != "&nbsp;":
        cmdr["balance"] = match.group(1)


@register_parser
def parse_name(text, cmdr):
    """ Parse name of CMDR from Inara page. """
    match = re.search(r'<span class="pflheadersmall">CMDR</span> ([^\<]+)</td>', text)
    if match:
        cmdr["name"] = match.group(1)


@register_parser
def parse_power(text, cmdr):
    """ Parse power of CMDR from Inara page. """
    match = re.search(r'Power</span><br>([^\<]+)</td>', text)
    if match and match.group(1) != "&nbsp;":
        cmdr["power"] = match.group(1)


@register_parser
def parse_profile_picture(text, cmdr):
    """ Parse profile picture of CMDR from Inara page. """
    match = re.search(r'<td rowspan="4" class="profileimage"><img src="([^\"]+)"', text)
    if match:
        cmdr["profile_picture"] = SITE + match.group(1)


@register_parser
def parse_rank(text, cmdr):
    """ Parse rank of CMDR from Inara page. """
    match = re.search(r'Rank</span><br>([^\<]+)</td>', text)
    if match and match.group(1) != "&nbsp;":
        cmdr["rank"] = match.group(1)


@register_parser
def parse_role(text, cmdr):
    """ Parse role of CMDR from Inara page. """
    match = re.search(r'<td><span class="pflcellname">Role</span><br>([^\<]+)</td>', text)
    if match and match.group(1) != "&nbsp;":
        cmdr["role"] = match.group(1)


@register_parser
def parse_wing(text, cmdr):
    """ Parse wing of CMDR from Inara page. """
    match = re.search(r'Wing</span><br>([^\<]+)</td>', text)
    if match and match.group(1) != "&nbsp;":
        cmdr["wing"] = match.group(1)


def parse_wing_url(text, cmdr):
    """ Parse wing of CMDR from Inara page. """
    match = re.search(r'<a href="(/wing/\d+/)"', text)
    if match and match.group(1) != "&nbsp;":
        cmdr['wing_url'] = SITE + match.group(1)


# TODO: Alternatively, just unroll the class to be static data and module functions?
# TODO: Alternative 2, simply make an api object per request and login separately.
#       Increases delay but removes all need for checking if we are logged in.
api = InaraApi()  # use as module, needs "bot" to be set. pylint: disable=C0103
atexit.register(api.http.close)  # Ensure proper close, move to cog.bot later
