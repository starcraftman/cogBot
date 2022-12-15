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
'''
import asyncio
import copy
import datetime
import functools
import logging
import re
try:
    import rapidjson as json
except ImportError:
    import json

import aiohttp
import bs4
import discord
import discord_components_mirror as dcom

import cog
import cog.exc
import cog.util
import cogdb

# Disable line too long, pylint: disable=C0301

try:
    HEADER_PROTO = cog.util.CONF.inara.proto_header.unwrap
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
    'Deadly'] + ['Elite' + x for x in ('', ' I', ' II', ' III', ' IV', ' V')]
EMPTY_IMG = "https://upload.wikimedia.org/wikipedia/commons/5/59/Empty.png"
EMPTY_INARA = 'unknown'
INARA_SYSTEM_SEARCH = "https://inara.cz/galaxy-starsystem/?search={}"
INARA_STATION_SEARCH = "https://inara.cz/galaxy-station/?search={}%20[{}]"  # system, station_name
INARA_FACTION_SEARCH = "https://inara.cz/galaxy-minorfaction/?search={}"
RATE_MAX = 12
RATE_RESUME = 9
RATE_WINDOW = 60  # seconds of the rate window
BUT_CANCEL = 'Cancel'
BUT_FRIENDLY = 'Friendly'
BUT_HOSTILE = 'Hostile'
BUT_APPROVE = 'Approve'
BUT_DENY = 'Deny'
KOS_INFO_PROTO = {
    'add': False,
    'is_friendly': False,
    'cmdr': EMPTY_INARA,
    'reason': EMPTY_INARA,
    'squad': EMPTY_INARA,
}


class InaraNoResult(Exception):
    """
    No result was returned from Inara.
    """
    def __init__(self, msg, req_id=None):
        super().__init__(msg)
        self.req_id = req_id


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
            "eventTimestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
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
        except TypeError as exc:
            raise cog.exc.InternalException('Inara API input JSON serialization failed.',
                                            lvl='exception') from exc


class RateLimiter():
    """
    Implement a simple rate limiter.
    Rate of requests will be limited to max_rate within window seconds.
    Requests once they exceed maximum will resume when they hit the resume_rate.
    """
    def __init__(self, *, max_rate, resume_rate, window=RATE_WINDOW):
        self.window = window  # Window of the rate limiter in seconds
        self.rate = 0  # Rate of requests in last 60 seconds
        self.max_rate = max_rate
        self.resume_rate = resume_rate
        self.rate_event = asyncio.Event()

    async def increment(self, bot_client, channel):
        """
        Will increment the current rate.
        While the current rate exceeds the max, wait for event from decrement.
        """
        self.rate += 1
        msg = None

        if self.rate >= self.max_rate:
            msg = await bot_client.send_message(
                channel, "Approaching inara rate limit, please wait a moment until we are under.")

        while self.rate >= self.max_rate:
            await self.rate_event.wait()
            self.rate_event.clear()

        if msg:
            await msg.delete()

    async def decrement(self, delay=None):
        """
        Will decrement the rate after delay and notify anyone waiting to resume via an event.
        """
        if not delay:
            delay = self.window

        await asyncio.sleep(delay)

        self.rate -= 1
        if self.rate <= self.resume_rate:
            self.rate_event.set()


class InaraApi():
    """
    Inara CMDR lookups done with aiohttp module.
    Each request tracked separately, allows for back and forth with bot on loose match.
    """
    def __init__(self):
        self.req_counter = 0  # count how many searches done with search_in_inara
        self.waiting_messages = {}  # 'Searching in inara.cz' messages. keys are req_id.
        self.rate_limit = RateLimiter(max_rate=RATE_MAX, resume_rate=RATE_RESUME)

    async def delete_waiting_message(self, req_id):  # pragma: no cover
        """ Delete the message which informs user about start of search """
        if req_id in self.waiting_messages:
            await self.waiting_messages[req_id].delete()
            del self.waiting_messages[req_id]

    async def search_inara_and_kos(self, looking_for_cmdr, msg):
        """
        Top level wrapper to search for a cmdr.
        Search both Inara and local KOS db, respond with appropriate information.
        Respond to user with information from both depending on what was found.

        Args:
            looking_for_cmdr: The cmdr's name to look on inara.
            msg: The message the user sent, tracks the channel/author to respond to.

        Returns:
            kos_info: The information on if a report for kos addition should be made to moderation.
        """
        try:
            cmdr_info = await self.search_with_api(looking_for_cmdr, msg, ignore_multiple_match=False)
            return await cog.inara.api.reply_with_api_result(cmdr_info["req_id"], cmdr_info["event_data"], msg)
        except InaraNoResult as exc:
            futs = []
            response = f"__Inara__ Could not find CMDR **{looking_for_cmdr}**"

            # Even if not on inara.cz, lookup in kos
            with cogdb.session_scope(cogdb.Session) as session:
                kos_embeds = kos_lookup_cmdr_embeds(session, looking_for_cmdr)

            if kos_embeds:
                futs += [cog.util.BOT.send_message(msg.channel, embed=embed) for embed in kos_embeds]
                futs += [self.delete_waiting_message(exc.req_id)]
            else:
                response += f"\n\n__KOS__ Could not find CMDR **{looking_for_cmdr}**"

            for fut in [cog.util.BOT.send_message(msg.channel, response)] + futs:
                await fut

            # Not found in KOS, will ask if should be added.
            if not kos_embeds:
                return await self.should_cmdr_be_on_kos(exc.req_id, looking_for_cmdr, msg)

    async def search_with_api(self, looking_for_cmdr, msg, ignore_multiple_match=False):
        """
        Search for a commander on Inara.

        Raises:
            CmdAborted - User let timeout occur or cancelled selection.
            RemoteError - If response code invalid or remote unreachable (Inara itself).
            InternalException - JSON serialization failed.
            InaraNoResult - No result returned from Inara.

        Returns:
            Dictionary with information below if a matching cmdr found on Inara.
                {
                    "req_id": int,  # The number of the request.
                    "inara_cmdr_url": String, # Url of cmdr on Inara.
                    "name": string, # The name of the cmr.
                    "event_data": event_data  # The raw event data returned from inara.
                }
            None if component disabled or not found on Inara.
        """
        # keep search disabled if there is no API_KEY
        if not HEADER_PROTO:
            await cog.util.BOT.send_message(msg.channel,
                                            "!whois is currently disabled. Inara API key is not set.")
            return None

        # request id
        req_id = self.req_counter
        self.req_counter = (self.req_counter + 1) % 1000
        try:
            # Ensure we don't flood inara, they have low rate.
            await self.rate_limit.increment(cog.util.BOT, msg.channel)

            # inform user about initiating the search.
            self.waiting_messages[req_id] = await cog.util.BOT.send_message(msg.channel,
                                                                            "Searching inara.cz ...")

            api_input = InaraApiInput()
            api_input.add_event("getCommanderProfile", {"searchName": looking_for_cmdr})

            # search for commander
            async with aiohttp.ClientSession() as http:
                async with http.post(API_ENDPOINT, data=api_input.serialize(),
                                     headers=API_HEADERS) as resp:
                    if resp.status != 200:
                        raise cog.exc.RemoteError(f"Inara search failed. HTTP Response code bad: {resp.status}")
                    response_json = await resp.json(loads=wrap_json_loads)

            # after here many things are unorthodox due api structure.
            # check if api accepted our request.
            r_code = response_json["header"]["eventStatus"]

            # handle rejection.
            if r_code == API_RESPONSE_CODES["error"] or r_code not in API_RESPONSE_CODES.values():
                logging.getLogger(__name__).error("INARA Response Failure: \n%s", response_json)
                raise cog.exc.RemoteError("Inara search failed. See log for details. API Response code bad: {r_code}")

            event = response_json["events"][0]
            if event["eventStatus"] == API_RESPONSE_CODES["no result"]:
                raise InaraNoResult(f"No matching CMDR on Inara for: {looking_for_cmdr}", req_id)

            event_data = event["eventData"]

            # fetch commander name, use userName if there is no commanderName set
            inara_cmdr_name = event_data.get("commanderName", event_data["userName"])
            inara_result = {
                "req_id": req_id,
                "inara_cmdr_url": event_data["inaraURL"],
                "name": inara_cmdr_name,
                "event_data": event_data
            }

            # other possible cmdr matches
            cmdrs = event_data.get("otherNamesFound", [])

            # return if exact match and no alternatives or selected
            if inara_cmdr_name.lower() == looking_for_cmdr.lower() and (
                    not cmdrs or ignore_multiple_match):
                return inara_result

            # not an exact match or multiple matches passing, will prompt user a list for selection.
            # list will come up, delete waiting message
            await self.delete_waiting_message(req_id)
            cmdrs.insert(0, inara_cmdr_name)
            selected_cmdr = await select_from_choices(cmdrs, msg)

            if selected_cmdr == inara_cmdr_name:
                return inara_result

            # selected from otherNamesFound, run it again for selected commander.
            # it will search using returned names, so ignore multiple match this time.
            return await self.search_with_api(selected_cmdr, msg, ignore_multiple_match=True)
        finally:
            asyncio.ensure_future(self.rate_limit.decrement())
            try:
                # Delete waiting message regardless of what happens.
                await self.delete_waiting_message(req_id)
            except discord.errors.NotFound:
                pass

    async def reply_with_api_result(self, req_id, event_data, msg):
        """
        Reply using event_data from Inara API getCommanderProfile.
        Send information to user based on Inara and KOS lookup.

        Args:
            req_id: The id of the request to search.
            event_data: The event_data returned by Inara.
            msg: The original message from a user requesting search.

        Returns:
            kos_info: A KOS info object explaining if searched cmdr should be added. See should_cmdr_be_on_kos method.
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
            embeds += [await squad_details_embed(event_data, cmdr)]
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

        with cogdb.session_scope(cogdb.Session) as session:
            kos_embeds = kos_lookup_cmdr_embeds(session, cmdr['name'], cmdr['profile_picture'])
            embeds += kos_embeds

        futs = [cog.util.BOT.send_message(msg.channel, embed=embed) for embed in embeds]
        futs += [self.delete_waiting_message(req_id)]
        for fut in futs:
            await fut

        # FIXME: Why was this not defined earlier? Odd flow
        kos_info = None
        if not kos_embeds:
            # Not found in KOS db, ask if should be added
            kos_info = await self.should_cmdr_be_on_kos(req_id, cmdr['name'], msg)
            kos_info['squad'] = cmdr.get('squad', EMPTY_INARA)

        return kos_info

    async def should_cmdr_be_on_kos(self, req_id, cmdr_name, msg):
        """
        Send a message with buttons to the user asking if the cmdr should be reported.

        Args:
            req_id: The request id.
            cmdr_name: The name of the commander being reported.
            msg: The original message with the channel/author reporting the cmdr.

        Returns:
            A dictionary with the information to add user to KOS. Format follows:
            {
                'add': True | False, # If True, add to the KOS. Otherwise, take no action.
                'is_friendly': True | False, # If the user is friendly or hostile,
                'cmdr': String, # The name of cmdr.
                'reason': String, # Reason to add cmdr,
                'squad': String, # The squadron of the cmdr if known.
            }
        """
        components = [
            dcom.Button(label=BUT_FRIENDLY, style=dcom.ButtonStyle.green),
            dcom.Button(label=BUT_HOSTILE, style=dcom.ButtonStyle.red),
            dcom.Button(label=BUT_CANCEL, style=dcom.ButtonStyle.grey),
        ]
        text = f"Should the CMDR {cmdr_name} be added as friendly or hostile?"
        sent = await cog.util.BOT.send_message(msg.channel, text, components=components)
        self.waiting_messages[req_id] = sent

        check = functools.partial(check_interaction_response, msg.author, sent)
        inter = await cog.util.BOT.wait_for('button_click', check=check)

        # Approved update
        kos_info = copy.deepcopy(KOS_INFO_PROTO)
        kos_info['cmdr'] = cmdr_name
        if inter.component.label == BUT_CANCEL:
            response = "This report will be cancelled. Have a nice day!"

        else:
            kos_info.update({
                'add': True,
                'is_friendly': inter.component.label == BUT_FRIENDLY,
                'reason': f"Manual report after a !whois in {msg.channel} by cmdr {msg.author}",
            })
            response = f"""You selected {inter.component.label}

Leadership will review your report. Thank you."""

        await inter.send(response)
        await self.delete_waiting_message(req_id)

        return kos_info


async def squad_details_embed(event_data, cmdr):
    """
    Get the wing details based on event_data and the cmdr object.

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
            'name': f"{cmdr['name']}'s Squadron",
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


async def inara_squad_parse(url):
    """
    Fetch information directly from the squadron page to supplement the missing info
    from the inara api.

    Args:
        url: The link to a squardron's main or about page page.

    Returns: A dict with information that can be added to squad details.
    """
    squad_data = {
        "allegiance": EMPTY_INARA,
        "power": EMPTY_INARA,
        "language": EMPTY_INARA,
        "age": EMPTY_INARA,
        "hq": EMPTY_INARA,
        "leader": EMPTY_INARA,
        "members": EMPTY_INARA,
        "minor": EMPTY_INARA,
    }
    # Squadron information moved to an about page, redirect request
    url = url.replace('/squadron/', '/squadron-about/')
    async with aiohttp.ClientSession() as http:
        async with http.get(url) as resp:
            text = await resp.text()
            soup = bs4.BeautifulSoup(text, 'html.parser')

    # Content seems split amongst divs, iterate them all
    for div in soup.find_all('div', class_='incontent'):
        for ele in div:
            if not ele or not ele.nextSibling:
                continue

            ele_str = str(ele)
            # FIXME: Hq and minor faction sometimes don't appear, need to investigate
            if 'Allegiance:</span>' in ele_str and ele.nextSibling.strip():
                squad_data['allegiance'] = ele.nextSibling.strip()
            elif 'Power:</span>' in ele_str and ele.nextSibling.strip():
                squad_data['power'] = ele.nextSibling.string.strip()
            elif 'Language:</span>' in ele_str and ele.nextSibling.strip():
                squad_data['language'] = ele.nextSibling.strip()
            elif 'Squadron commander:</span>' in ele_str and ele.nextSibling.nextSibling.string:
                node = ele.nextSibling.nextSibling
                squad_data['leader'] = f"[{node.string.strip()}]({SITE + node['href']})"
            elif 'Members:</span>' in ele_str and ele.nextSibling.strip():
                squad_data['members'] = ele.nextSibling.strip()
            elif 'Squadron age:</span>' in ele_str and ele.nextSibling.strip():
                squad_data['age'] = ele.nextSibling.strip()
            elif 'Headquarters:</span>' in ele_str and ele.nextSibling.nextSibling.string:
                node = ele.nextSibling.nextSibling
                squad_data['hq'] = f"[{node.string.strip()}]({SITE + node['href']})"
            elif 'Related minor faction:</span>' in ele_str and ele.nextSibling.nextSibling.string:
                node = ele.nextSibling.nextSibling
                squad_data['minor'] = f"[{node.string.strip()}]({SITE + node['href']})"

    return [
        {'name': 'Squad Leader', 'value': squad_data["leader"], 'inline': True},
        {'name': 'Squad Age', 'value': squad_data["age"], 'inline': True},
        {'name': 'Allegiance', 'value': squad_data["allegiance"], 'inline': True},
        {'name': 'Power', 'value': squad_data["power"], 'inline': True},
        {'name': 'Headquarters', 'value': squad_data["hq"], 'inline': True},
        {'name': 'Minor Faction', 'value': squad_data["minor"], 'inline': True},
        {'name': 'Language', 'value': squad_data["language"], 'inline': True},
    ]


async def select_from_choices(cmdrs, msg):
    """
    Present a discord selection drop down and allow users to
    select a choice, none of them or timeout if inactive.

    Returns:
        One of the choices if user selected in time.
        None if the bot timed out user interaction.

    Raises:
        CmdAborted - Cmdr either requested abort or failed to respond.
    """
    reply = "Please select a possible match from the list. Cancel with last option."
    components = [
        dcom.Select(
            placeholder="CMDRs here",
            options=[dcom.SelectOption(label=x, value=x) for x in cmdrs + [BUT_CANCEL]],
            custom_id='select_cmdrs',
        ),
    ]

    sent = await cog.util.BOT.send_message(msg.channel, reply, components=components)
    check = functools.partial(check_interaction_response, msg.author, sent)

    try:
        inter = await cog.util.BOT.wait_for('select_option', check=check, timeout=30)
        if inter.values[0] == BUT_CANCEL:
            raise cog.exc.CmdAborted("WhoIs lookup aborted, user cancelled.")

        return inter.values[0]
    except asyncio.TimeoutError as exc:
        raise cog.exc.CmdAborted("WhoIs lookup aborted, timeout from inactivity.") from exc
    finally:
        asyncio.ensure_future(sent.delete())


def check_interaction_response(orig_author, sent, inter):
    """
    Check if a user is the original requesting author
    or if the responding user to interaction is an admin.
    Use functools.partial to leave only inter arg.

    Args:
        orig_author: The original author who made request.
        sent: The message sent with options/buttons.
        inter: The interaction argument to check.

    Returns: True ONLY if responding to same message and user allowed.
    """
    user_allowed = inter.user == orig_author
    if not user_allowed:
        with cogdb.session_scope(cogdb.Session) as session:
            try:
                cogdb.query.get_admin(session, inter.user)
                user_allowed = True
            except cog.exc.NoMatch:
                pass

    return inter.message == sent and user_allowed


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

    Args:
        sys_list: List of system names to search on Inara.
        faction_list: List of faction names to search on Inara.

    Returns: A discord Embed.
    """
    fields = [{'name': "System", 'value': f"[{system}]({sys_link})", "inline": True}
              for system, sys_link in sys_list]
    fields += [{'name': "Faction", 'value': f"[{faction}]({fact_link})", "inline": True}
               for faction, fact_link in faction_list]

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


def kos_lookup_cmdr_embeds(session, cmdr_name, cmdr_pic=None):
    """
    Look up the cmdr in the KOS db, if found return embeds that match (up to 3 closest).

    Returns:
        [embed, ...]: The discord.py embeds who match the cmdr_name.
        [] : No matches in KOS db.
    """
    if not cmdr_pic:
        cmdr_pic = EMPTY_IMG

    kos_cmdrs = cogdb.query.kos_search_cmdr(session, cmdr_name)
    embeds = []
    for kos in kos_cmdrs[:3]:
        embeds += [discord.Embed.from_dict({
            'color': KOS_COLORS.get(kos.friendly, KOS_COLORS['default']),
            'author': {
                'name': "KOS Finder",
                'icon_url': cmdr_pic,
            },
            "fields": [
                {'name': 'Name', 'value': kos.cmdr, 'inline': True},
                {'name': 'Reg Squadron', 'value': kos.squad if kos.squad else "Indy", 'inline': True},
                {'name': 'Is Friendly ?', 'value': kos.friendly, 'inline': True},
                {'name': 'Reason', 'value': kos.reason if kos.reason else "No reason.", 'inline': False},
            ],
        })]

    return embeds


def kos_report_cmdr_embed(reporter, kos_info):
    """
    Return an embed that be used to inform of a report.

    Returns: A discord embed.
    """
    kill = "FRIENDLY" if kos_info['is_friendly'] else "KILL"

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
            {'name': 'CMDR', 'value': kos_info['cmdr'], 'inline': True},
            {'name': 'Squad', 'value': kos_info['squad'], 'inline': True},
            {'name': 'Kill', 'value': kill, 'inline': True},
            {'name': 'Reason', 'value': kos_info['reason'], 'inline': False},
        ],
    })


def wrap_json_loads(string):
    """ Loads JSON. Make aiohttp use this function for custom exceptions. """
    try:
        return json.loads(string)
    except TypeError as exc:
        raise cog.exc.RemoteError('Inara API responded with bad JSON.') from exc


api = InaraApi()  # use as module, needs "bot" to be set. pylint: disable=C0103
