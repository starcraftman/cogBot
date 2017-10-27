'''
Provide ability to search commanders on Inara.cz
Lookup can be exact or loose, responds with all relevant CMDR info.

Thanks to CMDR shotwn for the contribution.
Contributed: 20/10/2017
'''
import asyncio
import atexit
import math
import re
import urllib.parse

import aiohttp
import discord

import cog.exc
import cog.util

SITE = 'https://inara.cz'
SITE_LOGIN = SITE + '/login'
SITE_SEARCH = SITE + '/search?location=search&searchglobal='
PARSERS = []
PP_COLORS = {
    'Alliance': 0x008000,
    'Empire': 0x3232FF,
    'Federation': 0xB20000,
    'default': 0xDEADBF,
}


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

    async def login_to_inara(self):
        """
        Perform initial login to inara, required to see search results.

        Raises:
            RemoteError - Response was bad or login credentials invalid.
        """
        payload = cog.util.get_config('inara')

        # DO LOGIN, Inara doesn't use HTTP auth. It is a standard post.
        async with self.http.post(SITE_LOGIN, data=payload) as resp:
            if resp.status != 200:
                raise cog.exc.RemoteError("Inara login failed, response code bad: {}".format(resp.status))

            response_text = await resp.text()
            if "Wrong login/password!" in response_text:
                raise cog.exc.RemoteError("Bad Login or Password in Inara login")

    async def delete_waiting_message(self, req_id):  # pragma: no cover
        """ Delete the message which informs user about start of search """
        if req_id in self.waiting_messages:
            await self.bot.delete_message(self.waiting_messages[req_id])
            del self.waiting_messages[req_id]

    async def search_in_inara(self, cmdr_name, msg):
        """
        Search for a commander on Inara.

        Raises:
            CmdAborted - User let timeout occur or cancelled loose match.
            RemoteError - If response code invalid or remote unreachable.
        """
        req_id = self.req_counter
        self.req_counter += 1 % 1000

        try:
            self.waiting_messages[req_id] = await self.bot.send_message(msg.channel, "Searching inara.cz ...")  # when using one session for entire app, this behaviour will change

            # search for commander name
            async with self.http.get(SITE_SEARCH + urllib.parse.quote_plus(cmdr_name)) as resp:
                if resp.status != 200:
                    raise cog.exc.RemoteError("Inara search failed. Response code bad: {}".format(resp.status))

                response_text = await resp.text()

            # Possible login expires or wasn't called before start. Retry login.
            if "You must be logged in to view search results" in response_text:
                await self.login_to_inara()
                await self.delete_waiting_message(req_id)
                return await self.search_in_inara(cmdr_name, msg)  # call search again

            # Extract the block of commanders
            match = re.search(r'Commanders found</h2><div class="mainblock" style="-webkit-column-count: 3; -moz-column-count: 3; column-count: 3;">(.+?)</div>', response_text)
            if not match:
                await self.bot.send_message(msg.channel, "Could not find CMDR **{}**".format(cmdr_name))
                return None

            # Extract all cmdrs found, come out as tuple of form:
            #       [(URL, name), (URL, name) ...]
            cmdrs = re.findall(r'<a href="(\S+)" class="inverse">([^<]+)</a>', match.group(1))
            if len(cmdrs) == 1 and cmdrs[0][1].lower() == cmdr_name.lower():
                cmdr = cmdrs[0]
            else:
                await self.delete_waiting_message(req_id)
                cmdr = await self.select_from_choices(cmdr_name, cmdrs, msg)
        finally:
            await self.delete_waiting_message(req_id)

        return {
            "req_id": req_id,
            "url": SITE + cmdr[0],
            "name": cmdr[1],
        }

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
        cmdr_list = [fmt.format(ind, cmdr[1]) for ind, cmdr in enumerate(cmdrs, 1)]
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

    async def fetch_from_cmdr_page(self, found_commander, msg):
        """
        Fetch cmdr page, parse information, setup embed and send response.

        Raises:
            RemoteError - Failed response from Inara.
        """
        async with self.http.get(found_commander["url"]) as resp:
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


api = InaraApi()  # use as module, needs "bot" to be set. pylint: disable=C0103
atexit.register(api.http.close)  # Ensure proper close, move to cog.bot later
# TODO: Alternatively, just unroll the class to be static data and module functions?
