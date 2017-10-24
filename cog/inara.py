'''
Provide ability to search commanders on inara.cz

Thanks to CMDR shotwn for conntribution.
Contributed: 20/10/2017
'''
import asyncio
import math
import re
import urllib.parse

import aiohttp
import aiomock
import discord

import cog.exc
import cog.util
# TODO: Convert to module with statics
# TODO: Make smaller functions/better use of exceptions

INARA = 'https://inara.cz'
INARA_LOGIN = '{}/login'.format(INARA)
INARA_SEARCH = '{}/search?location=search&searchglobal='.format(INARA)
print(INARA, INARA_LOGIN, INARA_SEARCH)
PP_COLORS = {
    'Alliance': 0x008000,
    'Empire': 0x3232FF,
    'Federation': 0xB20000,
    'default': 0xDEADBF,
}


class InaraApi():
    """
    hillbilly inara.cz who-is api !!!!!
    """
    def __init__(self, bot):
        self.bot = bot
        self.waiting_messages = {}  # Searching in inara.cz messages. keys are req_id.
        self.req_counter = 0  # count how many searches done with search_in_inara
        self.http = aiohttp.ClientSession()

    def __del__(self):
        self.http.close()

    async def login_to_inara(self):
        """
        Perform initial login to inara, required to see search results.
        """
        payload = cog.util.get_config('inara')

        # DO LOGIN, Inara doesn't use HTTP auth. It is a standard post.
        async with self.http.post(INARA_LOGIN, data=payload) as resp:

            # Fail with HTTP Resp Code.
            if resp.status != 200:
                raise ValueError("Login failed. Response code: {}".format(resp.status))

            # Fail if wrong login
            response_text = await resp.text()
            if "WRONG LOGIN/PASSWORD" in response_text:
                raise ValueError("Bad Login or Password in inara login")

            return True

    async def delete_waiting_message(self, req_id):
        """ delete the message which informs user about start of search """

        await self.bot.delete_message(self.waiting_messages[req_id])
        del self.waiting_messages[req_id]
        return True

    async def search_in_inara(self, cmdr_name, msg):
        """ search commander name in inara.cz """

        # set req_id
        req_id = self.req_counter  # async disaster waiting here
        self.req_counter += 1  # check init for details

        # send waiting message
        self.waiting_messages[req_id] = await self.bot.send_message(msg.channel, "Searching inara.cz ...")  # when using one session for entire app, this behaviour will change

        # search for commander name
        async with self.http.get(INARA_SEARCH + urllib.parse.quote_plus(cmdr_name)) as resp:

            # fail with HTTP error
            if resp.status != 200:
                await self.delete_waiting_message(req_id)
                await self.bot.send_message(msg.channel, "Internal Error")
                raise ValueError("Inara Search Failed. HTTP response code: {}".format(resp.status))

            # wait for response text
            response_text = await resp.text()

        # logic to follow if response requires login
        if "You must be logged in to view search results" in response_text:

            # try loggin in
            try:
                await self.login_to_inara()
            except ValueError as error:
                print(error.args)  # TODO: integrade with internal debug. pprint ?
                return False  # die if can't login

            # login successful, try again
            await self.delete_waiting_message(req_id)  # delete previous message (this logic will work rarely. it is okay.)
            second_attempt = await self.search_in_inara(cmdr_name, msg)  # call search again
            return second_attempt  # return second attempt.

        # Extract the block of commanders
        match = re.search(r'Commanders found</h2><div class="mainblock" style="-webkit-column-count: 3; -moz-column-count: 3; column-count: 3;">(.+?)</div>', response_text)
        if not match:
            await self.bot.send_message(msg.channel, "Could not find CMDR **{}**".format(cmdr_name))
            await self.delete_waiting_message(req_id)
            return False

        # Extract all cmdrs found
        # group(1) is commander url in inara
        # group(2) is commander name
        cmdrs = re.findall(r'<a href="(\S+)" class="inverse">([^<]+)</a>', match.group(1))
        if len(cmdrs) == 1 and cmdrs[0][1].lower() == cmdr_name.lower():
            cmdr = cmdrs[0]
        else:
            cmdr = await self.select_from_choices(cmdr_name, cmdrs, msg, req_id)
            if not cmdr:
                await self.delete_waiting_message(req_id)
                await self.bot.send_message(msg.channel, 'Improper response. Resubmit command.')
                return None

        return {
            "req_id": req_id,
            "url": INARA + cmdr[0],
            "name": cmdr[1],
        }

    async def select_from_choices(self, cmdr_name, cmdrs, msg, req_id):
        """
        Present the loosely matched choices and wait for user selection.

        Returns:
            Present choices and await a valid numeric reply.
            None if any of the following true:
                1) Timesout waiting for user response.
                2) Invalid response from user (i.e. text, invalid number).
        """
        pad = str(math.ceil(len(cmdrs) / 10))
        fmt = '{:' + pad + '}) {}'
        cmdr_list = [fmt.format(ind, cmdr[1]) for ind, cmdr in enumerate(cmdrs, 1)]

        repy = 'No exact match for CMDR **{}**\nChoose from:{}'.format(
            cmdr_name, '\n    ' + '\n    '.join(cmdr_list))
        bot_choices = await self.bot.send_message(msg.channel, repy)
        author_choice = await self.bot.wait_for_message(timeout=30, author=msg.author,
                                                        channel=msg.channel)
        try:
            key = int(author_choice.content)
            cmdrs_dict = dict(enumerate(cmdrs, 1))
            return cmdrs_dict[key]
        except (KeyError, ValueError):
            return None
        finally:
            asyncio.ensure_future(asyncio.gather(self.bot.delete_message(bot_choices),
                                                 self.bot.delete_message(author_choice)))

    async def fetch_from_cmdr_page(self, found_commander, msg):
        """ fetch cmdr page, setup embed and send """
        async with self.http.get(found_commander["url"]) as resp:

            # fail with HTTP error
            if resp.status != 200:
                await self.bot.send_message(msg.channel, "I can't fetch page for " + str(found_commander["name"]))
                await self.delete_waiting_message(found_commander["req_id"])
                raise ValueError("Inara CMDR page: HTTP response code NOT 200. The code is: " + str(resp.status))

            # wait response text
            response_text = await resp.text()

        # cmdr prototype | defaults
        cmdr = {
            'name': 'ERROR',
            'profile_picture': '/images/userportraitback.png',
            'role': 'unknown',
            'allegiance': 'none',
            'rank': 'unknown',
            'power': 'none',
            'credit_balance': 'unknown',
            'wing': 'none',
            'assets': 'unknown'
        }

        # assignments here, could be done muuuuch more elegantly but for now works just fine.
        # TODO: make with with a loop for gods sake.
        cmdr_name = re.search(r'<td colspan="3" class="header"><span class="pflheadersmall">CMDR</span> ([^\<]+)</td>', response_text)
        if cmdr_name:
            cmdr["name"] = cmdr_name.group(1)

        cmdr_profile_picture = re.search(r'<td rowspan="4" class="profileimage"><img src="([^\"]+)"', response_text)
        if cmdr_profile_picture:
            cmdr["profile_picture"] = INARA + cmdr_profile_picture.group(1)

        cmdr_role = re.search(r'<td><span class="pflcellname">Role</span><br>([^\<]+)</td>', response_text)
        if cmdr_role and cmdr_role.group(1) != "&nbsp;":
            cmdr["role"] = cmdr_role.group(1)

        cmdr_allegiance = re.search(r'Allegiance</span><br>([^\<]+)</td>', response_text)
        if cmdr_allegiance and cmdr_allegiance.group(1) != "&nbsp;":
            cmdr["allegiance"] = cmdr_allegiance.group(1)

        cmdr_rank = re.search(r'Rank</span><br>([^\<]+)</td>', response_text)
        if cmdr_rank and cmdr_rank.group(1) != "&nbsp;":
            cmdr["rank"] = cmdr_rank.group(1)

        cmdr_power = re.search(r'Power</span><br>([^\<]+)</td>', response_text)
        if cmdr_power and cmdr_power.group(1) != "&nbsp;":
            cmdr["power"] = cmdr_power.group(1)

        cmdr_credit_balance = re.search(r'Credit Balance</span><br>([^\<]+)</td>', response_text)
        if cmdr_credit_balance and cmdr_credit_balance.group(1) != "&nbsp;":
            cmdr["credit_balance"] = cmdr_credit_balance.group(1)

        cmdr_wing = re.search(r'Wing</span><br>([^\<]+)</td>', response_text)
        if cmdr_wing and cmdr_wing.group(1) != "&nbsp;":
            cmdr["wing"] = cmdr_wing.group(1)

        cmdr_assets = re.search(r'Overall assets</span><br>([^\<]+)</td>', response_text)
        if cmdr_assets and cmdr_assets.group(1) != "&nbsp;":
            cmdr["assets"] = cmdr_assets.group(1)

        # match = re.search(r'<a href="(/wing/\d+/)"', response_text)
        # if match and match.group(1) != "&nbsp;":
            # cmdr['wing_url'] = INARA + match.group(1)

        # KOS HOOK WILL BE HERE !
        # to crosscheck who-is with KOS list.
        # and add a footer to embed if cmdr is in KOS

        if __name__ == "__main__":
            import pprint
            pprint.pprint(str(cmdr))
            return

        # Build Embed
        em = discord.Embed(colour=PP_COLORS.get(cmdr["allegiance"], PP_COLORS['default']))
        em.set_author(name=cmdr["name"], icon_url=cmdr["profile_picture"])
        em.set_thumbnail(url=cmdr["profile_picture"])
        em.url = found_commander["url"]
        em.provider.name = INARA
        em.add_field(name='Wing', value=cmdr["wing"], inline=True)
        em.add_field(name='Allegiance', value=cmdr["allegiance"], inline=True)
        em.add_field(name='Role', value=cmdr["role"], inline=True)
        em.add_field(name='Power', value=cmdr["power"], inline=True)
        em.add_field(name='Rank', value=cmdr["rank"], inline=True)
        em.add_field(name='Overall Assets', value=cmdr["assets"], inline=True)
        em.add_field(name='Credit Balance', value=cmdr["credit_balance"], inline=True)
        # em.set_footer(text='Wing Link: ' + cmdr['wing_url'])

        await self.bot.send_message(msg.channel, embed=em)
        await self.delete_waiting_message(found_commander["req_id"])


Inara = InaraApi(False)  # use as module, needs "bot" to be set. pylint: disable=C0103


async def whois(cmdr_name):
    msg = aiomock.Mock(channel='channel', content='!whois gearsandcogs')

    cmdr = await Inara.search_in_inara(cmdr_name, msg)
    if cmdr:
        await Inara.fetch_from_cmdr_page(cmdr, msg)


def main():
    mock_bot = aiomock.AIOMock()
    mock_bot.send_message.async_side_effect = lambda x, y: print(x, '//', y)
    mock_bot.delete_message.async_return_value = None
    Inara.bot = mock_bot
    loop = asyncio.get_event_loop()
    loop.run_until_complete(whois('gearsandcogs'))


if __name__ == "__main__":
    main()
