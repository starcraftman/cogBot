'''
Provide ability to search commanders on inara.cz

Thanks to CMDR shotwn for conntribution.
Contributed: 20/10/2017
'''
import asyncio
import re
import urllib.parse

import aiohttp
import aiomock
import discord

import cog.util


# TODO: Convert to module with statics
# TODO: Extract wing link from response.
# TODO: Show all possible matches.
# TODO: Make smaller functions/better use of exceptions


class InaraApi():
    """
    hillbilly inara.cz who-is api !
    """
    def __init__(self, bot):
        self.bot = bot
        self.waiting_messages = {} # -Searching in inara.cz messages. keys are req_id.
        self.req_counter = 0 # count how many searches done with search_in_inara

        self.session = aiohttp.ClientSession()

    async def login_to_inara(self):
        """
        Perform initial login to inara, required to see search results.
        """
        payload = cog.util.get_config('inara')

        # DO LOGIN, Inara doesn't use HTTP auth. It is a standard post.
        async with self.session.post('https://inara.cz/login', data=payload) as resp:

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
        req_id = self.req_counter # async disaster waiting here
        self.req_counter += 1 # check init for details

        # send waiting message
        self.waiting_messages[req_id] = await self.bot.send_message(msg.channel, "Searching in inara.cz") # when using one session for entire app, this behaviour will change

        # search for commander name
        async with self.session.get('https://inara.cz/search?location=search&searchglobal=' + urllib.parse.quote_plus(cmdr_name)) as resp:

            # fail with HTTP error
            if resp.status != 200:
                await self.delete_waiting_message(req_id)
                await self.bot.send_message(msg.channel, "Internal Error")
                raise ValueError("Inara Search: HTTP response code NOT 200. The code is: "+str(resp.status))

            # wait for response text
            response_text = await resp.text()

            # logic to follow if response requires login
            if "You must be logged in to view search results" in response_text:

                # try loggin in
                try:
                    await self.login_to_inara()
                except ValueError as error:
                    print(error.args) # TODO: integrade with internal debug. pprint ?
                    return False # die if can't login

                # login successful, try again
                await self.delete_waiting_message(req_id) # delete previous message (this logic will work rarely. it is okay.)
                second_attempt = await self.search_in_inara(cmdr_name, msg) # call search again
                return second_attempt # return second attempt.

            # mother of re: where we get our information
            # [1] is commander url in inara
            # [2] is commander name
            response_text_commander = re.search(r"Commanders found</h2><div class=\"mainblock\" style=\"-webkit-column-count: 3; -moz-column-count: 3; column-count: 3;\"><a href=\"([^\"]*)\" class=\"inverse\">([^<]*)", response_text)

            if response_text_commander is None: # couldn't find commander
                await self.bot.send_message(msg.channel, "Could not found commander " + str(cmdr_name))
                await self.delete_waiting_message(req_id)
                return False

            # a rough dict to return
            found_commander = {
                "req_id": req_id,
                "url": response_text_commander.group(1),
                "name": response_text_commander.group(2),
            }

            if cmdr_name.lower() != found_commander["name"].lower():
                await self.bot.send_message(msg.channel, "Could not found commander "+str(cmdr_name)+". Did you mean: " + found_commander["name"])
                await self.delete_waiting_message(req_id)
                return False

            return found_commander

    async def fetch_from_cmdr_page(self, found_commander, msg):
        """ fetch cmdr page, setup embed and send """
        async with self.session.get("https://inara.cz"+found_commander["url"]) as resp:

            # fail with HTTP error
            if resp.status != 200:
                await self.bot.send_message(msg.channel, "I can't fetch page for "+str(found_commander["name"]))
                await self.delete_waiting_message(found_commander["req_id"])
                raise ValueError("Inara CMDR page: HTTP response code NOT 200. The code is: "+str(resp.status))

            # wait response text
            response_text = await resp.text()

            # cmdr prototype | defaults
            cmdr = {
                'profile_picture': '/images/userportraitback.png',
                'name': 'ERROR',
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

            cmdr_profile_picture = re.search(r'<td rowspan=\"4\" class=\"profileimage\"><img src=\"([^\"]+)\"', response_text)
            if cmdr_profile_picture is not None:
                cmdr["profile_picture"] = cmdr_profile_picture.group(1)

            cmdr_name = re.search(r'<td colspan="3" class="header"><span class="pflheadersmall">CMDR</span> ([^\<]+)</td>', response_text)
            cmdr["name"] = cmdr_name.group(1)

            cmdr_role = re.search(r'<td><span class="pflcellname">Role</span><br>([^\<]+)</td>', response_text)
            if cmdr_role is not None and cmdr_role != "&nbsp;":
                cmdr["role"] = cmdr_role.group(1)

            cmdr_allegiance = re.search(r'Allegiance</span><br>([^\<]+)</td>', response_text)
            if cmdr_allegiance is not None and cmdr_allegiance.group(1) != "&nbsp;":
                cmdr["allegiance"] = cmdr_allegiance.group(1)

            cmdr_rank = re.search(r'Rank</span><br>([^\<]+)</td>', response_text)
            if cmdr_rank is not None and cmdr_rank.group(1) != "&nbsp;":
                cmdr["rank"] = cmdr_rank.group(1)

            cmdr_power = re.search(r'Power</span><br>([^\<]+)</td>', response_text)
            if cmdr_power is not None and cmdr_power.group(1) != "&nbsp;":
                cmdr["power"] = cmdr_power.group(1)

            cmdr_credit_balance = re.search(r'Credit Balance</span><br>([^\<]+)</td>', response_text)
            if cmdr_credit_balance is not None and cmdr_credit_balance.group(1) != "&nbsp;":
                cmdr["credit_balance"] = cmdr_credit_balance.group(1)

            cmdr_wing = re.search(r'Wing</span><br>([^\<]+)</td>', response_text)
            if cmdr_wing is not None and cmdr_wing.group(1) != "&nbsp;":
                cmdr["wing"] = cmdr_wing.group(1)

            cmdr_assets = re.search(r'Overall assets</span><br>([^\<]+)</td>', response_text)
            if cmdr_assets is not None and cmdr_assets.group(1) != "&nbsp;":
                cmdr["assets"] = cmdr_assets.group(1)

            colors_for_powers = {'Federation': 0xB20000, 'Empire': 0x3232FF, 'Alliance': 0x008000}
            if cmdr["allegiance"] in colors_for_powers:
                em = discord.Embed(colour=colors_for_powers[cmdr["allegiance"]])
            else:
                em = discord.Embed(colour=0xDEADBF)

            # KOS HOOK WILL BE HERE !
            # to crosscheck who-is with KOS list.
            # and add a footer to embed if cmdr is in KOS

            if __name__ == "__main__":
                import pprint
                pprint.pprint(str(cmdr))
                return

            # Build Embed
            em.set_author(name=cmdr["name"], icon_url="https://inara.cz"+cmdr["profile_picture"])
            em.set_thumbnail(url="https://inara.cz"+cmdr["profile_picture"])
            em.add_field(name='Wing', value=cmdr["wing"], inline=True)
            em.add_field(name='Allegiance', value=cmdr["allegiance"], inline=True)
            em.add_field(name='Role', value=cmdr["role"], inline=True)
            em.add_field(name='Power', value=cmdr["power"], inline=True)
            em.add_field(name='Rank', value=cmdr["rank"], inline=True)
            em.add_field(name='Overall assets', value=cmdr["assets"], inline=True)
            em.add_field(name='Credit Balance', value=cmdr["credit_balance"], inline=True)
            em.url = "https://inara.cz"+found_commander["url"]
            em.provider.name = "https//inara.cz & Marvin KOS DB"

            await self.bot.send_message(msg.channel, embed=em)
            await self.delete_waiting_message(found_commander["req_id"])


Inara = InaraApi(False) # use as module, needs "bot" to be set. pylint: disable=C0103


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
    loop.run_until_complete(whois('gears'))


if __name__ == "__main__":
    main()
