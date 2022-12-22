"""
Specific actions for pvp bot
"""
import concurrent.futures as cfut
import functools

import discord
import discord.ui as dui

import cog.tbl
from cogdb.eddb import LEN as EDDB_LEN
import pvp.journal
# Make available reused actions
from cog.actions import (Action, Donate, Feedback, Near, Repair, Route, Status, Time, Trigger, WhoIs)  # noqa: F401 pylint: disable=unused-import


class Help(Action):
    """
    Provide an overview of help.
    """
    async def execute(self):
        prefix = self.bot.prefix
        overview = '\n'.join([
            'Here is an overview of my commands.',
            '',
            f'For more information do: `{prefix}Command -h`',
            f'       Example: `{prefix}drop -h`'
            '',
        ])
        lines = [
            ['Command', 'Effect'],
            ['{prefix}dist', 'Determine the distance from the first system to all others'],
            ['{prefix}donate', 'Information on supporting the dev.'],
            ['{prefix}feedback', 'Give feedback or report a bug'],
            ['{prefix}help', 'This help command.'],
            ['{prefix}near', 'Find things near you.'],
            ['{prefix}repair', 'Show the nearest orbitals with shipyards'],
            ['{prefix}route', 'Plot the shortest route between these systems'],
            ['{prefix}status', 'Info about this bot'],
            ['{prefix}time', 'Show game time and time to ticks'],
            ['{prefix}trigger', 'Calculate fort and um triggers for systems'],
            ['{prefix}whois', 'Search for commander on inara.cz'],
        ]
        lines = [[line[0].format(prefix=prefix), line[1]] for line in lines]

        response = overview + cog.tbl.format_table(lines, header=True)[0]
        await self.bot.send_ttl_message(self.msg.channel, response)
        try:
            await self.msg.delete()
        except discord.HTTPException:
            pass


async def cmdr_setup(eddb_session, client, msg):
    """
    Perform first time cmdr setup via interactive questions.
    """
    def check_button(orig_author, sent, inter):
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
        return inter.message == sent and inter.user == orig_author

    view = dui.View().\
        add_item(dui.Button(label="Yes", custom_id="Yes", style=discord.ButtonStyle.green)).\
        add_item(dui.Button(label="No", custom_id="No", style=discord.ButtonStyle.red))

    text = "This bot will store information based on uploads and link it to your Discord ID. Ok?"
    sent = await msg.channel.send(text, view=view)
    check = functools.partial(check_button, msg.author, sent)
    inter = await client.wait_for('interaction', check=check)

    print(inter.data)
    if not inter.data['custom_id'].lower().strip().startswith('y'):
        await inter.response.send_message('Aborting upload. Bye.', ephemeral=True)
        return False

    view = dui.View().\
        add_item(dui.TextInput(label="CMDR name", custom_id="cmdr_name", style=discord.TextStyle.short,
                               placeholder='CMDR name', required=True, min_length=2, max_length=EDDB_LEN['pvp_name']))
    text = "What is your in game cmdr name?"
    sent = await msg.channel.send(text, view=view)
    check = functools.partial(check_button, msg.author, sent)
    inter = await client.wait_for('interaction', check=check)
    print(inter.data)

    pvp.schema.add_pvp_cmdr(eddb_session, msg.author.id, 'name')
    eddb_session.flush()


class FileUpload(Action):
    """
    Handle a file upload and scan for pvp information.

    For this action self.session is an instance of EDDBSession.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = kwargs['file']

    async def execute(self):
        discord_id = self.msg.author.id
        cmdr = pvp.schema.get_pvp_cmdr(self.session, discord_id)
        if not cmdr:
            if not await cmdr_setup(self.session, self.bot, self.msg):
                return

        #  with cfut.ProcessPoolExecutor(max_workers=1) as pool:
            #  await self.bot.loop.run_in_executor(
                #  pool, pvp.journal.load_journal_possible, self.file, discord_id,
            #  )
