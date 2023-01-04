"""
Specific actions for pvp bot
"""
import concurrent.futures as cfut
import functools

import discord
import discord.ui as dui

import cog.tbl
from cogdb.eddb import LEN as EDDB_LEN
import pvp
import pvp.journal
# Make available reused actions
from cog.actions import (Action, Dist, Donate, Feedback, Near,
                         Repair, Route, Time, Trigger, WhoIs)  # noqa: F401 pylint: disable=unused-import


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


class Status(Action):
    """
    Display the status of this bot.
    """
    async def execute(self):
        lines = [
            ['Created By', 'GearsandCogs'],
            ['Uptime', self.bot.uptime],
            ['Version', f'{pvp.__version__}'],
            ['Contributors:', ''],
        ]

        await self.bot.send_message(self.msg.channel, cog.tbl.format_table(lines)[0])


async def cmdr_setup(eddb_session, client, msg):
    """
    Perform first time cmdr setup via interactive questions.
    """
    def check_button(orig_author, sent, inter):
        """
        Check the interaction is for same message and author.
        """
        return inter.message == sent and inter.user == orig_author

    view = dui.View().\
        add_item(dui.Button(label="Yes", custom_id="Yes", style=discord.ButtonStyle.green)).\
        add_item(dui.Button(label="No", custom_id="No", style=discord.ButtonStyle.red))

    text = "This bot will store information derived from logs and link it to your Discord ID. Is that ok?"
    sent = await msg.channel.send(text, view=view)
    inter = await client.wait_for('interaction', check=functools.partial(check_button, msg.author, sent))

    if inter.data['custom_id'] == "No":
        await inter.response.send_message('Aborting registration. No data stored.', ephemeral=True)
        return False

    modal = CMDRRegistration()
    await inter.response.send_modal(modal)
    await modal.wait()
    return pvp.schema.add_pvp_cmdr(eddb_session, msg.author.id, modal.name)


class CMDRRegistration(dui.Modal, title='CMDR Registration'):
    """ Register a cmdr by getting name via a modal input. """
    name = dui.TextInput(label='CMDR Name', min_length=5, max_length=EDDB_LEN['pvp_name'],
                         style=discord.TextStyle.short, placeholder="Your in game name", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        self.name = pvp.journal.clean_cmdr_name(str(self.name))
        await interaction.response.send_message(f'You are now registered, CMDR {self.name}!', ephemeral=True)


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

        with cfut.ProcessPoolExecutor(max_workers=1) as pool:
            await self.bot.loop.run_in_executor(
                pool, pvp.journal.load_journal_possible, self.file, discord_id,
            )
