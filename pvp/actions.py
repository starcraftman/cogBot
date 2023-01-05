"""
Specific actions for pvp bot
"""
import concurrent.futures as cfut
import functools

import discord
import discord.ui as dui

import cogdb
from cogdb.eddb import LEN as EDDB_LEN
from cog.inara import PP_COLORS
import cog.tbl
import pvp
import pvp.journal
# Make available reused actions
from cog.actions import (Action, Dist, Donate, Feedback, Near,
                         Repair, Route, Time, Trigger, WhoIs)  # noqa: F401 pylint: disable=unused-import


class PVPAction(Action):
    """
    Top level action, contains shared logic for all PVP actions.
    All actions will require an eddb_session by design.
    When file needed can be set, otherwise will be None.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.eddb_session = kwargs['eddb_session']
        self.fname = kwargs.get('fname')


class FileUpload(PVPAction):
    """
    Handle a file upload and scan for pvp information.
    """
    async def execute(self):
        discord_id = self.msg.author.id
        cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, discord_id)
        if not cmdr:
            if not await cmdr_setup(self.eddb_session, self.bot, self.msg):
                return

        with cfut.ProcessPoolExecutor(max_workers=1) as pool:
            await self.bot.loop.run_in_executor(
                pool, parse_in_process, self.fname, discord_id,
            )

        await self.bot.send_message(self.msg.channel, "Upload received and read. Have a nice day CMDR.")


class Help(PVPAction):
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


class Log(PVPAction):
    """
    Display the most recent parsed events.
    """
    async def execute(self):
        events = pvp.schema.get_pvp_events(self.eddb_session, self.msg.author.id)
        msg = '__Most Recent Events__\n\n' + '\n'.join([str(x) for x in events])
        await self.bot.send_message(self.msg.channel, msg)


class Stats(PVPAction):
    """
    Display statistics based on file uploads to bot.
    """
    async def execute(self):
        msg = "__CMDR Statistics__\n\nNo recorded events.",
        embed = None
        stats = pvp.schema.get_pvp_stats(self.eddb_session, self.msg.author.id)
        if stats:
            msg = None
            embed = discord.Embed.from_dict({
                'color': PP_COLORS['Federation'],
                'author': {
                    'name': 'FedCAT',
                },
                'provider': {
                    'name': 'FedCAT',
                },
                'title': f"Commander {stats.cmdr.name}",
                "fields": stats.embed_values,
            })

        await self.bot.send_message(self.msg.channel, msg, embed=embed)


class Status(PVPAction):
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


class CMDRRegistration(dui.Modal, title='CMDR Registration'):
    """ Register a cmdr by getting name via a modal input. """
    name = dui.TextInput(label='CMDR Name', min_length=5, max_length=EDDB_LEN['pvp_name'],
                         style=discord.TextStyle.short, placeholder="Your in game name", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        self.name = pvp.journal.clean_cmdr_name(str(self.name))
        await interaction.response.send_message(f'You are now registered, CMDR {self.name}!', ephemeral=True)


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


def parse_in_process(fname, discord_id):
    """
    Helper to run the parsing in a separate process.

    Args:
        fname: The filename of the journal fragment.
        discord_id: The discord id of the CMDR.

    Returns: The results of the parse operation.
    """
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        parser = pvp.journal.Parser(fname=fname, cmdr_id=discord_id, eddb_session=eddb_session)
        parser.load()
        return parser.parse()
