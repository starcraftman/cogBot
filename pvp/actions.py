"""
Specific actions for pvp bot
"""
import asyncio
import concurrent.futures as cfut
import tempfile
import functools
import traceback

import discord
import discord.ui as dui

import cogdb
from cogdb.eddb import LEN as EDDB_LEN
import cog.tbl
import pvp
import pvp.journal
# Make available reused actions
from cog.actions import (Action, Dist, Donate, Feedback, Near,
                         Repair, Route, Time, Trigger, WhoIs)  # noqa: F401 pylint: disable=unused-import

DISCLAIMER = """
This bot will store information derived from logs uploaded and link it to your Discord ID.
Your in game name will be read from logs and linked to your Discord ID as well (if present).

Do you consent to this use of your data?
"""


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


class Admin(PVPAction):
    """
    Admin command console. For knowledgeable users only.
    """
    def check_cmd(self):
        """ Sanity check that cmd exists. """
        self.args.rule_cmds = [x.replace(',', '') for x in self.args.rule_cmds]
        cmd_set = set(cog.parse.CMD_MAP.values())
        cmd_set.remove('admin')
        not_found = set(self.args.rule_cmds) - cmd_set
        if not self.args.rule_cmds or len(not_found) != 0:
            msg = f"""Rules require a command in following set:

            {sorted(list(cmd_set))}

            The following were not matched:
            {', '.join(list(not_found))}
            """
            raise cog.exc.InvalidCommandArgs(msg)

    async def add(self):
        """
        Takes one of the following actions:
            1) Add 1 or more admins
            2) Add a single channel rule
            3) Add a single role rule
        """
        if not self.args.rule_cmds and self.msg.mentions:
            for member in self.msg.mentions:
                cogdb.query.add_admin(self.session, member)
            response = "Admins added:\n\n" + '\n'.join([member.name for member in self.msg.mentions])

        return response

    async def remove(self, admin):
        """
        Takes one of the following actions:
            1) Remove 1 or more admins
            2) Remove a single channel rule
            3) Remove a single role rule
        """
        if not self.args.rule_cmds and self.msg.mentions:
            for member in self.msg.mentions:
                admin.remove(self.session, cogdb.query.get_admin(self.session, member))
            response = "Admins removed:\n\n" + '\n'.join([member.name for member in self.msg.mentions])

        return response

    async def regenerate(self):  # pragma: no cover, don't hit the guild repeatedly
        """
        Regenerate the PVP database by reparsing logs.
        """
        await self.bot.send_message(self.msg.channel, "Please halt bot usage while working.")
        response = "Regenerating PVP database has completed."
        pvp.schema.empty_tables(keep_cmdrs=True)

        # Channel archiving pvp logs
        log_chan = self.bot.get_channel(cog.util.CONF.channels.pvp_log)
        try:
            async for msg in log_chan.history(limit=1000000, oldest_first=True):
                if not msg.content.startswith('Discord ID'):
                    continue

                discord_id = int(msg.content.split('\n')[0].replace('Discord ID: ', ''))

                with tempfile.NamedTemporaryFile(suffix='.jsonl') as tfile:
                    for attach in msg.attachments:
                        await attach.save(tfile.name)
                        await FileUpload(
                            args=None, bot=self, msg=msg, session=self.session,
                            eddb_session=self.eddb_session, fname=tfile.name
                        ).parse_log(discord_id=discord_id, log_upload=False)
        except discord.Forbidden:
            response = f"Missing message history permission on: {log_chan.name} on {log_chan.guild.name}"
        except discord.HTTPException:
            response = "Received HTTP Error, may be intermittent issue. Investigate."

        return response

    async def execute(self):
        try:
            admin = cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not an admin!") from exc

        try:
            func = getattr(self, self.args.subcmd)
            if self.args.subcmd == "remove":
                response = await func(admin)
            else:
                response = await func()
            if response:
                await self.bot.send_message(self.msg.channel, response)
        except (AttributeError, TypeError) as exc:
            traceback.print_exc()
            raise cog.exc.InvalidCommandArgs("Bad subcommand of `!admin`, see `!admin -h` for help.") from exc


class FileUpload(PVPAction):
    """
    Handle a file upload and scan for pvp information.
    """
    async def parse_log(self, *, discord_id, log_upload=True):
        """
        Helper to allow usage internally.

        Args:
            discord_id: The CMDR's ID.
            log_upload: If True, upload a copy to archive channel.
        """
        cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_id=discord_id)
        if not cmdr:
            name = await pvp.journal.find_cmdr_name(self.fname)
            cmdr = await cmdr_setup(self.eddb_session, self.bot, self.msg, cmdr_name=name)
            if not cmdr:  # Rejected registration
                return

        # Archive all uploaded logs to ensure ability to reconstruct database.
        if log_upload:
            log_chan = self.bot.get_channel(cog.util.CONF.channels.pvp_log)
            with open(self.fname, 'rb') as fin:
                await log_chan.send(f"Discord ID: {cmdr.id}\nCMDR: {cmdr.name}", file=discord.File(fin))

        with cfut.ProcessPoolExecutor(max_workers=1) as pool:
            events = await asyncio.get_event_loop().run_in_executor(
                pool, parse_in_process, self.fname, discord_id,
            )

        return events

    async def execute(self):
        events = await self.parse_log(log_upload=True, discord_id=self.msg.author.id)
        await self.bot.send_message(self.msg.channel, f"Log parsed, {len(events)} events detected.")


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
        if self.args.name:
            cmdr_name = ' '.join(self.args.name)
            cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_name=cmdr_name)
            msg = f"__CMDR Statistics__\n\nCMDR {cmdr_name} not found!"
        else:
            cmdr = pvp.schema.get_pvp_cmdr(self.eddb_session, cmdr_id=self.msg.author.id)
            msg = "__CMDR Statistics__\n\nNo recorded events."
        if not cmdr:
            await self.bot.send_message(self.msg.channel, msg)
            return

        embed = None
        stats = pvp.schema.get_pvp_stats(self.eddb_session, cmdr.id)
        if stats:
            msg = None
            embed = discord.Embed.from_dict({
                'color': cmdr.hex_value,
                'author': {
                    'name': 'PVP Statistics',
                    'icon_url': self.bot.user.display_avatar.url,
                },
                'provider': {
                    'name': 'FedCAT',
                },
                'thumbnail': {
                    'url': self.msg.author.display_avatar.url,
                },
                'title': f"CMDR {stats.cmdr.name}",
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
    hex = dui.TextInput(label='Hex colour for embeds. Red: B20000', min_length=6, max_length=6,
                        style=discord.TextStyle.short, placeholder="B20000", required=True)
    name = None

    async def on_submit(self, interaction: discord.Interaction):
        if not self.name:
            self.name = pvp.journal.clean_cmdr_name(self.children[-1].value)
        await interaction.response.send_message(f'You are now registered, CMDR {self.name}!', ephemeral=True)


async def cmdr_setup(eddb_session, client, msg, *, cmdr_name=None):
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

    sent = await msg.channel.send(DISCLAIMER, view=view)
    inter = await client.wait_for('interaction', check=functools.partial(check_button, msg.author, sent))

    if inter.data['custom_id'] == "No":
        await inter.response.send_message('Aborting registration. No data stored.', ephemeral=True)
        return False

    modal = CMDRRegistration()
    modal.name = cmdr_name
    if not modal.name:
        modal.add_item(
            dui.TextInput(
                label='In Game CMDR Name', min_length=5, max_length=EDDB_LEN['pvp_name'],
                style=discord.TextStyle.short, placeholder="Your in game name", required=True
            )
        )

    await inter.response.send_modal(modal)
    await modal.wait()

    return pvp.schema.add_pvp_cmdr(eddb_session, msg.author.id, modal.name, modal.hex)


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
