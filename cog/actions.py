"""
To facilitate complex actions based on commands create a
hierarchy of actions that can be recombined in any order.
All actions have async execute methods.
"""
import asyncio
import concurrent
import concurrent.futures as cfut
import datetime
import functools
import logging
import math
import os
import pprint
import re
import tempfile
import traceback

import aiofiles
import decorator
import discord
import discord.ui as dui
import googleapiclient.errors
import gspread
import sqlalchemy.exc
import textdistance

import cogdb
import cogdb.eddb
import cogdb.query
import cogdb.scanners
import cogdb.scrape
import cogdb.side
import cogdb.spy_squirrel as spy
import cog.inara
import cog.task_monitor
import cog.tbl
import cog.util
from cogdb.schema import FortUser, UMUser, EUMSheet
from cogdb.scanners import get_scanner


async def bot_shutdown(bot):  # pragma: no cover
    """
    Shutdown the bot. Gives background jobs grace window to finish  unless empty.
    """
    logging.getLogger(__name__).error('FINAL SHUTDOWN REQUESTED')
    cog.scheduler.POOL.shutdown()
    await bot.logout()


def user_info(user):  # pragma: no cover
    """
    Trivial message formatter based on user information.
    """
    lines = [
        ['Username', f'{user.name}#{user.discriminator}'],
        ['ID', str(user.id)],
        ['Status', str(user.status)],
        ['Join Date', str(user.joined_at)],
        ['All Roles:', str([str(role) for role in user.roles[1:]])],
        ['Top Role:', str(user.top_role).replace('@', '@ ')],
    ]
    return cog.tbl.format_table(lines, prefix=f'**{user.display_name}**\n')


@decorator.decorator
async def check_mentions(coro, *args, **kwargs):
    """ If a single member mentioned, resubmit message on their behalf. """
    self = args[0]
    if self.msg.mentions:
        if len(self.msg.mentions) != 1:
            raise cog.exc.InvalidCommandArgs('Mention only 1 member per command.')

        self.log.info('DROP %s - Substituting author -> %s.',
                      self.msg.author, self.msg.mentions[0])
        self.msg.author = self.msg.mentions[0]
        self.msg.mentions = []
        await self.bot.on_message(self.msg)

    else:
        await coro(*args, **kwargs)


# TODO: Improve by not flushing when overall function raises error
async def check_sheet(*, client, scanner_name, attr, user_cls, sheet_src=None):
    """Common function that will check and add user to sheet and db if needed.

    When user not found in sheet:
    - Create sheet user in database.
    - Add payload to client.payloads to add user to new row.
    - Notify user with message.

    Args:
        client: The bot client.
        scanner_name: The name of the scanner found in config.
        attr: The name of the attribute to find the existing sheet_user on the db DUser object.
        user_cls: The class that is to be used to instantiate a user.
        sheet_src: The specific sheet_src to be used with class to create a user.

    Returns: The newly created sheet user if it was added. Otherwise None.
    """
    # Confirm the user is in the sheet and db concurs
    if getattr(client.duser, attr):
        return None

    client.log.info('USERS %s - Adding to %s as %s.',
                    client.duser.display_name, user_cls.__name__, client.duser.pref_name)
    scanner = get_scanner(scanner_name)
    sheet = cogdb.query.add_sheet_user(
        client.session, cls=user_cls, discord_user=client.duser,
        start_row=scanner.user_row, sheet_src=sheet_src
    )
    await scanner.send_batch(scanner.__class__.update_sheet_user_dict(
        sheet.row, sheet.cry, sheet.name))
    await client.bot.send_message(client.msg.channel,
                                  f'Will add {client.duser.pref_name} to the sheet. See !user command to change.')

    return sheet


class Action():
    """
    Top level action, contains shared logic.
    """
    def __init__(self, **kwargs):
        self.args = kwargs['args']
        self.bot = kwargs['bot']
        self.msg = kwargs['msg']
        self.log = logging.getLogger(__name__)
        self.session = kwargs['session']
        self.__duser = None
        self.payloads = []
        # TODO: For now used to track updates to sheet prior to sending.
        # Ideally have a separate mechanism to bunch and store inside of Scanners and
        # flush to sheet on a window (i.e. every 10s)

    @property
    def duser(self):
        """ DUser associated with message author. """
        if not self.__duser:
            self.__duser = cogdb.query.ensure_duser(self.session, self.msg.author)
            self.log.info('DUSER - %s', str(self.__duser))

        return self.__duser

    async def moderate_kos_report(self, kos_info):
        """
        Send a request to approve or deny a KOS addition.

        Args:
            kos_info: A dictionary of the form below, same as kos_info from should_cmdr_be_on_kos in cog.inara.
                {
                    'is_friendly': True | False, # If the user is friendly or hostile,
                    'cmdr': String, # The name of cmdr.
                    'reason': String, # Reason to add cmdr,
                    'squad': String, # The squadron of the cmdr if known.
                }
        """
        # Check for dupes before bothering, KOS list should have unique cmdr names.
        scanner = get_scanner('hudson_kos')
        await scanner.update_cells()
        cnt, row = scanner.find_dupe(kos_info['cmdr'])
        if cnt:
            raise cog.exc.InvalidCommandArgs(f"Duplicate *{kos_info['cmdr']}* reported as {row[2]} on row {cnt} with reason: {row[-1]}.\n\nCheck sheet. KOS addition aborted.")

        # Request approval
        chan = self.msg.guild.get_channel(cog.util.CONF.channels.ops)
        kos_info['squad'] = kos_info['squad'].capitalize() if kos_info['squad'] == cog.inara.EMPTY_INARA else kos_info['squad']
        view = dui.View().\
            add_item(dui.Button(label=cog.inara.BUT_APPROVE, custom_id=cog.inara.BUT_APPROVE, style=discord.ButtonStyle.green)).\
            add_item(dui.Button(label=cog.inara.BUT_DENY, custom_id=cog.inara.BUT_DENY, style=discord.ButtonStyle.red))
        sent = await chan.send(
            embed=cog.inara.kos_report_cmdr_embed(self.msg.author.name, kos_info),
            view=view,
        )

        check = functools.partial(cog.inara.check_interaction_response, self.msg.author, sent)
        inter = await self.bot.wait_for('interaction', check=check)

        response = "No change to KOS made."
        if inter.data['custom_id'] == cog.inara.BUT_APPROVE:
            await scanner.update_cells()
            payload = scanner.add_report_dict(kos_info)
            await scanner.send_batch(payload)
            cogdb.query.kos_add_cmdr(self.session, kos_info)
            self.session.commit()
            response = "CMDR has been added to KOS."

        await asyncio.gather(
            sent.delete(),
            chan.send(response),
        )

    async def execute(self):
        """
        Take steps to accomplish requested action, including possibly
        invoking and scheduling other actions.
        """
        raise NotImplementedError


class Admin(Action):
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

        else:
            self.check_cmd()

            if self.msg.channel_mentions:
                cogdb.query.add_channel_perms(self.session, self.args.rule_cmds,
                                              self.msg.channel.guild,
                                              self.msg.channel_mentions)
                response = "Channel permission added."

            else:
                cogdb.query.add_role_perms(self.session, self.args.rule_cmds,
                                           self.msg.channel.guild,
                                           self.msg.role_mentions)
                response = "Role permission added."

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

        else:
            self.check_cmd()

            if self.msg.channel_mentions:
                cogdb.query.remove_channel_perms(self.session, self.args.rule_cmds,
                                                 self.msg.channel.guild,
                                                 self.msg.channel_mentions)
                response = "Channel permission removed."

            else:
                cogdb.query.remove_role_perms(self.session, self.args.rule_cmds,
                                              self.msg.channel.guild,
                                              self.msg.role_mentions)
                response = "Role permission removed."

        return response

    async def show_rules(self):
        """
        Show all rules currently in effect for the guild.
        """
        return cogdb.query.show_guild_perms(self.session, self.msg.guild, prefix=self.bot.prefix)

    async def active(self):  # pragma: no cover
        """
        Analyze the activity of users going back months for the mentioned channels.
        Upload a report directly to channel requesting the information in a text file.
        """
        if self.args.days < 1 or self.args.days > 365:
            raise cog.exc.InvalidCommandArgs("Please choose a range of days in [1, 365].")

        all_members = []
        for member in self.msg.guild.members:
            for channel in self.msg.channel_mentions:
                if channel.permissions_for(member).read_messages and member not in all_members:
                    all_members += [member]

        after = datetime.datetime.utcnow() - datetime.timedelta(days=self.args.days)
        last_msgs = {}
        for channel in self.msg.channel_mentions:
            try:
                async for msg in channel.history(limit=100000, after=after, oldest_first=True):
                    last_msgs[msg.author.id] = msg
            except discord.errors.Forbidden as exc:
                raise cog.exc.InvalidCommandArgs("Bot has no permissions for channel: " + channel.name) from exc

        for msg in last_msgs.values():
            try:
                all_members.remove(msg.author)
            except ValueError:
                pass

        all_members = sorted(all_members, key=lambda x: x.name.lower())
        all_members = sorted(all_members, key=lambda x: x.top_role.name)
        with tempfile.NamedTemporaryFile(mode='r') as tfile:
            async with aiofiles.open(tfile.name, 'w') as fout:
                day_suffix = "" if self.args.days == 1 else "s"
                await fout.write(f"__Members With No Activity in Last {self.args.days} Day{day_suffix}__\n")
                for member in all_members:
                    await fout.write(f"{member.name}, Top Role: {member.top_role}\n")

                await fout.write("\n\n__Members With Activity__\n")
                for msg in sorted(last_msgs.values(), key=lambda x: x.created_at):
                    author = msg.author
                    await fout.write(f"{author.name}, Top Role: {author.top_role}, Last Msg Sent on {msg.created_at} in {msg.channel.name}\n")

        fname = f'activity_report_{self.msg.guild.name}_{datetime.datetime.utcnow().replace(microsecond=0)}.txt'
        await self.msg.channel.send("Report generated in this file.",
                                    file=discord.File(fp=tfile.name, filename=fname))
        await asyncio.sleep(5)

    async def cast(self):
        """ Broacast a message accross a server. """
        await self.bot.broadcast(' '.join(self.args.content))
        return 'Broadcast completed.'

    async def deny(self):
        """ Toggle bot's acceptance of commands. """
        self.bot.deny_commands = not self.bot.deny_commands
        prefix = 'Dis' if self.bot.deny_commands else 'En'
        return f'Commands: **{prefix}abled**'

    async def dump(self):
        """ Dump the entire database to a file on server. """
        cogdb.query.dump_db(self.session)
        return 'Db has been dumped to server file.'

    async def halt(self):
        """ Schedule the bot for safe shutdown. """
        self.bot.deny_commands = True
        asyncio.ensure_future(bot_shutdown(self.bot))
        return 'Shutdown scheduled. Will wait for jobs to finish or max 60s.'

    async def info(self):
        """ Information on user, deprecated? """
        if self.msg.mentions:
            response = ''
            for user in self.msg.mentions:
                response += user_info(user) + '\n'
        else:
            response = user_info(self.msg.author)
        self.msg.channel = self.msg.author  # Not for public

        return response

    async def scan(self):
        """ Schedule all sheets for update. """
        self.bot.sched.schedule_all()
        return 'All sheets scheduled for update.'

    async def top(self, limit=5):
        """ Schedule all sheets for update. """
        cycle = cog.util.CONF.scanners.hudson_cattle.page
        prefix = f"__Top Merits for {cycle}__\n\n"
        try:
            exclude_roles = ["FRC Leadership", "Special Agent"] if not self.args.leaders else []
            arg_limit = self.args.limit
        except AttributeError:
            exclude_roles = ["FRC Leadership", "Special Agent"]
            arg_limit = limit
        parts = []

        top_all = await self.bot.loop.run_in_executor(
            None, cogdb.query.users_with_all_merits, self.session,
        )
        top_recruits, top_members = filter_top_dusers(self.msg.guild, top_all, exclude_roles, limit=arg_limit)
        lines = [[f"Top {limit} Recruits", "Merits", f"Top {limit} Members", "Merits"]]
        lines += [[rec[0], rec[1], mem[0], mem[1]] for rec, mem in zip(top_recruits, top_members)]
        parts += cog.tbl.format_table(lines, header=True, prefix=prefix, suffix="\n\n")

        top_fort = await self.bot.loop.run_in_executor(
            None, cogdb.query.users_with_fort_merits, self.session,
        )
        top_recruits, top_members = filter_top_dusers(self.msg.guild, top_fort, exclude_roles, limit=arg_limit)
        lines = [[f"Top {limit} Fort Recruits", "Merits", f"Top Fort {limit} Members", "Merits"]]
        lines += [[rec[0], rec[1], mem[0], mem[1]] for rec, mem in zip(top_recruits, top_members)]
        parts += cog.tbl.format_table(lines, header=True, suffix="\n\n")

        top_um = await self.bot.loop.run_in_executor(
            None, cogdb.query.users_with_um_merits, self.session,
        )
        top_recruits, top_members = filter_top_dusers(self.msg.guild, top_um, exclude_roles, limit=arg_limit)
        lines = [[f"Top {limit} UM Recruits", "Merits", f"Top UM {limit} Members", "Merits"]]
        lines += [[rec[0], rec[1], mem[0], mem[1]] for rec, mem in zip(top_recruits, top_members)]
        parts += cog.tbl.format_table(lines, header=True, suffix="\n\n")

        for part in cog.util.merge_msgs_to_least(parts):
            await self.bot.send_message(self.msg.channel, part)

    async def cycle(self, globe):
        """
        Rollover scanners to new sheets post cycle tick.
        Run the top 5 command and then cycle.

        Configs will be modified and scanners re-initialized.

        Raises:
            InternalException - No parseable numeric component found in tab.
            RemoteError - The sheet/tab combination could not be resolved. Tab needs creating.
        """
        await self.bot.send_message(self.msg.channel, "Cycling in progress ...")
        scanners = cogdb.scanners.SCANNERS
        # Zero some data before cycling
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            cogdb.query.post_cycle_db_cleanup(self.session, eddb_session)
        self.bot.deny_commands = True
        confs = cog.util.CONF.scanners.unwrap
        lines = [['Document', 'Active Page']]

        try:
            for name, template in [['hudson_cattle', 'New Template Fort'],
                                   ['hudson_undermine', 'New Template UM'],
                                   ['hudson_tracker', 'New Template Tracker']]:
                config = confs[name]
                new_page = cog.util.number_increment(config['page'])
                config['page'] = new_page
                try:
                    if name == 'hudson_cattle':
                        await scanners[name].asheet.batch_update(scanners[name].update_import_mode_dict('B9:B9', 'FALSE'), 'USER_ENTERED')
                    elif name == 'hudson_tracker':
                        await scanners[name].asheet.batch_update(scanners[name].update_import_mode_dict('B13:B13', 'FALSE'), 'USER_ENTERED')

                    # Copy template to new page and point asheet at it.
                    try:
                        await scanners[name].asheet.duplicate_sheet(template, new_page)
                    except gspread.exceptions.APIError as exc:
                        logging.getLogger(__name__).error("Failed to duplicate sheet: %s\nExc: %s", name, str(exc))
                    await scanners[name].asheet.change_worksheet(new_page)

                    if name == 'hudson_cattle':
                        await scanners[name].asheet.batch_update(scanners[name].update_import_mode_dict('B9:B9', 'TRUE'), 'USER_ENTERED')
                    elif name == 'hudson_tracker':
                        await scanners[name].asheet.batch_update(scanners[name].update_import_mode_dict('B13:B13', 'TRUE'), 'USER_ENTERED')
                except gspread.exceptions.WorksheetNotFound as exc:
                    msg = f"Missing **{new_page}** worksheet on {name}. Please fix and rerun cycle. No change made."
                    raise cog.exc.InvalidCommandArgs(msg) from exc

                globe.show_vote_goal = False
                globe.vote_goal = 75

                self.bot.sched.schedule(name, delay=1)
                lines += [[await scanners[name].asheet.title(), new_page]]

            gal_scanner = cogdb.scanners.get_scanner("hudson_gal")
            with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
                powers = eddb_session.query(cogdb.eddb.Power).\
                    filter(cogdb.eddb.Power.text != "None").\
                    order_by(cogdb.eddb.Power.eddn).\
                    all()
                for power in powers:
                    await gal_scanner.asheet.change_worksheet(power.eddn.upper())
                    await gal_scanner.cycle_reset()
            await cog.util.CONF.aupdate("scanners", value=confs)

            prefix = "Cycle incremented. Changed sheets scheduled for update.\n\n"
            return cog.tbl.format_table(lines, header=True, prefix=prefix)[0]
        except ValueError as exc:
            raise cog.exc.InternalException(f"Impossible to increment scanner: {name}") from exc
        except (AssertionError, googleapiclient.errors.HttpError) as exc:
            raise cog.exc.RemoteError(f"The sheet {name} with tab {confs[name]['page']} does not exist!") from exc
        finally:
            self.bot.deny_commands = False

    async def addum(self):
        """Add a system(s) to the um sheet"""
        values = []

        reinforcement_value = self.args.reinforced
        priority = self.args.priority
        if reinforcement_value < 0 or reinforcement_value > 50:
            raise cog.exc.InvalidCommandArgs("Wrong reinforcement value, min 0 max 50")

        um_scanner = get_scanner("hudson_undermine")
        systems_in_sheet = cogdb.query.um_get_systems(self.session, exclude_finished=False)

        # TODO Using set() to avoid many looping
        # systems_to_add = list(set([x.name for x in systems]) - set([x.name for x in systems_in_sheet]))
        # systems_in_both = list(set([x.name for x in systems_in_sheet]).intersection(set([x.name for x in systems])))
        # systems_to_add = await self.bot.loop.run_in_executor(
        #     None, cogdb.eddb.get_systems, eddb_session, systems_to_add)
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            systems = await self.bot.loop.run_in_executor(
                None, cogdb.eddb.get_systems, eddb_session,
                process_system_args(self.args.system))

            found_list = []
            msgs = []
            for system in systems:
                found = False
                for system_in_sheet in systems_in_sheet:
                    if system_in_sheet.name == system.name:
                        found = True
                        found_list.append(system.name)
                if not found:
                    power = cogdb.eddb.get_power_hq(system.power.text.lower())
                    pow_hq = cogdb.eddb.get_systems(eddb_session, [power[1]])[0]
                    if system.name != pow_hq.name:
                        reinforced_trigger = system.calc_um_trigger(pow_hq, reinforcement_value)

                        msgs += cog.tbl.format_table([
                            ["System", system.name],
                            ["Power", power[0]],
                            ["UM Trigger", system.calc_um_trigger(pow_hq)],
                            [f"UM Trigger {reinforced_trigger}%"],
                            ["Priority", priority]
                        ])
                        values.append({
                            "power": power[0],
                            "priority": priority,
                            "sys_name": system.name,
                            "security": system.security.text,
                            "trigger": reinforced_trigger,
                        })
                    else:
                        found_list.append(system.name)

        if values:
            cogdb.query.um_add_system_targets(self.session, values)
            record = cogdb.query.add_sheet_record(
                self.session, discord_id=self.msg.author.id, channel_id=self.msg.channel.id,
                command=self.msg.content, sheet_src='um',
            )
            um_sheet = await um_scanner.get_batch(['D1:13'], 'COLUMNS', 'FORMULA')
            data = cogdb.scanners.UMScanner.slide_templates(um_sheet, values)
            await um_scanner.send_batch(data, input_opt='USER_ENTERED')
            record.flushed_sheet = True

            msgs = cog.util.merge_msgs_to_least(msgs)
            for msg in cog.util.merge_msgs_to_least(msgs):
                await self.bot.send_message(self.msg.channel, msg)
            await asyncio.sleep(1)
            if found_list:
                return f"Systems added to the UM sheet.\n\nThe following systems were ignored : {', '.join(found_list)}"
            return 'Systems added to the UM sheet.'

        return 'All systems asked are already in the sheet or are invalid'

    async def removeum(self):
        """Remove a system(s) from the um sheet"""
        systems = process_system_args(self.args.system)
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            systems = await self.bot.loop.run_in_executor(
                None, cogdb.eddb.get_systems, eddb_session, systems
            )
            if len(systems) != 1:
                raise cog.exc.InvalidCommandArgs("Remove only 1 UM system from sheet at a time.")

            systems_in_sheet = [x.name for x in cogdb.query.um_get_systems(self.session, exclude_finished=False)]
            found = [x.name for x in systems if x.name in systems_in_sheet]
            reply = f'System {systems[0].name} is not in the UM sheet.'

        um_scanner = get_scanner("hudson_undermine")
        if found:
            um_sheet = await um_scanner.get_batch(['D1:ZZ'], 'COLUMNS', 'FORMULA')
            with concurrent.futures.ProcessPoolExecutor() as pool:  # CPU intensive
                data = await self.bot.loop.run_in_executor(
                    pool, cogdb.scanners.UMScanner.remove_um_system, um_sheet[0], found[0]
                )
            await um_scanner.send_batch(data, input_opt='USER_ENTERED')
            self.bot.sched.schedule("hudson_undermine", 1)
            await asyncio.sleep(1)
            reply = f'System {found[0]} removed from the UM sheet.'

        return reply

    async def execute(self):
        globe = cogdb.query.get_current_global(self.session)
        try:
            admin = cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not an admin!") from exc

        try:
            func = getattr(self, self.args.subcmd)
            if self.args.subcmd == "remove":
                response = await func(admin)
            elif self.args.subcmd == "cycle":
                response = await func(globe)
            else:
                response = await func()
            if response:
                await self.bot.send_message(self.msg.channel, response)
        except (AttributeError, TypeError) as exc:
            traceback.print_exc()
            raise cog.exc.InvalidCommandArgs("Bad subcommand of `!admin`, see `!admin -h` for help.") from exc


class BGS(Action):
    """
    Provide bgs related commands.
    """
    async def age(self, system_name, **kwargs):
        """ Handle age subcmd. """
        control_name = cogdb.query.complete_control_name(system_name, True)
        self.log.info('BGS - Looking for age around: %s', control_name)

        systems = cogdb.side.exploited_systems_by_age(kwargs['side_session'], control_name)
        systems = await self.bot.loop.run_in_executor(None, cogdb.side.exploited_systems_by_age,
                                                      kwargs['side_session'], control_name)
        lines = [['Control', 'System', 'Age']]
        lines += [[system.control, system.system, system.age] for system in systems]
        return cog.tbl.format_table(lines, header=True)[0]

    async def dash(self, system_name, **kwargs):
        """ Handle dash subcmd. """
        control_name = cogdb.query.complete_control_name(system_name, True)
        control, systems, net_inf, facts_count = await self.bot.loop.run_in_executor(
            None, cogdb.side.dash_overview, kwargs['side_session'], control_name)

        lines = [['Age', 'System', 'Control Faction', 'Gov', 'Inf', 'Net', 'N', 'Pop']]
        strong_cnt, weak_cnt = 0, 0

        strong, weak = cogdb.eddb.bgs_funcs(control_name)
        for system, faction, gov, inf, age in systems:
            lines += [[
                age if age else 0, system.name[-12:], faction.name[:20], gov.text[:3],
                f'{inf.influence:.1f}', net_inf[system.name],
                facts_count[system.name], system.log_pop
            ]]

            if system.name == control_name:
                continue

            if weak(gov.text):
                weak_cnt += 1
            elif strong(gov.text):
                strong_cnt += 1

        tot_systems = len(systems) - 1
        hlines = [
            ["Strong", f"{strong_cnt}/{tot_systems}"],
            ["Weak", f"{weak_cnt}/{tot_systems}"],
            ["Neutral", f"{tot_systems - strong_cnt - weak_cnt}/{tot_systems}"],
        ]
        explain = """
**Net**: Net change in influence over last 5 days. There may not be 5 days of data.
         If Net == Inf, they just took control.
**N**: The number of factions present in a system.
**Pop**: log10(population), i.e. log10(10000) = 4.0
         This is the exponent that would carry 10 to the population of the system.
         Example: Pop = 4.0 then actual population is: 10 ^ 4.0 = 10000
        """
        msgs = cog.tbl.format_table(hlines, prefix=f"**{control.name}**")
        msgs += cog.tbl.format_table(lines, header=True, suffix=explain)

        return cog.util.merge_msgs_to_least(msgs)[0]

    async def edmc(self, system_name, **kwargs):
        """ Handle edmc subcmd. """
        if not system_name:
            controls = cogdb.side.WATCH_BUBBLES
        else:
            controls = process_system_args(system_name.split(' '))
        controls = [x.name for x in cogdb.eddb.get_systems(kwargs['eddb_session'], controls)]

        resp = "__**EDMC Route**__\nIf no systems listed under control, up to date."
        resp += "\n\n__Bubbles By Proximity__\n"
        if len(controls) > 2:
            _, route = await self.bot.loop.run_in_executor(None, cogdb.eddb.find_best_route,
                                                           kwargs['eddb_session'], controls)
            controls = [sys.name for sys in route]
        resp += "\n".join(controls)

        control_ages = await self.bot.loop.run_in_executor(None, cogdb.side.get_system_ages,
                                                           kwargs['side_session'], controls, self.args.age)
        for control_name in control_ages:
            resp += f"\n\n__{control_name}__\n"
            ages = control_ages[control_name]
            if len(ages) > 2:
                _, systems = await self.bot.loop.run_in_executor(None, cogdb.eddb.find_best_route,
                                                                 kwargs['eddb_session'],
                                                                 [age.system for age in ages])
                resp += "\n".join([sys.name for sys in systems])

        return resp

    async def exp(self, system_name, **kwargs):
        """ Handle exp subcmd. """
        eddb_session, side_session = kwargs['eddb_session'], kwargs['side_session']
        centre = await self.bot.loop.run_in_executor(None, cogdb.eddb.get_systems,
                                                     eddb_session, [system_name])
        centre = centre[0]

        factions = await self.bot.loop.run_in_executor(None, cogdb.side.get_factions_in_system,
                                                       side_session, centre.name)
        prompt = "Please select a faction to expand with:\n"
        for ind, name in enumerate([fact.name for fact in factions]):
            prompt += f"\n({ind}) {name}"
        sent = await self.bot.send_message(self.msg.channel, prompt)
        select = await self.bot.wait_for(
            'message',
            check=lambda m: m.author == self.msg.author and m.channel == self.msg.channel,
            timeout=30)

        try:
            ind = int(select.content)
            if ind not in range(len(factions)):
                raise ValueError

            cands = await self.bot.loop.run_in_executor(None, cogdb.side.expansion_candidates,
                                                        side_session, centre, factions[ind])
            prefix = f"**Would Expand To**\n\n{centre.name}, {factions[ind].name}\n\n"
            return cog.tbl.format_table(cands, header=True, prefix=prefix)[0]
        except ValueError as exc:
            raise cog.exc.InvalidCommandArgs("Selection was invalid, try command again.") from exc
        finally:
            try:
                await sent.channel.delete_messages([sent, select])
            except discord.errors.DiscordException:
                pass

    async def expto(self, system_name, **kwargs):
        """ Handle expto subcmd. """
        matches = await self.bot.loop.run_in_executor(None, cogdb.side.expand_to_candidates,
                                                      kwargs['side_session'], system_name)
        return cog.tbl.format_table(matches, header=True, prefix="**Nearby Expansion Candidates**\n\n")[0]

    async def faction(self, _, **kwargs):
        """ Handle faction subcmd. """
        names = []
        if self.args.faction:
            names = process_system_args(self.args.faction)
        return await self.bot.loop.run_in_executor(None, cogdb.side.monitor_factions,
                                                   kwargs['side_session'], names)

    async def find(self, system_name, **kwargs):
        """ Handle find subcmd. """
        matches = await self.bot.loop.run_in_executor(None, cogdb.side.find_favorable,
                                                      kwargs['side_session'], system_name,
                                                      self.args.max)
        return cog.tbl.format_table(matches, header=True, prefix="**Favorable Factions**\n\n")[0]

    async def inf(self, system_name, **kwargs):
        """ Handle influence subcmd. """
        self.log.info('BGS - Looking for influence like: %s', system_name)
        infs = await self.bot.loop.run_in_executor(None, cogdb.side.influence_in_system,
                                                   kwargs['side_session'], system_name)

        if not infs:
            raise cog.exc.InvalidCommandArgs("Invalid system name or system is not tracked in db.")

        prefix = f"**{system_name}**\n{infs[0][-1]} (UTC)\n\n"
        lines = [['Faction Name', 'Inf', 'Gov', 'PMF?']] + [inf[:-1] for inf in infs]
        return cog.tbl.format_table(lines, header=True, prefix=prefix)[0]

    async def report(self, _, **kwargs):
        """ Handle influence subcmd. """
        session = kwargs['side_session']
        system_ids = await self.bot.loop.run_in_executor(None, cogdb.side.get_monitor_systems,
                                                         session, cogdb.side.WATCH_BUBBLES)
        report = await asyncio.gather(
            self.bot.loop.run_in_executor(None, cogdb.side.control_dictators,
                                          kwargs['side_session'], system_ids),
            self.bot.loop.run_in_executor(None, cogdb.side.moving_dictators,
                                          kwargs['side_session'], system_ids),
            self.bot.loop.run_in_executor(None, cogdb.side.monitor_events,
                                          kwargs['side_session'], system_ids))
        report = "\n".join(report)

        paste_url = await cog.util.pastebin_new_paste(f"BGS Report {datetime.datetime.utcnow()}", report)

        return f"Report Generated: <{paste_url}>"

    async def sys(self, system_name, **kwargs):
        """ Handle sys subcmd. """
        self.log.info('BGS - Looking for overview like: %s', system_name)
        system, factions = await self.bot.loop.run_in_executor(None, cogdb.side.system_overview,
                                                               kwargs['side_session'], system_name)

        if not system:
            raise cog.exc.InvalidCommandArgs(f"System **{system_name}** not found. Spelling?")
        if not factions:
            msg = f"""We aren't tracking influence in: **{system_name}**

If we should contact Gears or Sidewinder"""
            raise cog.exc.InvalidCommandArgs(msg)

        lines = []
        for faction in factions:
            is_pmf = ' (PMF)' if faction['player'] else ''
            lines += [f"{faction['name']}{is_pmf}: {faction['state']} -> {faction['pending']}"]
            if faction['stations']:
                lines += ['    Owns: ' + ', '.join(faction['stations'])]
            lines += [
                '    ' + ' | '.join([f'{inf.short_date:^5}' for inf in faction['inf_history']]),
                '    ' + ' | '.join([f'{inf.influence:^5.1f}' for inf in faction['inf_history']]),
            ]

        header = f"**{system.name}**: {system.population:,}\n\n"
        return header + '```autohotkey\n' + '\n'.join(lines) + '```\n'

    async def execute(self):
        try:
            func = getattr(self, self.args.subcmd)
            with cogdb.session_scope(cogdb.SideSession) as side_session, \
                 cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
                response = await func(' '.join(self.args.system),
                                      side_session=side_session, eddb_session=eddb_session)
                if response:
                    await self.bot.send_message(self.msg.channel, response)
        except AttributeError as exc:
            raise cog.exc.InvalidCommandArgs("Bad subcommand of `!bgs`, see `!bgs -h` for help.") from exc
        except (cog.exc.NoMoreTargets, cog.exc.RemoteError) as exc:
            response = str(exc)


class Dashboard(Action):
    """
    Handle logic related to displaying the dashboard of services.
    """
    async def execute(self):
        try:
            cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not an admin!") from exc

        if self.args.subcmd == "restart":
            task_name = ' '.join(self.args.task)
            try:
                await cog.task_monitor.TASK_MON.restart_task(name=task_name)
                response = f"Restart of {task_name} complete."
            except ValueError as exc:
                response = str(exc)

        else:
            loop = asyncio.get_event_loop()
            with cogdb.session_scope(cogdb.SideSession) as side_session:
                cells = await loop.run_in_executor(
                    None,
                    cogdb.side.service_status, side_session
                )
            with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
                cells += await cogdb.spy_squirrel.service_status(eddb_session)
                cells += await loop.run_in_executor(
                    None,
                    cogdb.eddb.service_status, eddb_session
                )
            response = f"""__Dashboard__

{cog.tbl.format_table(cells, header=False)[0]}
{cog.task_monitor.TASK_MON.format_table(header=True, wrap_msgs=True)}
SnipeMeritMonitor will stop if not configured properly.
This is most likely the case in dev environments."""

        await self.bot.send_message(self.msg.channel, response)


class Dist(Action):
    """
    Handle logic related to finding the distance between a start system and any following systems.
    """
    async def execute(self):
        system_names = process_system_args(self.args.system)
        if len(system_names) < 2:
            raise cog.exc.InvalidCommandArgs("At least **2** systems required.")

        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            dists = await self.bot.loop.run_in_executor(None, cogdb.eddb.compute_dists,
                                                        eddb_session, system_names)

        prefix = f'Distances From: **{system_names[0].capitalize()}**\n\n'
        lines = [[name, f'{dist:.2f}ly'] for name, dist in dists]
        for msg in cog.tbl.format_table(lines, prefix=prefix):
            await self.bot.send_message(self.msg.channel, msg)


class Donate(Action):
    """
    Information on how to donate. Command will not actually process anything here.
    """
    async def execute(self):
        text_path = cog.util.rel_to_abs(cog.util.CONF.paths.donate)
        async with aiofiles.open(text_path, 'r', encoding='utf-8') as fin:
            content = await fin.read()
            await self.bot.send_message(self.msg.channel, content)


class Drop(Action):
    """
    Handle the logic of dropping a fort at a target.
    """
    def finished(self, system):
        """
        Additional reply when a system is finished (i.e. deferred or 100%).
        """
        try:
            new_target = cogdb.query.fort_get_next_targets(self.session, count=1)[0]
            response = '\n\n__Next Fort Target__:\n' + new_target.display()
        except cog.exc.NoMoreTargets:
            response = '\n\n Could not determine next fort target.'

        lines = [f'**{self.duser.display_name}** Have a :cookie: for completing {system.name}']
        try:
            merits = list(reversed(sorted(system.merits)))
            top = merits[0]
            lines += ['Bonus for highest contribution:']
            for merit in merits:
                if merit.amount != top.amount:
                    break
                lines.append(f'    :cookie: for **{merit.user.name}** with {merit.amount} supplies')
        except IndexError:
            lines += ["No found contributions. Heres a :cookie: for the unknown commanders."]

        response += '\n\n' + '\n'.join(lines)

        return response

    def deferred(self, system):
        """
        Additional reply when a system is tagged as deferred (below the treshold).
        """
        try:
            new_target = cogdb.query.fort_get_next_targets(self.session, count=1)[0]
            response = '\n\n__Next Fort Target__:\n' + new_target.display()
        except cog.exc.NoMoreTargets:
            response = '\n\n Could not determine next fort target.'

        lines = [
            f'**{self.duser.display_name}** Thank you for contributing to the fort of this system.',
            f'__**{system.name}** is **almost done** and should stay **untouched** until further orders.__'
        ]

        response += '\n\n' + '\n'.join(lines)

        return response

    @check_mentions
    async def execute(self):
        """
        Drop forts at the fortification target.
        """
        globe = cogdb.query.get_current_global(self.session)
        self.log.info('DROP %s - Matched duser with id %s and sheet name %s.',
                      self.duser.display_name, self.duser.id, self.duser.fort_user)

        system = cogdb.query.fort_find_system(self.session, ' '.join(self.args.system))
        self.log.info('DROP %s - Matched system %s from: \n%s.',
                      self.duser.display_name, system.name, system)

        await check_sheet(client=self, scanner_name='hudson_cattle', attr='fort_user', user_cls=FortUser)
        drop = cogdb.query.fort_add_drop(self.session, system=system,
                                         user=self.duser.fort_user, amount=self.args.amount)
        record = cogdb.query.add_sheet_record(
            self.session, discord_id=self.msg.author.id, channel_id=self.msg.channel.id,
            command=self.msg.content, sheet_src='fort',
        )

        if self.args.set:
            system.set_status(self.args.set)
        self.log.info('DROP %s - After drop, Drop: %s\nSystem: %s.',
                      self.duser.display_name, drop, system)
        self.session.commit()

        self.payloads += cogdb.scanners.FortScanner.update_system_dict(
            drop.system.sheet_col, drop.system.fort_status, drop.system.um_status
        )
        self.payloads += cogdb.scanners.FortScanner.update_drop_dict(
            drop.system.sheet_col, drop.user.row, drop.amount
        )
        scanner = get_scanner("hudson_cattle")
        await scanner.send_batch(self.payloads)
        record.flushed_sheet = True
        self.log.info('DROP %s - Sucessfully dropped %d at %s.',
                      self.duser.display_name, self.args.amount, system.name)

        response = system.display()
        if check_system_deferred_and_globe(system, globe):
            response += self.deferred(system)
        elif system.is_fortified:
            response += self.finished(system)
        await self.bot.send_message(self.msg.channel,
                                    self.bot.emoji.fix(response, self.msg.guild))


class Fort(Action):
    """
    Provide information on and manage the fort sheet.
    """
    def find_missing(self, left):
        """ Show systems with 'left' remaining. """
        lines = [f'__Systems Missing {left} Supplies__']
        lines += [x.display(miss=True) for x in cogdb.query.fort_get_systems_x_left(self.session, left)]

        return '\n'.join(lines)

    def system_summary(self):
        """ Provide a quick summary of systems. """
        states = cogdb.query.fort_get_systems_by_state(self.session)

        total = len(cogdb.query.fort_get_systems(self.session, ignore_skips=False))
        keys = ['cancelled', 'fortified', 'undermined', 'skipped', 'left', 'almost_done']
        lines = [
            [key.capitalize() for key in keys],
            [f'{len(states[key])}/{total}' for key in keys],
        ]

        return cog.tbl.format_table(lines, sep='|', header=True)[0]

    def system_details(self):
        """
        Provide a detailed system overview.
        """
        system_names = process_system_args(self.args.system)
        if len(system_names) != 1 or system_names[0] == '':
            raise cog.exc.InvalidCommandArgs('Exactly one system required.')

        system = cogdb.query.fort_find_system(self.session, system_names[0])

        merits = [['CMDR Name', 'Merits']]
        merits += [[merit.user.name, merit.amount] for merit in reversed(sorted(system.merits))]
        merit_table = cog.tbl.format_table(merits, header=True)[0]
        return system.display_details() + "\n" + merit_table

    async def set(self):
        """
        Set the system's fort status and um status.
        """
        system_name = ' '.join(self.args.system)
        if ',' in system_name:
            raise cog.exc.InvalidCommandArgs('One system at a time with --set flag')

        system = cogdb.query.fort_find_system(self.session, system_name)
        system.set_status(self.args.set)
        record = cogdb.query.add_sheet_record(
            self.session, discord_id=self.msg.author.id, channel_id=self.msg.channel.id,
            command=self.msg.content, sheet_src='fort',
        )
        self.session.commit()

        self.payloads += cogdb.scanners.FortScanner.update_system_dict(
            system.sheet_col, system.fort_status, system.um_status
        )
        scanner = get_scanner("hudson_cattle")
        await scanner.send_batch(self.payloads)
        record.flushed_sheet = True

        return system.display()

    def order(self):
        """
        Manage the manual fort order interface.
        """

        cogdb.query.fort_order_drop(self.session)
        if self.args.system:
            system_names = process_system_args(self.args.system)
            cogdb.query.fort_order_set(self.session, system_names)
            response = """Fort order has been manually set.
When all systems completed order will return to default.
To unset override, simply set an empty list of systems.
"""
        else:
            response = "Manual fort order unset. Resuming normal order."

        return response

    def default_show(self, manual_order):
        """
        Default show fort information to users.
        """
        if manual_order:
            response = cogdb.query.fort_response_manual(self.session)
        else:
            next_count = self.args.next if self.args.next else 3
            with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
                response = cogdb.query.fort_response_normal(self.session, eddb_session, next_systems=next_count)

        return response

    async def execute(self):
        cogdb.query.fort_order_remove_finished(self.session)
        manual_order = cogdb.query.fort_order_get(self.session)

        if self.args.set:
            response = await self.set()

        elif self.args.miss:
            response = self.find_missing(self.args.miss)

        elif self.args.details:
            response = self.system_details()

        elif self.args.order:
            response = self.order()

        elif self.args.system:
            globe = cogdb.query.get_current_global(self.session)
            lines = ['__Search Results__']
            for name in process_system_args(self.args.system):
                system = cogdb.query.fort_find_system(self.session, name)
                lines.append(system.display())
                if check_system_deferred_and_globe(system, globe):
                    lines.append('This system is **almost done** and should stay **untouched** until further orders.\n')
            response = '\n'.join(lines)

        elif self.args.next:
            manual_text = ' (Manual Order)' if manual_order else ''
            lines = [f"__Next Targets{manual_text}__"]
            next_up = cogdb.query.fort_get_next_targets(self.session, offset=1, count=self.args.next)
            lines += [system.display() for system in next_up]

            response = '\n'.join(lines)

        elif self.args.priority:
            globe = cogdb.query.get_current_global(self.session)
            globe.show_almost_done = not globe.show_almost_done
            show_msg = "SHOW" if globe.show_almost_done else "NOT show"
            response = f"Will now {show_msg} the almost done fort systems."

        else:
            response = self.default_show(manual_order)

        await self.bot.send_message(self.msg.channel,
                                    self.bot.emoji.fix(response, self.msg.guild))


class Feedback(Action):
    """
    Send bug reports to Gears' Hideout reporting channel.
    """
    async def execute(self):
        lines = [
            ['Guild', self.msg.guild.name],
            ['Channel', self.msg.channel.name],
            ['Author', self.msg.author.name],
            ['Date (UTC)', datetime.datetime.utcnow()],
        ]
        response = cog.tbl.format_table(lines)[0] + '\n\n'
        response += '__Bug Report Follows__\n\n' + ' '.join(self.args.content)

        self.log.info('FEEDBACK %s - Left a bug report.', self.msg.author.name)
        await self.bot.send_message(self.bot.get_channel_by_name('feedback'), response)


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
        ])
        lines = [
            ['Command', 'Effect'],
            [f'{prefix}admin', 'Admin commands'],
            [f'{prefix}bgs', 'Display information related to BGS work'],
            [f'{prefix}dist', 'Determine the distance from the first system to all others'],
            [f'{prefix}donate', 'Information on supporting the dev.'],
            [f'{prefix}drop', 'Drop forts into the fort sheet'],
            [f'{prefix}feedback', 'Give feedback or report a bug'],
            [f'{prefix}fort', 'Get information about our fort systems'],
            [f'{prefix}help', 'This help command.'],
            [f'{prefix}hold', 'Declare held merits or redeem them'],
            [f'{prefix}kos', 'Manage or search kos list'],
            [f'{prefix}near', 'Find things near you.'],
            [f'{prefix}recruits', 'Manage recruits on the recruit sheet.'],
            [f'{prefix}repair', 'Show the nearest orbitals with shipyards'],
            [f'{prefix}route', 'Plot the shortest route between these systems'],
            [f'{prefix}scout', 'Generate a list of systems to scout'],
            [f'{prefix}status', 'Info about this bot'],
            [f'{prefix}time', 'Show game time and time to ticks'],
            [f'{prefix}track', 'Track carrier movement by system or id.'],
            [f'{prefix}trigger', 'Calculate fort and um triggers for systems'],
            [f'{prefix}um', 'Get information about undermining targets'],
            [f'{prefix}user', 'Manage your user, set sheet name and tag'],
            [f'{prefix}vote', 'Check and record cycle vote for prep/consolidation.'],
            [f'{prefix}whois', 'Search for commander on inara.cz'],
        ]
        response = overview + '\n\n' + cog.tbl.format_table(lines, header=True)[0]
        await self.bot.send_ttl_message(self.msg.channel, response)
        try:
            await self.msg.delete()
        except discord.HTTPException:
            pass


class Hold(Action):
    """
    Update a user's held merits.
    """
    @property
    def um_user(self):
        """ Property to return the right user for a hold. """
        return self.duser.um_user

    async def set_hold(self):
        """ Set the hold on a system. """
        system = cogdb.query.um_find_system(self.session, ' '.join(self.args.system),
                                            sheet_src=self.args.sheet_src)
        self.log.info('HOLD %s - Matched system name %s: \n%s.',
                      self.duser.display_name, self.args.system, system)
        hold = cogdb.query.um_add_hold(self.session, system=system,
                                       user=self.um_user, held=self.args.amount,
                                       sheet_src=self.args.sheet_src)

        if self.args.set:
            system.set_status(self.args.set)
            # TODO: Same payload now, but will have to switch if diverge.
            self.payloads += cogdb.scanners.UMScanner.update_systemum_dict(
                system.sheet_col, system.progress_us, system.progress_them
            )

        self.log.info('Hold %s - After update, hold: %s\nSystem: %s.',
                      self.duser.display_name, hold, system)

        response = hold.system.display()
        if hold.system.is_skipped:
            response += '\n\nThis system should be left for now. Type `!um` for more targets.'
        if hold.system.is_undermined:
            response += '\n\nSystem is finished with held merits. Type `!um` for more targets.'
            response += f'\n\n**{self.duser.display_name}** Have a :skull: for completing {system.name}. Don\'t forget to redeem.'

        return ([hold], response)

    async def check_sheet_user(self):
        """
        Decorate this function to prevent duplicate decorator running.
        """
        await check_sheet(client=self, scanner_name='hudson_undermine', attr='um_user',
                          user_cls=UMUser, sheet_src=EUMSheet.main)

    @check_mentions
    async def execute(self):
        self.log.info('HOLD %s - Matched self.duser with id %s and sheet name %s.',
                      self.duser.display_name, self.duser.id, self.um_user)

        if self.args.died:
            holds = cogdb.query.um_reset_held(self.session, self.um_user,
                                              sheet_src=self.args.sheet_src)
            self.log.info('HOLD %s - User reset merits.', self.duser.display_name)
            response = 'Sorry you died :(. Held merits reset.'

        elif self.args.redeem:
            holds, redeemed = cogdb.query.um_redeem_merits(self.session, self.um_user,
                                                           sheet_src=self.args.sheet_src)
            self.log.info('HOLD %s - Redeemed %d merits.', self.duser.display_name, redeemed)

            response = f'**Redeemed Now** {redeemed}\n\n__Cycle Summary__\n'
            lines = [['System', 'Hold', 'Redeemed']]
            lines += [[merit.system.name, merit.held, merit.redeemed] for merit
                      in self.um_user.merits if merit.held + merit.redeemed > 0]
            response += cog.tbl.format_table(lines, header=True)[0]

        elif self.args.redeem_systems:
            system_strs = " ".join(self.args.redeem_systems).split(",")
            holds, redeemed = cogdb.query.um_redeem_systems(self.session, self.um_user, system_strs,
                                                            sheet_src=self.args.sheet_src)

            response = f'**Redeemed Now** {redeemed}\n\n__Cycle Summary__\n'
            lines = [['System', 'Hold', 'Redeemed']]
            lines += [[merit.system.name, merit.held, merit.redeemed] for merit
                      in self.um_user.merits if merit.held + merit.redeemed > 0]
            response += cog.tbl.format_table(lines, header=True)[0]

        else:  # Default case, update the hold for a system
            if not self.args.system:
                raise cog.exc.InvalidCommandArgs("You forgot to specify a system to update.")

            await self.check_sheet_user()
            holds, response = await self.set_hold()

        record = cogdb.query.add_sheet_record(
            self.session, discord_id=self.msg.author.id, channel_id=self.msg.channel.id,
            command=self.msg.content, sheet_src='um' if self.args.sheet_src == EUMSheet.main else "snipe"
        )
        self.session.commit()

        for hold in holds:
            self.payloads += cogdb.scanners.UMScanner.update_hold_dict(
                hold.system.sheet_col, hold.user.row, hold.held, hold.redeemed)

        scanner = get_scanner("hudson_undermine" if self.args.sheet_src == EUMSheet.main else "hudson_snipe")
        await scanner.send_batch(self.payloads)
        record.flushed_sheet = True

        await self.bot.send_message(self.msg.channel, response)


class SnipeHold(Hold):
    """
    SnipeHold, same as Hold but for snipe sheet.
    """
    @property
    def um_user(self):
        return self.duser.snipe_user

    async def check_sheet_user(self):
        """
        Decorate this function to prevent duplicate decorator running.
        """
        await check_sheet(client=self, scanner_name='hudson_snipe', attr='snipe_user',
                          user_cls=UMUser, sheet_src=EUMSheet.snipe)


class KOS(Action):
    """
    Handle the KOS command.
    """
    async def report(self):  # pragma: no cover
        """
        Handle the reporting of a new cmdr.
        First ask for approval of addition, then add to kos list.
        """
        cmdr = ' '.join(self.args.cmdr)
        await self.msg.channel.send(f'CMDR {cmdr} has been reported for moderation.')
        await self.moderate_kos_report({
            'cmdr': cmdr,
            'squad': ' '.join(self.args.squad),
            'reason': ' '.join(self.args.reason) + f" -{self.msg.author.name}",
            'is_friendly': self.args.is_friendly,
        })

    async def execute(self):
        msg = 'KOS: Invalid subcommand'

        if self.args.subcmd == 'report':
            await self.report()
            msg = None

        elif self.args.subcmd == 'pull':
            scanner = get_scanner('hudson_kos')
            await scanner.update_cells()
            with cfut.ProcessPoolExecutor(max_workers=1) as pool:
                await self.bot.loop.run_in_executor(
                    pool, scanner.scheduler_run
                )
            msg = 'KOS list refreshed from sheet.'

        elif self.args.subcmd == 'search':
            msg = 'Searching for "{self.args.term}" against known CMDRs\n\n'
            cmdrs = cogdb.query.kos_search_cmdr(self.session, self.args.term)
            if cmdrs:
                lines = [['CMDR Name', 'Faction', 'Is Friendly?', 'Reason']]
                lines += [[x.cmdr, x.squad, x.friendly, x.reason] for x in cmdrs]
                msg += cog.tbl.format_table(lines, header=True)[0]
            else:
                msg += "No matches!"

        if msg:
            await self.bot.send_message(self.msg.channel, msg)


class Near(Action):
    """
    Handle the KOS command.
    """
    TRADER_MAP = {
        'data': cogdb.eddb.TraderType.MATS_DATA,
        'guardian': cogdb.eddb.TraderType.BROKERS_GUARDIAN,
        'human': cogdb.eddb.TraderType.BROKERS_HUMAN,
        'manu': cogdb.eddb.TraderType.MATS_MANUFACTURED,
        'raw': cogdb.eddb.TraderType.MATS_RAW,
    }

    async def control(self, eddb_session):
        """
        Find nearest controls.
        """
        sys_name = ' '.join(self.args.system)
        centre = cogdb.eddb.get_systems(eddb_session, [sys_name])[0]
        systems = await self.bot.loop.run_in_executor(
            None,
            functools.partial(
                cogdb.eddb.get_nearest_controls, eddb_session,
                centre_name=centre.name, power='%' + self.args.power, limit=10
            )
        )

        lines = [['System', 'Distance']] + [[x.name, f"{x.dist_to(centre):.2f}"] for x in systems]
        return "__Closest 10 Controls__\n\n" + \
            cog.tbl.format_table(lines, header=True)[0]

    async def prison(self, eddb_session):
        """
        Find nearest prison megaship.
        """
        sys_name = ' '.join(self.args.system)
        stations = await self.bot.loop.run_in_executor(
            None,
            functools.partial(
                cogdb.eddb.get_closest_station_by_government, eddb_session,
                sys_name, "Prison", limit=10
            )
        )

        lines = [['Station', 'System', 'Distance']]
        lines += [[f"{station.name}", f"{system.name}", f"{dist:.2f}"] for station, system, dist in stations]
        return f"__Closest 10 Prison Megaships__\nCentred on: {sys_name}\n\n" + \
            cog.tbl.format_table(lines, header=True)[0]

    async def _get_station_features(self, eddb_session, *, features=None, include_medium=False):
        """Helper function, find and return table of stations with required features.

        Args:
            eddb_session: A session onto the db.
            features: A list of StationFeatures to filter on.

        Returns: Table formatted for system, station reading.
        """
        sys_name = ' '.join(self.args.system)
        centre = cogdb.eddb.get_systems(eddb_session, [sys_name])[0]
        stations = await self.bot.loop.run_in_executor(
            None,
            functools.partial(
                cogdb.eddb.get_nearest_stations_with_features, eddb_session,
                centre_name=centre.name, features=features if features else [], include_medium=include_medium
            )
        )

        stations = [["System", "Distance", "Station", "Arrival"]] + stations
        title = ' '.join([x.capitalize() for x in features[0].split('_')])
        title = 'Interstellar Factors' if title.startswith('Apex')

        return cog.tbl.format_table(
            stations, header=True, prefix=f"__Nearby {title}__\nCentred on: {sys_name}\n\n",
            suffix="[L] Large pads.\n[M] M pads only."
        )[0]

    async def _get_traders(self, eddb_session):
        """Helper function, find and return table of stations with required features.

        Args:
            eddb_session: A session onto the db.
            features: A list of StationFeatures to filter on.

        Returns: Table formatted for system, station reading.
        """
        sys_name = ' '.join(self.args.system)
        centre = cogdb.eddb.get_systems(eddb_session, [sys_name])[0]
        sys_dist = 75
        stations = []
        # Broaden criteria if no results first go
        while not stations:
            stations = await self.bot.loop.run_in_executor(
                None,
                functools.partial(
                    cogdb.eddb.get_nearest_traders, eddb_session,
                    centre_name=centre.name, trader_type=self.TRADER_MAP[self.args.subcmd],
                    sys_dist=sys_dist * 2, arrival=5000,
                )
            )

        stations = [["System", "Distance", "Station", "Arrival"]] + stations
        title = self.args.subcmd.capitalize() + "s"
        return cog.tbl.format_table(
            stations, header=True, prefix=f"__Nearby {title}__\nCentred on: {sys_name}\n\n",
            suffix="[L] Large pads.\n[M] M pads only."
        )[0]

    async def execute(self):
        msg = 'Invalid near sub command.'
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            if self.args.subcmd == 'if':
                msg = await self._get_station_features(
                    eddb_session, features=['apexinterstellar'],
                    include_medium=self.args.medium
                )
            elif self.args.subcmd in self.TRADER_MAP:
                msg = await self._get_traders(eddb_session)
            else:
                try:
                    self.log.error("Trace subcmd: %s", self.args.subcmd)  # TODO: Remove, tracing anomaly in crash
                    msg = await getattr(self, self.args.subcmd)(eddb_session)
                except TypeError:
                    pass

        await self.bot.send_message(self.msg.channel, msg)


class Pin(Action):
    """
    Create an objetives pin.
    """
    # TODO: Incomplete, expect bot to manage pin entirely. Left undocumented.
    async def execute(self):
        preps = cogdb.query.fort_get_preps(self.session)
        priority, deferred = cogdb.query.fort_get_priority_targets(self.session)
        regular = cogdb.query.fort_get_next_targets(self.session, count=5)
        systems = preps + priority + deferred + regular

        lines = [f":Fortifying: {sys.name}{f' **{sys.notes}**' if sys.notes else ''}" for sys in systems]
        lines += [":Fortifying: The things in the list after that"]

        await self.bot.send_message(self.msg.channel, cog.tbl.wrap_markdown('\n'.join(lines)))

        # TODO: Use later in proper pin manager
        # to_delete = [msg]
        # async for message in self.bot.logs_from(msg.channel, 10):
        # if not message.content or message.content == "!pin":
        # to_delete += [message]
        # await to_delete.delete()


class Recruits(Action):
    """
    Manage recruits in the recruit sheet.
    """
    def duplicate_verifier(self, r_scanner, cmdr_name, discord_name):
        """
        Looks in the sheet for similar names. Names are similar if:
          - Identical to either cmdr name or discord name on sheet.
          - Hamming distance is close enough to warrant flagging.

        Args:
            r_scanner: The Recruits scanner object.
            cmdr_name: The name of the commander to look for.
            discord_name: The discord name of the commander to look for.

        Returns:
            (row, similar_cmdr) - If a close match is found amongst names, return row and cmdr name similar.
            (None, None) - No similar cmdr exists in the sheet.
        """
        # Repetition required due to wanting to know row + second column may be empty for a given row
        all_cmdr_names = r_scanner.cells_col_major[0]
        for row, sheet_name in enumerate(all_cmdr_names, start=1):
            if sheet_name and sheet_name in (cmdr_name, discord_name) or \
                    textdistance.hamming(sheet_name, cmdr_name) <= 3:
                return row, sheet_name

        all_discord_names = r_scanner.cells_col_major[1]
        for row, sheet_name in enumerate(all_discord_names, start=1):
            if sheet_name and sheet_name in (cmdr_name, discord_name) or \
                    textdistance.hamming(sheet_name, cmdr_name) <= 3:
                return row, sheet_name

        return None, None

    async def execute(self):
        try:
            cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms("{self.msg.author.mention} You are not an admin!") from exc

        r_scanner = get_scanner('hudson_recruits')
        await r_scanner.update_cells()
        r_scanner.update_first_free()

        cmdr = " ".join(self.args.cmdr)
        discord_name = " ".join(self.args.discord_name) if self.args.discord_name else cmdr
        notes = " ".join(self.args.notes)
        if not re.match(r'.*-\s*\S+$', notes):
            notes += f" -{self.msg.author.name}"

        row = None  # By default there's no similar cmdr.
        if not self.args.force:
            row, similar_cmdr = self.duplicate_verifier(r_scanner, cmdr, discord_name)
        if row:
            dupe_msg = """CMDR {cmdr} is similar to {similar} in row {row}.
Please manually check the recruits sheet {author}.

To bypass this check use the `--force` flag. See `{prefix}recruits -h for information."""
            response = dupe_msg.format(cmdr=cmdr, similar=similar_cmdr, row=row,
                                       author=self.msg.author.mention, prefix=self.bot.prefix)
        else:
            await r_scanner.send_batch(r_scanner.add_recruit_dict(
                cmdr=cmdr,
                discord_name=discord_name,
                rank=self.args.rank,
                platform=self.args.platform,
                pmf=" ".join(self.args.pmf),
                notes=notes,
            ))

            response = f"CMDR {cmdr} has been added to row: {r_scanner.first_free - 1}"

        await self.bot.send_message(self.msg.channel, response)


class Repair(Action):
    """
    Find a nearby station with a shipyard.
    """
    async def execute(self):
        max_dist = cogdb.eddb.DEFAULT_DIST * 3
        if self.args.distance > max_dist:
            raise cog.exc.InvalidCommandArgs(f"Searching beyond **{max_dist}**ly would be too taxing a query.")

        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            stations = await self.bot.loop.run_in_executor(
                None,
                functools.partial(
                    cogdb.eddb.get_shipyard_stations,
                    eddb_session, ' '.join(self.args.system),
                    sys_dist=self.args.distance, arrival=self.args.arrival,
                    include_medium=self.args.medium
                )
            )

        parts = ["No results. Please check system name. Otherwise not near populations."]
        if stations:
            stations = [["System", "Distance", "Station", "Arrival"]] + stations
            parts = cog.tbl.format_table(stations, header=True)
            parts[0] = "__Nearby orbitals__\n" + parts[0]
            suffix = """[L] Large pads.
[M] M pads only.
All stations: Repair, Rearm, Refuel, Outfitting
L stations: Shipyard"""
            parts[-1] += suffix

        for part in parts:
            await self.bot.send_message(self.msg.channel, part)


class Route(Action):
    """
    Find a nearby station with a shipyard.
    """
    async def execute(self):
        # TODO: Add ability to fix endpoint. That is solve route but then add distance to jump back.
        # TODO: Probably allow dupes.
        self.args.system = [arg.lower() for arg in self.args.system]
        system_names = process_system_args(self.args.system)

        if len(system_names) < 2:
            raise cog.exc.InvalidCommandArgs("Need at least __two unique__ systems to plot a course.")

        if len(system_names) != len(set(system_names)):
            raise cog.exc.InvalidCommandArgs("Don't duplicate system names.")

        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            if self.args.optimum:
                result = await self.bot.loop.run_in_executor(
                    None, cogdb.eddb.find_best_route, eddb_session, system_names)
            else:
                result = await self.bot.loop.run_in_executor(
                    None, cogdb.eddb.find_route, eddb_session, system_names[0], system_names[1:])

            lines = ["__Route Plotted__", f"Total Distance: **{round(result[0])}**ly", ""]
            lines += [sys.name for sys in result[1]]

        await self.bot.send_message(self.msg.channel, "\n".join(lines))


class Token(Action):  # pragma: no cover, trivial implementation
    """
    Allow the api session token to be updated by command.
    """
    async def execute(self):
        msg = "Permission denied, user not authorised."

        if self.msg.author.id in cog.util.CONF.scrape.unwrap.get('allowed', []):
            await cog.util.CONF.aupdate('scrape', 'token', value=self.args.token)
            msg = "API Session token updated to config."

        await self.bot.send_message(self.msg.channel, msg)


class Scout(Action):
    """
    Generate scout route.
    """
    async def interact_revise(self, systems):
        """
        Interactively edit the list of systems to scout.

        Returns:
            The list of systems after changes.
        """
        while True:
            l_systems = [sys.lower() for sys in systems]
            try:
                prompt = SCOUT_INTERACT.format('    ' + '\n    '.join(systems))
                responses = [await cog.util.BOT.send_message(self.msg.channel, prompt)]
                user_msg = await cog.util.BOT.wait_for(
                    'message',
                    check=lambda m: m.author == self.msg.author and m.channel == self.msg.channel,
                    timeout=30)

                if user_msg:
                    responses += [user_msg]
                if not user_msg:
                    break

                system = user_msg.content.strip()
                if system.lower() == 'stop':
                    break

                if system.lower() in l_systems:
                    systems = [sys for sys in systems if sys.lower() != system.lower()]
                elif system:
                    systems.append(system)
                    l_systems.append(system.lower())
            finally:
                asyncio.ensure_future(responses[0].channel.delete_messages(responses))

        return systems

    async def execute(self):
        if not self.args.round and not self.args.custom:
            raise cog.exc.InvalidCommandArgs("Select a --round or provide a --custom list.")

        if self.args.custom:
            systems = process_system_args(self.args.custom)
        else:
            systems = SCOUT_RND[self.args.round]
            systems = await self.interact_revise(systems)

        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            result = await self.bot.loop.run_in_executor(
                None, cogdb.eddb.find_best_route, eddb_session, systems)
            system_list = "\n".join([":Exploration: " + sys.name for sys in result[1]])

            now = datetime.datetime.utcnow()
            lines = SCOUT_TEMPLATE.format(
                round(result[0], 2), now.strftime("%B"),
                now.day, now.year + 1286, system_list)

        await self.bot.send_message(self.msg.channel, lines)


class Scrape(Action):
    """
    Interface with the spy_squirrel stuff.
    """
    async def bgs_scrape(self, eddb_session):
        """
        Execute the bgs scrape.
        """
        system_names = process_system_args(self.args.systems)
        found, not_found = cogdb.eddb.get_all_systems_named(eddb_session, system_names)

        msg = f"{len(found)} systems were found and will be updated."
        if not_found:
            msg += f"\n\nThe following systems weren't found: \n{pprint.pformat(not_found)}"
        await self.bot.send_message(self.msg.channel, msg)

        try:
            influence_ids = await spy.post_systems(found, callback=self.msg.channel.send)
            influences = cogdb.eddb.get_influences_by_id(eddb_session, influence_ids)
            scanner = get_scanner('bgs_demo')
            await scanner.clear_cells()
            await scanner.send_batch(scanner.update_dict(influences=influences))
            return "Update completed successfully."
        except cog.exc.RemoteError:
            return "The remote API is down, try again later."

    async def held_for_power(self, eddb_session):
        """
        Scan all held merits for a given power's controls.

        Returns: A message to return to invoker.
        """
        power = cogdb.eddb.get_power_by_name(eddb_session, self.args.name)
        await self.msg.channel.send(f"Will scrape all controls for {power.text} with held merits updated older than {self.args.hours} hours. Ok? Y/N")
        response = await self.bot.wait_for('message', check=lambda msg: msg.author == self.msg.author and msg.channel == self.msg.channel)
        if not response.content.lower().startswith("y"):
            return 'Cancelling power scrape.'

        await spy.execute_power_scrape(eddb_session, power.text, callback=self.msg.channel.send, hours_old=self.args.hours)
        return f'Scheduled scrape for {power.text}.'

    async def execute(self):
        try:
            with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
                if self.args.subcmd == "bgs":
                    msg = await self.bgs_scrape(eddb_session)
                    await self.bot.send_message(self.msg.channel, msg)

                elif self.args.subcmd == "held":
                    msg = await self.held_for_power(eddb_session)
                    await self.bot.send_message(self.msg.channel, msg)

                elif self.args.subcmd == "power":
                    await self.bot.send_message(self.msg.channel,
                                                "Initiated the scrape in the background.")

                    await monitor_powerplay_api(self.bot, repeat=False, delay=0)

                    await self.bot.send_message(self.msg.channel,
                                                "Finished the scrape.")
        except cog.exc.RemoteError:
            return "Could not perform the scrape now. Site is down."


class Status(Action):
    """
    Display the status of this bot.
    """
    async def execute(self):
        lines = [
            ['Created By', 'GearsandCogs'],
            ['Uptime', self.bot.uptime],
            ['Version', f'{cog.__version__}'],
            ['Contributors:', ''],
            ['    Shotwn', 'Inara search'],
            ['    Prozer', 'Various Contributions'],
        ]

        await self.bot.send_message(self.msg.channel, cog.tbl.format_table(lines)[0])


def time_cmd_helper():
    """
    Helper to time fetch and calculation away from main loop.

    Returns: The message for Time command
    """
    now = datetime.datetime.utcnow().replace(microsecond=0)
    weekly_tick = cog.util.next_weekly_tick(now)

    with cogdb.session_scope(cogdb.SideSession) as side_session:
        try:
            tick = cogdb.side.next_bgs_tick(side_session, now)
        except (cog.exc.NoMoreTargets, cog.exc.RemoteError) as exc:
            tick = str(exc)

    return '\n'.join([
        f"Game Time: **{now.strftime('%H:%M:%S')}**",
        tick,
        f'Cycle Ends in **{weekly_tick - now}**',
        'All Times UTC',
    ])


class Time(Action):
    """
    Provide in game time and time to import in game ticks.

    Shows the time ...
    - In game
    - To daily BGS tick
    - To weekly tick
    """
    async def execute(self):
        msg = await self.bot.loop.run_in_executor(
            None, time_cmd_helper
        )

        await self.bot.send_message(self.msg.channel, msg)


class Track(Action):
    """
    Manage the ability to track what carriers are doing.
    """
    async def add(self):
        """ Subcmd add for track command. """
        system_names = process_system_args(self.args.systems)
        cogdb.query.track_add_systems(self.session, system_names, self.args.distance)

        added = []
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            for centre in system_names:
                to_add = cogdb.eddb.get_systems_around(eddb_session, centre, self.args.distance)
                to_add = [x.name for x in to_add]
                add, _ = cogdb.query.track_systems_computed_add(self.session, to_add, centre)
                added += add

        new_systems = sorted(added)
        response = f"__Systems Added To Tracking__\n\nSystems added: {len(new_systems)} First few follow ...\n\n"
        return response + ", ".join(new_systems[:TRACK_LIMIT])

    async def remove(self):
        """ Subcmd remove for track command. """
        system_names = process_system_args(self.args.systems)
        cogdb.query.track_remove_systems(self.session, system_names)

        removed = []
        for centre in system_names:
            deleted, _ = cogdb.query.track_systems_computed_remove(self.session, centre)
            removed += deleted

        removed = sorted(removed)
        response = f"__Systems Removed From Tracking__\n\nSystems removed: {len(removed)} First few follow ...\n\n"
        return response + ", ".join(removed[:TRACK_LIMIT])

    async def ids(self):
        """ Subcmd ids for track command. """
        response = ""

        if self.args.add:
            ids = process_system_args(self.args.add)
            ids_dict = {x: {'id': x, 'squad': " ".join(self.args.squad), 'override': True} for x in ids}
            cogdb.query.track_ids_update(self.session, ids_dict)
            response = "Carrier IDs added successfully to tracking."
        elif self.args.remove:
            cogdb.query.track_ids_remove(self.session, process_system_args(self.args.remove))
            response = "Carrier IDs removed successfully from tracking."
        else:
            with cogdb.query.track_ids_show(self.session) as fname:
                await self.bot.send_message(self.msg.channel, file=discord.File(fp=fname, filename='trackedIDs.txt'))

        return response

    async def show(self):
        """ Subcmd show for track command. """
        for msg in cogdb.query.track_show_systems(self.session):
            await self.bot.send_message(self.msg.channel, msg)

    async def channel(self):
        """ Subcmd channel for track command. """
        await cog.util.CONF.aupdate("channels", "ops", value=self.msg.channel.id)
        return f"Channel set to: {self.msg.channel.name}"

    async def scan(self):
        """ Subcmd scan for track command. """
        scanner = get_scanner("hudson_carriers")
        await scanner.update_cells()
        with cfut.ProcessPoolExecutor(max_workers=1) as pool:
            await self.bot.loop.run_in_executor(
                pool, scanner.scheduler_run
            )

        return "Scan finished."

    async def execute(self):
        try:
            cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not an admin!") from exc

        try:
            func = getattr(self, self.args.subcmd)
            response = await func()
            self.session.commit()
            if response:
                await self.bot.send_message(self.msg.channel, response)
        except (AttributeError, TypeError) as exc:
            self.log.warning("Error for Track: %s", exc)
            raise cog.exc.InvalidCommandArgs("Bad subcommand of `!admin`, see `!admin -h` for help.")


class Trigger(Action):
    """
    Calculate the estimated triggers relative Hudson.
    """
    async def execute(self):
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            self.args.power = " ".join(self.args.power).lower()
            power = cogdb.eddb.get_power_hq(self.args.power)
            pow_hq = cogdb.eddb.get_systems(eddb_session, [power[1]])[0]
            lines = [
                "__Predicted Triggers__",
                f"Power: {power[0]}",
                f"Power HQ: {power[1]}\n",
            ]

            systems = await self.bot.loop.run_in_executor(
                None, cogdb.eddb.get_systems, eddb_session,
                process_system_args(self.args.system))
            for system in systems:
                lines += [
                    cog.tbl.format_table([
                        ["System", system.name],
                        ["Distance", round(system.dist_to(pow_hq), 1)],
                        ["Upkeep", system.calc_upkeep(pow_hq)],
                        ["Fort Trigger", system.calc_fort_trigger(pow_hq)],
                        ["UM Trigger", system.calc_um_trigger(pow_hq)],
                    ])[0]
                ]

        await self.bot.send_message(self.msg.channel, '\n'.join(lines))


class UM(Action):
    """
    Command to show um systems and update status.
    """
    async def execute(self):
        # Sanity check
        if (self.args.set or self.args.offset) and not self.args.system:
            raise cog.exc.InvalidCommandArgs("You forgot to specify a system to update.")

        if self.args.list:
            now = datetime.datetime.utcnow().replace(microsecond=0)
            weekly_tick = cog.util.next_weekly_tick(now)

            prefix = f"**Held Merits**\n\n'DEADLINE **{weekly_tick - now}**'\n"
            held_merits = cogdb.query.um_all_held_merits(self.session, sheet_src=self.args.sheet_src)
            response = cog.tbl.format_table(held_merits, header=True, prefix=prefix)[0]

        elif self.args.system:
            system = cogdb.query.um_find_system(self.session, ' '.join(self.args.system),
                                                sheet_src=self.args.sheet_src)

            record = cogdb.query.add_sheet_record(
                self.session, discord_id=self.msg.author.id, channel_id=self.msg.channel.id,
                command=self.msg.content, sheet_src='um' if self.args.sheet_src == EUMSheet.main else "snipe"
            )
            if self.args.offset:
                system.map_offset = self.args.offset
            if self.args.priority:
                try:
                    cogdb.query.get_admin(self.session, self.duser)
                except cog.exc.NoMatch as exc:
                    raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not an admin!") from exc
                system.priority = " ".join(self.args.priority)
                self.payloads += cogdb.scanners.UMScanner.update_systemum_priority_dict(system.sheet_col, system.priority)
            if self.args.set:
                system.set_status(self.args.set)
            if self.args.set or self.args.offset:
                # TODO: Same payload now, but will have to switch if diverge.
                self.payloads += cogdb.scanners.UMScanner.update_systemum_dict(
                    system.sheet_col, system.progress_us, system.progress_them
                )

            if self.payloads:
                self.session.commit()
                scanner = get_scanner("hudson_undermine" if self.args.sheet_src == EUMSheet.main else "hudson_snipe")
                await scanner.send_batch(self.payloads)
                record.flushed_sheet = True

            response = system.display()

        elif self.args.npcs:
            # Send embed tables of undermining targets, then end the coroutine
            #
            # Note: This needs to send as two embed messages which requires a
            #       different call syntax than other cases.
            #       So we hijack the the control flow, ending the couroutine
            #       before the shared ending send_message

            # Index of the first power to display in the 2nd embed.
            # A single embed can display up to 6 powers
            # therefore, with 9 powers the valid values are 4-7 inclusive.
            split_pos = 5

            embed1 = discord.Embed(title="Undermining Ships")
            for power in UM_NPC_TABLE[1:split_pos]:
                embed1.add_field(name=power[0], value="Power", inline=False)
                embed1.add_field(name=power[1], value="Fighter", inline=True)
                embed1.add_field(name=power[2], value="Transport", inline=True)
                embed1.add_field(name=power[3], value="Expansion", inline=True)
            await self.bot.send_message(self.msg.channel, embed=embed1)

            embed2 = discord.Embed(title="Undermining Ships (cont.)")
            for power in UM_NPC_TABLE[split_pos:]:
                embed2.add_field(name=power[0], value="Power", inline=False)
                embed2.add_field(name=power[1], value="Fighter", inline=True)
                embed2.add_field(name=power[2], value="Transport", inline=True)
                embed2.add_field(name=power[3], value="Expansion", inline=True)
            await self.bot.send_message(self.msg.channel, embed=embed2)

            return

        else:
            systems = cogdb.query.um_get_systems(self.session, sheet_src=self.args.sheet_src, exclude_finished=True)
            response = '__Current Combat / Undermining Targets__\n\n' + '\n'.join(
                [system.display() for system in systems])

        await self.bot.send_message(self.msg.channel, response)


class Snipe(UM):
    """
    Snipe, same as UM but for snipe sheet.
    """
    async def execute(self):
        if self.args.cycle:
            # Limit cycle change to admins
            try:
                cogdb.query.get_admin(self.session, self.duser)
            except cog.exc.NoMatch as exc:
                raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not an admin!") from exc

            # Set the cycle if it exists
            try:
                scanner_name = 'hudson_snipe'
                new_page = self.args.cycle

                try:
                    await cogdb.scanners.SCANNERS[scanner_name].asheet.change_worksheet(new_page)
                except gspread.exceptions.WorksheetNotFound as exc:
                    msg = f"Missing **{new_page}** worksheet on {scanner_name}. Please fix and rerun cycle. No change made."
                    raise cog.exc.InvalidCommandArgs(msg) from exc

                await cog.util.CONF.aupdate("scanners", "hudson_snipe", "page", value=new_page)
                self.bot.sched.schedule(scanner_name, delay=1)
                response = f"Snipe tab set to {new_page}. Snipe scanner scheduled for update.\n\n"
                await self.bot.send_message(self.msg.channel, response)
            except (AssertionError, googleapiclient.errors.HttpError) as exc:
                raise cog.exc.RemoteError(f"The sheet {scanner_name} with tab {new_page} does not exist!") from exc

        else:
            await super().execute()


class User(Action):
    """
    Manage your user settings.
    """
    async def execute(self):
        if self.args.name:
            self.update_name()
        if self.args.cry:
            self.update_cry()

        coros = []
        if self.args.name or self.args.cry:
            if self.duser.fort_user:
                sheet = self.duser.fort_user
                self.payloads += cogdb.scanners.FortScanner.update_sheet_user_dict(
                    sheet.row, sheet.cry, sheet.name)
                scanner = get_scanner("hudson_cattle")
                coros += [scanner.send_batch(self.payloads)]

            if self.duser.um_user:
                sheet = self.duser.um_user
                self.payloads += cogdb.scanners.UMScanner.update_sheet_user_dict(
                    sheet.row, sheet.cry, sheet.name)
                scanner = get_scanner("hudson_undermine")
                coros += [scanner.send_batch(self.payloads)]

            await asyncio.gather(*coros)

        msgs = ['\n'.join([
            f'__{self.msg.author.display_name}__',
            f'Sheet Name: {self.duser.pref_name}',
            f"Default Cry:{' ' + self.duser.pref_cry if self.duser.pref_cry else ''}\n"
            '\n',
        ])]
        if self.duser.fort_user:
            prefix = "\n".join([
                '__Fortification__',
                f'    Cry: {self.duser.fort_user.cry}',
                f'    Total: {self.duser.fort_user.merit_summary()}\n',
            ])
            lines = [['System', 'Amount']]
            lines += [[merit.system.name, merit.amount] for merit in self.duser.fort_user.merits
                      if merit.amount > 0]
            msgs += cog.tbl.format_table(lines, header=True, prefix=prefix)
        if self.duser.um_user:
            prefix = "\n".join([
                '\n__Undermining__',
                f'    Cry: {self.duser.um_user.cry}',
                f'    Total: {self.duser.um_user.merit_summary()}\n',
            ])
            lines = [['System', 'Hold', 'Redeemed']]
            lines += [[merit.system.name, merit.held, merit.redeemed] for merit
                      in self.duser.um_user.merits if merit.held + merit.redeemed > 0]
            msgs += cog.tbl.format_table(lines, header=True, prefix=prefix)

        for msg in cog.util.merge_msgs_to_least(msgs):
            await self.bot.send_message(self.msg.channel, msg)

    def update_name(self):
        """ Update the user's cmdr name in the sheets. """
        new_name = ' '.join(self.args.name)
        self.log.info('USER %s - DUser.pref_name from %s -> %s',
                      self.duser.display_name, self.duser.pref_name, new_name)
        cogdb.query.check_pref_name(self.session, new_name)

        try:
            if self.duser.fort_user:
                self.duser.fort_user.name = new_name
            if self.duser.um_user:
                self.duser.um_user.name = new_name
            self.session.commit()
        except sqlalchemy.exc.IntegrityError as exc:
            raise cog.exc.InvalidCommandArgs("Please try another name, a possible name collision was detected.") from exc

        nduser = cogdb.query.get_duser(self.session, self.duser.id)
        nduser.pref_name = new_name
        self.session.commit()

    def update_cry(self):
        """ Update the user's cry in the sheets. """
        new_cry = ' '.join(self.args.cry)
        self.log.info('USER %s - DUser.pref_cry from %s -> %s',
                      self.duser.display_name, self.duser.pref_cry, new_cry)

        if self.duser.fort_user:
            self.duser.fort_user.cry = new_cry
        if self.duser.um_user:
            self.duser.um_user.cry = new_cry
        self.session.commit()

        nduser = cogdb.query.get_duser(self.session, self.duser.id)
        nduser.pref_cry = new_cry
        self.session.commit()


class Voting(Action):
    """
    Cast a vote based on CMDR discord ID.
    """
    async def execute(self):
        self.duser  # Ensure duser captured for db.
        globe = cogdb.query.get_current_global(self.session)
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            current_vote = spy.get_vote_of_power(eddb_session)

        if self.args.set:
            try:
                cogdb.query.get_admin(self.session, self.duser)
            except cog.exc.NoMatch as exc:
                raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not an admin!") from exc
            globe.vote_goal = self.args.set
            msg = f"New vote goal is **{self.args.set}%**, current vote is {current_vote}%."

        elif self.args.vote_tuple:
            vote_type, amount = cog.parse.parse_vote_tuple(self.args.vote_tuple)
            vote = cogdb.query.add_vote(self.session, self.msg.author.id, vote_type, amount)
            msg = str(vote)

        elif self.args.display:
            msg = self.display(globe)

        elif self.args.summary:
            msg = await self.summary(globe, current_vote)

        else:
            msg = self.vote_direction(globe, current_vote)

        if msg:
            await self.bot.send_message(self.msg.channel, msg)

    def display(self, globe):
        """Display vote goal"""
        try:
            cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not an admin!") from exc

        globe.show_vote_goal = not globe.show_vote_goal
        show_msg = "SHOW" if globe.show_vote_goal else "NOT show"
        return f"Will now {show_msg} the vote goal."

    async def summary(self, globe, current_vote):  # pragma: no cover
        """ Show an executive/complete summary of votes. """
        try:
            cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not an admin!") from exc

        lines = [["CMDR", "Type", "Strength", "Date"]]
        cons_tally, prep_tally = 0, 0
        for vote, duser in cogdb.query.get_all_votes(self.session):
            lines += [[duser.pref_name, vote.vote_type, vote.amount, vote.updated_at]]
            if vote.vote == cogdb.schema.EVoteType.cons:
                cons_tally += vote.amount
            else:
                prep_tally += vote.amount
        now = datetime.datetime.utcnow()

        prefix = f"""__All Votes Cycle {cog.util.current_cycle()}__

Cons: {cons_tally}
Prep: {prep_tally}
Goal: {globe.vote_goal}
Current Consolidation: {current_vote}
Date (UTC): {now}

"""
        with tempfile.NamedTemporaryFile(mode='r') as tfile:
            async with aiofiles.open(tfile.name, 'w') as fout:
                await fout.write('\n'.join(cog.tbl.format_table(lines, prefix=prefix, wrap_msgs=False)))

            await self.msg.channel.send("All votes summary.",
                                        file=discord.File(fp=tfile.name, filename=f"AllVotes.{now.day}_{now.month}_{now.hour}{now.minute}.txt"))

    def vote_direction(self, globe, current_vote):
        """Display vote direction"""
        if globe.show_vote_goal or cog.util.is_near_tick():
            if math.fabs(globe.vote_goal - current_vote) <= 1.0:
                vote_choice = 'Hold your vote (<=1% of goal)'
            elif current_vote > globe.vote_goal:
                vote_choice = 'vote Preparation'
            elif current_vote < globe.vote_goal:
                vote_choice = 'vote Consolidation'
            else:
                vote_choice = 'Hold your vote'
            msg = f"Current vote goal is {globe.vote_goal}%, current consolidation {current_vote}%, please **{vote_choice}**."
        else:
            msg = "Please hold your vote for now. A ping will be sent once we have a final decision."

        return msg


class WhoIs(Action):
    """
    Who is request to Inara for CMDR info.
    """
    async def execute(self):
        cmdr_name = ' '.join(self.args.cmdr)
        kos_info = await cog.inara.api.search_inara_and_kos(cmdr_name, self.msg)

        if kos_info and kos_info.pop('add'):
            await self.moderate_kos_report(kos_info)


class Summary(Action):
    """
    Replace Fort summary to have better control.
    """

    def system_summary(self):
        """ Provide a quick summary of systems. """
        states = cogdb.query.fort_get_systems_by_state(self.session)

        total = len(cogdb.query.fort_get_systems(self.session, ignore_skips=False))
        keys = ['cancelled', 'fortified', 'undermined', 'skipped', 'left', 'almost_done']
        lines = [
            [key.capitalize() for key in keys],
            [f'{len(states[key])}/{total}' for key in keys],
        ]

        return cog.tbl.format_table(lines, sep='|', header=True)[0]

    async def execute(self):
        admin = None
        try:
            admin = cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch:
            pass

        member = self.msg.guild.get_member(self.duser.id)
        role_names = [x.name for x in member.roles]
        if admin or ("FRC Veteran" in role_names):
            response = self.system_summary()
            await self.bot.send_message(self.msg.channel,
                                        self.bot.emoji.fix(response, self.msg.guild))
        else:
            raise cog.exc.InvalidPerms(f"{self.msg.author.mention} You are not allowed to use this command!")


def check_system_deferred_and_globe(system, globe):
    """Retrurn True IFF the system is deferred and conditions met for almost done messages.
    N.B. This includes system can't be priority or prep.

    Args:
        system: A System db object.
        globe: A Globe db object.
    """
    return system.is_deferred and not system.is_priority and not system.is_prep\
        and (not globe.show_almost_done and not cog.util.is_near_tick())


# TODO: I'm not sure why this is "lowered"
def process_system_args(args):
    """
    Process the system args by:
        Joining text on spaces.
        Removing trailing/leading spaces around commas.
        Split on commas and return list of systems.

    Intended to be used when systems collected with nargs=*/+
    """
    system_names = ' '.join(args).lower()
    system_names = re.sub(r'\s*,\s*', ',', system_names)
    return system_names.split(',')


def filter_top_dusers(guild, dusers, exclude_roles, limit=5):
    """
    Generate a top N list from existing DiscordUser list.
    Both lists are guaranteed to be at least limit long and will be
    padded if not enough entries.

    Args:
        guild: The guild to query and get member roles from.
        dusers: The DiscordUser list, of form [[DiscordUser, merits]]
        exclude_roles: List of roles to exclude.
        limit: The top limit will be generated. Default 5.

    Returns: [top_recruits, top_members]
        top_recruits: A list of form [[name, merits], [name, merits]].
        top_members: A list of form [[name, merits], [name, merits]].
    """
    top_recruits, top_members = [], []

    for duser, merits in dusers:
        member = guild.get_member(duser.id)
        if not member:
            continue  # User left or wrong discord id
        role_names = [x.name for x in member.roles]

        # If has an exclude role, ignore
        if len(list(set(role_names) - set(exclude_roles))) != len(role_names):
            continue
        if "FRC Member" in role_names and len(top_members) != limit:
            top_members += [(duser.pref_name, merits)]
        elif "FRC Recruit" in role_names and len(top_recruits) != limit:
            top_recruits += [(duser.pref_name, merits)]

        if len(top_recruits) == limit and len(top_members) == limit:
            break

    while len(top_recruits) != limit:
        top_recruits += [('', '')]
    while len(top_members) != limit:
        top_members += [('', '')]

    return top_recruits, top_members


async def monitor_carrier_events(client, *, next_summary, last_timestamp=None, delay=60):  # pragma: no cover
    """
    Simple async task that just checks for new events every delay.

    Args:
        client: The bot.
        next_summary: Datetime object representing next time to do daily summary.
        last_seen_time: Last known timestamp for a TrackByID.
        delay: The short delay between normal summaries, in seconds.
    """
    if not last_timestamp:
        last_timestamp = datetime.datetime.utcnow()

    while True:
        await asyncio.sleep(delay)

        with cogdb.session_scope(cogdb.Session) as session:
            if datetime.datetime.utcnow() < next_summary:
                header = f"__Fleet Carriers Detected Last {delay} Seconds__\n"
                tracks = await client.loop.run_in_executor(
                    None, cogdb.query.track_ids_newer_than, session, last_timestamp
                )
                if tracks:
                    await report_to_leadership(client, header + '\n'.join([str(x) for x in tracks]))

            else:
                header = f"__Daily Fleet Carrier Summary For {next_summary}__\n"
                yesterday = next_summary - datetime.timedelta(days=1)
                next_summary = next_summary + datetime.timedelta(days=1)
                tracks = await client.loop.run_in_executor(
                    None, cogdb.query.track_ids_newer_than, session, yesterday
                )

                if tracks:
                    last_timestamp = tracks[-1].updated_at
                    with tempfile.NamedTemporaryFile() as tfile:
                        async with aiofiles.open(tfile.name, 'w', encoding='utf-8') as fout:
                            await fout.write(header)
                            await fout.writelines([f'\n{track}' for track in tracks])

                        await report_to_leadership(client, header.replace('__', ''),
                                                   file=discord.File(fp=tfile.name, filename='recentCarriers.txt'))


async def monitor_snipe_merits(client, *, repeat=True):  # pragma: no cover
    """
    Schedule self to check snipe merits at the following times.
    This task will sleep until required.
        - 12 hours before tick, remind everyone with @here ping to redeem.
        - 30 minutes before tick, ping unredeemed users directly.

    Kwargs:
        repeat: If true, will schedule itself infinitely.
    """
    def compute_delay_seconds(next_date, message):
        """ Just compute delay required. """
        now = datetime.datetime.utcnow()
        diff_dates = next_date - now
        delay_seconds = diff_dates.seconds + diff_dates.days * 24 * 60 * 60
        log.warning("Sleeping for %d seconds until %s. Next event is %s", delay_seconds, next_date, message)

        return delay_seconds

    snipe_chans = [client.get_channel(x) for x in cog.util.CONF.channels.snipe]
    if not snipe_chans:  # Don't bother if not set
        return

    log = logging.getLogger(__name__)
    for snipe_chan in snipe_chans:
        log.error('Snipe Reminder Channel: %s', snipe_chan)

    while repeat:
        #  # 12 hours to tick, ping here
        now = datetime.datetime.utcnow()
        next_cycle = cog.util.next_weekly_tick(now)
        next_date = next_cycle - datetime.timedelta(hours=12)
        if now < next_date:
            await asyncio.sleep(
                compute_delay_seconds(next_date, "12 hour reminder")
            )

            # Notify here
            log.warning("Issuing 12 hour notice to snipe channel.")
            with cogdb.session_scope(cogdb.Session) as session:
                if cogdb.scanners.get_scanner("hudson_undermine").asheet.sheet_page == cogdb.scanners.get_scanner("hudson_snipe").asheet.sheet_page and cogdb.query.get_all_snipe_holds(session):
                    for chan in snipe_chans:
                        await client.send_message(
                            chan,
                            "@here Snipe members it is tick day and there are 12 hours remaining."
                        )

        # 30 mins to tick ping remaining in message
        next_date = next_cycle - datetime.timedelta(minutes=30)
        now = datetime.datetime.utcnow()
        if now < next_date:
            await asyncio.sleep(
                compute_delay_seconds(next_date, "30 min reminder")
            )

            # Notify members holding
            log.warning("Issuing 30 min notice to snipe channel.")
            with cogdb.session_scope(cogdb.Session) as session:
                if cogdb.scanners.get_scanner("hudson_undermine").asheet.sheet_page == cogdb.scanners.get_scanner("hudson_snipe").asheet.sheet_page:
                    msg = """__Final Snipe Reminder__
        There are less than **30 minutes left**. The following members are still holding.
        """
                    for reminder in cogdb.query.get_snipe_members_holding(session):
                        for chan in snipe_chans:
                            await client.send_message(chan, msg + reminder)

        # Sleep until 5 mins after tick
        next_date = next_cycle + datetime.timedelta(minutes=5)
        await asyncio.sleep(
            compute_delay_seconds(next_date, "idle until 5 minutes after tick")
        )


async def push_spy_to_gal_scanner():  # pragma: no cover | tested elsewhere
    """
    Push spy information into the gal scanner sheet.
    """
    gal_scanner = cogdb.scanners.get_scanner("hudson_gal")
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        powers = eddb_session.query(cogdb.eddb.Power).\
            filter(cogdb.eddb.Power.text != "None").\
            order_by(cogdb.eddb.Power.eddn).\
            all()

        loop = asyncio.get_event_loop()
        for power in powers:
            systems, preps, expansions, vote = await loop.run_in_executor(
                None, spy.get_spy_systems_for_galpow, eddb_session, power.id
            )

            logging.getLogger(__name__).error("Updating sheet for: %s", power.eddn)
            await gal_scanner.asheet.change_worksheet(power.eddn.upper())
            await gal_scanner.clear_cells()
            await gal_scanner.send_batch(gal_scanner.update_dict(systems=systems, preps=preps, exps=expansions, vote=vote))


async def push_spy_to_sheets():  # pragma: no cover | tested elsewhere
    """
    Push the spy information into the fort and um sheets if needed.
    """
    log = logging.getLogger(__name__)

    with cogdb.session_scope(cogdb.Session) as session,\
         cogdb.session_scope(cogdb.EDDBSession) as eddb_session:

        log.error("Processing scrape results to sheets")
        forts = spy.compare_sheet_fort_systems_to_spy(session, eddb_session)
        if forts:
            scanner = cogdb.scanners.get_scanner("hudson_cattle")
            payloads = scanner.bulk_update_fort_status(forts)
            log.error("Fort sheet will be updated.")
            await scanner.send_batch(payloads)

        umsystems = spy.compare_sheet_um_systems_to_spy(session, eddb_session, sheet_src=EUMSheet.main)
        if umsystems:
            __import__('pprint').pprint(umsystems)
            scanner = cogdb.scanners.get_scanner("hudson_undermine")
            payloads = []
            for umsys in umsystems:
                payloads += scanner.update_systemum_dict(
                    umsys['sheet_col'], umsys['progress_us'], umsys['progress_them']
                )
            print("UM Payloads")
            __import__('pprint').pprint(payloads)
            log.error("Operations sheet will be updated.")
            await scanner.send_batch(payloads, input_opt='USER_ENTERED')

        scanner = cogdb.scanners.get_scanner("hudson_snipe")
        try:
            snipe_cycle = int(scanner.asheet.sheet_page[1:])  # Format: "C384"
            if cog.util.current_cycle() == snipe_cycle:
                umsystems = spy.compare_sheet_um_systems_to_spy(session, eddb_session, sheet_src=EUMSheet.snipe)
                __import__('pprint').pprint(umsystems)
                payloads = []
                for umsys in umsystems:
                    payloads += scanner.update_systemum_dict(
                        umsys['sheet_col'], umsys['progress_us'], umsys['progress_them']
                    )
                print("Snipe Payloads")
                __import__('pprint').pprint(payloads)
                log.error("Snipe sheet will be updated.")
                await scanner.send_batch(payloads, input_opt='USER_ENTERED')
        except ValueError:
            pass  # Cycle not available in named page


async def monitor_powerplay_api(client, *, repeat=True, delay=1800):
    """Poll the powerplay page for info every delay seconds.

    N.B. This depends on multiple scanners being operable. Start this task ONLY when they are ready.

    Args:
        client: The discord.py client.
        repeat: If True schedule self at end of execution to run again.
        delay: The delay in seconds between checks.
    """
    log = logging.getLogger(__name__)
    last_base = None
    log.warning("Started powerplay monitor.")

    while True:
        await asyncio.sleep(delay)
        params = {'token': cog.util.CONF.scrape.token}

        try:
            with cfut.ProcessPoolExecutor(max_workers=4) as pool:
                try:
                    if last_base != cog.util.current_cycle():
                        last_base = cog.util.current_cycle()
                        log.warning("Fetching base.json.")
                        base_text = await cog.util.get_url(os.path.join(cog.util.CONF.scrape.api, 'getraw', 'base.json'), params=params)
                        await client.loop.run_in_executor(
                            pool, spy.load_base_json, base_text,
                        )

                    log.warning("Fetching refined.json.")
                    ref_text = await cog.util.get_url(os.path.join(cog.util.CONF.scrape.api, 'getraw', 'refined.json'), params=params)
                    await client.loop.run_in_executor(
                        pool, spy.load_refined_json, ref_text,
                    )
                except (asyncio.CancelledError, asyncio.InvalidStateError) as exc:
                    log.error("Error with future: %s", str(exc))

            log.warning("Handle sheet updates.")
            await push_spy_to_gal_scanner()
            await push_spy_to_sheets()

            log.warning("Check held for federal.")
            await spy.check_federal_held()
            log.warning("End monitor powerplay.")
        except cog.exc.RemoteError:
            if repeat:
                log.error("Spy service not operating. Will try again in %d seconds.", delay)
        except:  # noqa: E722, pylint: disable=bare-except
            traceback.print_exc()
            log.error("CRIT ERROR POWMON: %s", traceback.format_exc())

        if not repeat:
            break


async def monitor_spy_site(client, *, repeat=True, delay=900):
    """Poll the powerplay page for info every delay seconds.

    When site fails to load, IMMEDIATELY ping for help.

    Args:
        client: The discord.py client.
        repeat: If True schedule self at end of execution to run again.
        delay: The delay in seconds between checks.
    """
    log = logging.getLogger(__name__)
    working = True
    last_working = datetime.datetime.utcnow()

    while repeat:
        await asyncio.sleep(delay)
        try:
            params = {'token': cog.util.CONF.scrape.token}
            await cog.util.get_url(os.path.join(cog.util.CONF.scrape.api, 'getraw', 'base.json'), params=params)
            if not working:
                diff_time = datetime.datetime.utcnow() - last_working
                hours = diff_time.seconds // 3600 + diff_time.days * 24
                msg = f"Spy service restored. Outage began at {last_working}. Lasted {hours} hours."
                log.error(msg)
                await cog.util.emergency_notice(client, msg)
            working = True
            last_working = datetime.datetime.utcnow()
        except cog.exc.RemoteError:
            if working:
                msg = "Spy service is suspected offline. If expected please ignore."
                log.error(msg)
                await cog.util.emergency_notice(client, msg)
            working = False


async def report_to_leadership(client, msg, **kwargs):  # pragma: no cover
    """
    Send messages to the channel configured to receive reports.

    Args:
        client: The bot client.
        msgs: A list of messages, each should be under discord char limit.
    """
    chan_id = cog.util.CONF.channels.ops
    if chan_id:
        chan = client.get_channel(chan_id)
        await client.send_message(chan, msg, **kwargs)


SCOUT_RND = {  # TODO: Extract to data config or tables.
    1: [
        "Epsilon Scorpii",
        "39 Serpentis",
        "Parutis",
        "Mulachi",
        "Aornum",
        "WW Piscis Austrini",
        "LHS 142",
        "LHS 6427",
        "LP 580-33",
        "BD+42 3917",
        "Venetic",
        "Kaushpoos",
    ],
    2: [
        "Atropos",
        "Alpha Fornacis",
        "Rana",
        "Anlave",
        "NLTT 46621",
        "16 Cygni",
        "Adeo",
        "LTT 15449",
        "LHS 3447",
        "Lalande 39866",
        "Abi",
        "Gliese 868",
        "Othime",
        "Phra Mool",
        "Wat Yu",
        "Shoujeman",
        "Phanes",
        "Dongkum",
        "Nurundere",
        "LHS 3749",
        "Mariyacoch",
        "Frey",
    ],
    3: [
        "GD 219",
        "Wolf 867",
        "Gilgamesh",
        "LTT 15574",
        "LHS 3885",
        "Wolf 25",
        "LHS 6427",
        "LHS 1541",
        "LHS 1197",
    ]
}
SCOUT_TEMPLATE = """__**Scout List**__
Total Distance: **{}**ly

```**REQUESTING NEW RECON OBJECTIVES __{} {}__  , {}**

If you are running more than one system, do them in this order and you'll not ricochet the whole galaxy. Also let us know if you want to be a member of the FRC Scout Squad!
@here @FRC Scout

{}

:o7:```"""
SCOUT_INTERACT = """Will generate scout list with the following systems:\n

{}

To add system: type system name **NOT** in list
To remove system: type system name in list
To generate list: reply with **stop**

__This message will delete itself on success or 30s timeout.__"""
UM_NPC_TABLE = [
    ['Power', 'UM Fighter', 'UM Transprort', 'Expansion'],
    ['A.Lavigny-Duval', 'Shield of Justice', 'Imperial Supply', 'Imperial Enforcers'],
    ['Aisling Duval', 'Aislings Guardian', 'Campaign Ship', 'Aisling\'s Angels'],
    ['Denton Patreus', 'Patreus Sentinel', 'Imperial Support', 'Imperial Warships'],
    ['Zemina Torval', 'Torvals Shield', 'Private Security', 'Torval\'s Brokers'],
    ['Yuri Grom', 'EGP Operative', 'EGP Agents', 'Enforcer Warships'],
    ['Edmund Mahon', 'Alliance Enforcer', 'Alliance Diplomat', 'Alliance Diplomats'],
    ['Li Yong-Rui', 'Sirius Security', 'Sirius Transport', 'Corporation ships'],
    ['Pranav Antal', 'Utopian Overseer', 'Reform Ships', 'Utopian Agitators'],
    ['Archon Delaine', 'Kumo Crew Watch', 'Kumo Crew Transport', 'Kumo Crew Strike Ships']
]
TRACK_LIMIT = 20
FUC_GUILD = "Federal United Command"
