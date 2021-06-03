"""
To facilitate complex actions based on commands create a
hierarchy of actions that can be recombined in any order.
All actions have async execute methods.
"""
import asyncio
import datetime
import functools
import logging
import re
import string
import tempfile

import aiofiles
import decorator
import discord
import googleapiclient.errors
import sqlalchemy.exc

import cogdb
import cogdb.eddb
import cogdb.query
import cogdb.scanners
import cogdb.side
import cog.inara
import cog.tbl
import cog.util
from cogdb.schema import FortUser, UMUser


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
        ['Username', '{}#{}'.format(user.name, user.discriminator)],
        ['ID', str(user.id)],
        ['Status', str(user.status)],
        ['Join Date', str(user.joined_at)],
        ['All Roles:', str([str(role) for role in user.roles[1:]])],
        ['Top Role:', str(user.top_role).replace('@', '@ ')],
    ]
    return cog.tbl.format_table(lines, prefix='**{}**\n'.format(user.display_name))


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


def check_sheet(scanner_name, attr, user_cls):
    """ Check if user present in sheet. """
    @decorator.decorator
    async def inner(coro, *args, **kwargs):
        """ The actual decorator. """
        self = args[0]
        if not getattr(self.duser, attr):
            self.log.info('USERS %s - Adding to %s as %s.',
                          self.duser.display_name, user_cls.__name__, self.duser.pref_name)
            sheet = cogdb.query.add_sheet_user(
                self.session, cls=user_cls, discord_user=self.duser,
                start_row=get_scanner(scanner_name).user_row
            )

            self.payloads += get_scanner(scanner_name).__class__.update_sheet_user_dict(
                sheet.row, sheet.cry, sheet.name)

            notice = 'Will automatically add {} to sheet. See !user command to change.'.format(
                self.duser.pref_name)
            asyncio.ensure_future(self.bot.send_message(self.msg.channel, notice))

        await coro(*args, **kwargs)

    return inner


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

    @property
    def cattle(self):
        """ User's current cattle sheet. """
        return self.duser.fort_user

    @property
    def undermine(self):
        """ User's current undermining sheet. """
        return self.duser.um_user

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
        cmd_set = sorted([cls.__name__ for cls in cog.actions.Action.__subclasses__()])
        cmd_set.remove('Admin')  # Admin cannot be restricted even by admins
        if not self.args.rule_cmd or self.args.rule_cmd not in cmd_set:
            raise cog.exc.InvalidCommandArgs("Rules require a command in following set: \n\n%s"
                                             % str(cmd_set))

    async def add(self):
        """
        Takes one of the following actions:
            1) Add 1 or more admins
            2) Add a single channel rule
            3) Add a single role rule
        """
        if not self.args.rule_cmd and self.msg.mentions:
            for member in self.msg.mentions:
                cogdb.query.add_admin(self.session, member)
            response = "Admins added:\n\n" + '\n'.join([member.name for member in self.msg.mentions])

        else:
            self.check_cmd()

            if self.msg.channel_mentions:
                cogdb.query.add_channel_perm(self.session, self.args.rule_cmd,
                                             self.msg.channel.guild,
                                             self.msg.channel_mentions[0])
                response = "Channel permission added."

            elif self.args.role:
                cogdb.query.add_role_perm(self.session, self.args.rule_cmd,
                                          self.msg.channel.guild,
                                          self.msg.role_mentions[0])
                response = "Role permission added."

        return response

    async def remove(self, admin):
        """
        Takes one of the following actions:
            1) Remove 1 or more admins
            2) Remove a single channel rule
            3) Remove a single role rule
        """
        if not self.args.rule_cmd and self.msg.mentions:
            for member in self.msg.mentions:
                admin.remove(self.session, cogdb.query.get_admin(self.session, member))
            response = "Admins removed:\n\n" + '\n'.join([member.name for member in self.msg.mentions])

        else:
            self.check_cmd()

            if self.msg.channel_mentions:
                cogdb.query.remove_channel_perm(self.session, self.args.rule_cmd,
                                                self.msg.channel.guild,
                                                self.msg.channel_mentions[0])
                response = "Channel permission removed."

            elif self.args.role:
                cogdb.query.remove_role_perm(self.session, self.args.rule_cmd,
                                             self.msg.channel.guild,
                                             self.msg.role_mentions[0])
                response = "Role permission removed."

        return response

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

        after = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=self.args.days)
        last_msgs = {}
        for channel in self.msg.channel_mentions:
            try:
                async for msg in channel.history(limit=100000, after=after, oldest_first=True):
                    last_msgs[msg.author.id] = msg
            except discord.errors.Forbidden as exc:
                raise cog.exc.InvalidCommandArgs("Bot has no permissions for channel: " + channel.name) from exc

        for msg in last_msgs.values():
            all_members.remove(msg.author)

        all_members = sorted(all_members, key=lambda x: x.name.lower())
        all_members = sorted(all_members, key=lambda x: x.top_role.name)
        try:
            tfile = tempfile.NamedTemporaryFile(mode='r')
            async with aiofiles.open(tfile.name, 'w') as fout:
                await fout.write("__Members With No Activity in Last {} Day{}__\n".format(self.args.days,
                                                                                          "" if self.args.days == 1 else "s"))
                for member in all_members:
                    await fout.write("{}, Top Role: {}\n".format(member.name, member.top_role))

                await fout.write("\n\n__Members With Activity__\n")
                for msg in sorted(last_msgs.values(), key=lambda x: x.created_at):
                    await fout.write("{}, Top Role: {}, Last Msg Sent on {} in {}\n".format(
                        msg.author.name, msg.author.top_role, msg.created_at, msg.channel.name))

            fname = 'activity_report_{}_{}.txt'.format(
                self.msg.guild.name, datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0))
            await self.msg.channel.send("Report generated in this file.",
                                        file=discord.File(fp=tfile.name, filename=fname))
            await asyncio.sleep(5)
        finally:
            tfile.close()

    async def cast(self):
        """ Broacast a message accross a server. """
        await self.bot.broadcast(' '.join(self.args.content))
        return 'Broadcast completed.'

    async def deny(self):
        """ Toggle bot's acceptance of commands. """
        self.bot.deny_commands = not self.bot.deny_commands
        return 'Commands: **{}abled**'.format('Dis' if self.bot.deny_commands else 'En')

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
        cycle = cog.util.get_config("scanners", "hudson_cattle", "page", default="Cycle Unknown")
        prefix = "__Top Merits for {}__\n\n".format(cycle)
        exclude_roles = ["FRC Leadership", "Special Agent"]
        parts = []

        top_all = await self.bot.loop.run_in_executor(
            None, cogdb.query.users_with_all_merits, self.session,
        )
        top_recruits, top_members = filter_top_dusers(self.msg.guild, top_all, exclude_roles, limit=self.args.limit)
        lines = [["Top {} Recruits".format(limit), "Merits", "Top {} Members".format(limit), "Merits"]]
        lines += [[rec[0], rec[1], mem[0], mem[1]] for rec, mem in zip(top_recruits, top_members)]
        parts += cog.tbl.format_table(lines, header=True, prefix=prefix, suffix="\n\n")

        top_fort = await self.bot.loop.run_in_executor(
            None, cogdb.query.users_with_fort_merits, self.session,
        )
        top_recruits, top_members = filter_top_dusers(self.msg.guild, top_fort, exclude_roles, limit=self.args.limit)
        lines = [["Top {} Fort Recruits".format(limit), "Merits", "Top Fort {} Members".format(limit), "Merits"]]
        lines += [[rec[0], rec[1], mem[0], mem[1]] for rec, mem in zip(top_recruits, top_members)]
        parts += cog.tbl.format_table(lines, header=True, suffix="\n\n")

        top_um = await self.bot.loop.run_in_executor(
            None, cogdb.query.users_with_all_merits, self.session,
        )
        top_recruits, top_members = filter_top_dusers(self.msg.guild, top_um, exclude_roles, limit=self.args.limit)
        lines = [["Top {} UM Recruits".format(limit), "Merits", "Top UM {} Members".format(limit), "Merits"]]
        lines += [[rec[0], rec[1], mem[0], mem[1]] for rec, mem in zip(top_recruits, top_members)]
        parts += cog.tbl.format_table(lines, header=True, suffix="\n\n")

        for part in cog.util.merge_msgs_to_least(parts):
            await self.bot.send_message(self.msg.channel, part)

    # TODO: Increase level of automation:
    #   - Actually MAKE the new sheets, copy from templates.
    #   - Turn on import in new sheet, turn off import in older sheet.
    async def cycle(self):
        """
        Rollover scanners to new sheets post cycle tick.

        Configs will be modified and scanners re-initialized.

        Raises:
            InternalException - No parseable numeric component found in tab.
            RemoteError - The sheet/tab combination could not be resolved. Tab needs creating.
        """
        self.bot.deny_commands = True
        scanner_configs = cog.util.get_config('scanners')
        lines = [['Document', 'Active Page']]

        try:
            for name in ['hudson_cattle', 'hudson_undermine']:
                new_page = cog.util.number_increment(scanner_configs[name]['page'])
                scanner_configs[name]['page'] = new_page
                await SCANNERS[name].asheet.change_worksheet(new_page)
                self.bot.sched.schedule(name, delay=1)
                lines += [[await SCANNERS[name].asheet.title(), new_page]]
            cog.util.update_config(scanner_configs, 'scanners')

            prefix = "Cycle incremented. Changed sheets scheduled for update.\n\n"
            return cog.tbl.format_table(lines, header=True, prefix=prefix)[0]
        except ValueError as exc:
            raise cog.exc.InternalException("Impossible to increment scanner: {}".format(name)) from exc
        except (AssertionError, googleapiclient.errors.HttpError) as exc:
            raise cog.exc.RemoteError("The sheet {} with tab {} does not exist!".format(
                name, scanner_configs[name]['page'])) from exc
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
                            ["UM Trigger {}%".format(reinforcement_value), reinforced_trigger],
                            ["Priority", priority]
                        ])
                        values.append({"sys_name": system.name, "power": power[0], "trigger": reinforced_trigger,
                                      "priority": priority})
                    else:
                        found_list.append(system.name)

        if values:
            um_sheet = await um_scanner.get_batch(['D1:13'], 'COLUMNS', 'FORMULA')
            data = cogdb.scanners.UMScanner.slide_templates(um_sheet, values)
            await um_scanner.send_batch(data, input_opt='USER_ENTERED')
            self.bot.sched.schedule("hudson_undermine", 1)
            msgs = cog.util.merge_msgs_to_least(msgs)
            for msg in cog.util.merge_msgs_to_least(msgs):
                await self.bot.send_message(self.msg.channel, msg)
            await asyncio.sleep(1)
            if found_list:
                return "Systems added to the UM sheet.\n\nThe following systems were ignored : {}"\
                    .format(", ".join(found_list))
            return 'Systems added to the UM sheet.'

        return 'All systems asked are already in the sheet or are invalid'

    async def removeum(self):
        """Remove a system(s) from the um sheet"""
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            systems = await self.bot.loop.run_in_executor(
                None, cogdb.eddb.get_systems, eddb_session,
                process_system_args(self.args.system))

            um_scanner = get_scanner("hudson_undermine")
            systems_in_sheet = cogdb.query.um_get_systems(self.session, exclude_finished=False)
            found_list = []
            unlisted_system = []
            for system in systems:
                found = False
                for system_in_sheet in systems_in_sheet:
                    if system_in_sheet.name == system.name:
                        found = True
                        found_list.append(system.name)
                if not found:
                    unlisted_system.append(system.name)

            if found_list:
                um_sheet = await um_scanner.get_batch(['D1:13'], 'COLUMNS', 'FORMULA')
                data = cogdb.scanners.UMScanner.remove_um(um_sheet, found_list)
                await um_scanner.send_batch(data, input_opt='USER_ENTERED')
                self.bot.sched.schedule("hudson_undermine", 1)
                await asyncio.sleep(1)
                if unlisted_system:
                    return "Systems removed from the UM sheet.\n\nThe following systems were not found : {}" \
                        .format(", ".join(unlisted_system))
                return 'Systems removed from the UM sheet.'

            return 'All systems asked are not on the sheet'

    async def execute(self):
        try:
            admin = cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms("{} You are not an admin!".format(self.msg.author.mention)) from exc

        try:
            func = getattr(self, self.args.subcmd)
            if self.args.subcmd == "remove":
                response = await func(admin)
            else:
                response = await func()
            if response:
                await self.bot.send_message(self.msg.channel, response)
        except (AttributeError, TypeError) as exc:
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
                '{:.1f}'.format(inf.influence), net_inf[system.name],
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
            ["Strong", "{}/{}".format(strong_cnt, tot_systems)],
            ["Weak", "{}/{}".format(weak_cnt, tot_systems)],
            ["Neutral", "{}/{}".format(tot_systems - strong_cnt - weak_cnt, tot_systems)],
        ]
        explain = """
**Net**: Net change in influence over last 5 days. There may not be 5 days of data.
         If Net == Inf, they just took control.
**N**: The number of factions present in a system.
**Pop**: log10(population), i.e. log10(10000) = 4.0
         This is the exponent that would carry 10 to the population of the system.
         Example: Pop = 4.0 then actual population is: 10 ^ 4.0 = 10000
        """
        msgs = cog.tbl.format_table(hlines, prefix="**{}**".format(control.name))
        msgs += cog.tbl.format_table(lines, header=True, suffix=explain)

        return cog.util.merge_msgs_to_least(msgs)[0]

    async def edmc(self, system_name, **kwargs):
        """ Handle edmc subcmd. """
        if not system_name:
            controls = cogdb.side.WATCH_BUBBLES
        else:
            controls = process_system_args(system_name.split(' '))

        resp = "__**EDMC Route**__\nIf no systems listed under control, up to date."
        resp += "\n\n__Bubbles By Proximity__\n"
        if len(controls) > 2:
            _, route = await self.bot.loop.run_in_executor(None, cogdb.eddb.find_best_route,
                                                           kwargs['eddb_session'], controls)
            controls = [sys.name for sys in route]
        resp += "\n".join(controls)

        for control in controls:
            resp += "\n\n__{}__\n".format(string.capwords(control))
            systems = await self.bot.loop.run_in_executor(None, cogdb.side.get_edmc_systems,
                                                          kwargs['side_session'], [control])
            if len(systems) > 2:
                _, systems = await self.bot.loop.run_in_executor(None, cogdb.eddb.find_best_route,
                                                                 kwargs['eddb_session'],
                                                                 [system.name for system in systems])
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
            prompt += "\n({}) {}".format(ind, name)
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
            prefix = "**Would Expand To**\n\n{}, {}\n\n".format(centre.name, factions[ind].name)
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

        prefix = "**{}**\n{} (UTC)\n\n".format(system_name, infs[0][-1])
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

        title = "BGS Report {}".format(datetime.datetime.now(datetime.timezone.utc))
        paste_url = await cog.util.pastebin_new_paste(title, report)

        return "Report Generated: <{}>".format(paste_url)

    async def sys(self, system_name, **kwargs):
        """ Handle sys subcmd. """
        self.log.info('BGS - Looking for overview like: %s', system_name)
        system, factions = await self.bot.loop.run_in_executor(None, cogdb.side.system_overview,
                                                               kwargs['side_session'], system_name)

        if not system:
            raise cog.exc.InvalidCommandArgs("System **{}** not found. Spelling?".format(system_name))
        if not factions:
            msg = """We aren't tracking influence in: **{}**

If we should contact Gears or Sidewinder""".format(system_name)
            raise cog.exc.InvalidCommandArgs(msg)

        lines = []
        for faction in factions:
            lines += [
                '{}{}: {} -> {}'.format(faction['name'], ' (PMF)' if faction['player'] else '',
                                        faction['state'], faction['pending']),
            ]
            if faction['stations']:
                lines += ['    Owns: ' + ', '.join(faction['stations'])]
            lines += [
                '    ' + ' | '.join(['{:^5}'.format(inf.short_date) for inf in faction['inf_history']]),
                '    ' + ' | '.join(['{:^5.1f}'.format(inf.influence) for inf in faction['inf_history']]),
            ]

        header = "**{}**: {:,}\n\n".format(system.name, system.population)
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
            response = exc.reply()


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

        prefix = 'Distances From: **{}**\n\n'.format(system_names[0].capitalize())
        lines = [[name, '{:.2f}ly'.format(dist)] for name, dist in dists]
        for msg in cog.tbl.format_table(lines, prefix=prefix):
            await self.bot.send_message(self.msg.channel, msg)


class Drop(Action):
    """
    Handle the logic of dropping a fort at a target.
    """
    def finished(self, system):
        """
        Additional reply when a system is finished (i.e. deferred or 100%).
        """
        try:
            new_target = cogdb.query.fort_get_targets(self.session)[0]
            response = '\n\n__Next Fort Target__:\n' + new_target.display()
        except cog.exc.NoMoreTargets:
            response = '\n\n Could not determine next fort target.'

        lines = [
            '**{}** Have a :cookie: for completing {}'.format(self.duser.display_name, system.name),
        ]

        try:
            merits = list(reversed(sorted(system.merits)))
            top = merits[0]
            lines += ['Bonus for highest contribution:']
            for merit in merits:
                if merit.amount != top.amount:
                    break
                lines.append('    :cookie: for **{}** with {} supplies'.format(
                    merit.user.name, merit.amount))
        except IndexError:
            lines += ["No found contributions. Heres a :cookie: for the unknown commanders."]

        response += '\n\n' + '\n'.join(lines)

        return response

    @check_mentions
    @check_sheet('hudson_cattle', 'fort_user', FortUser)
    async def execute(self):
        """
        Drop forts at the fortification target.
        """
        self.log.info('DROP %s - Matched duser with id %s and sheet name %s.',
                      self.duser.display_name, self.duser.id, self.cattle)

        system = cogdb.query.fort_find_system(self.session, ' '.join(self.args.system))
        self.log.info('DROP %s - Matched system %s from: \n%s.',
                      self.duser.display_name, system.name, system)

        drop = cogdb.query.fort_add_drop(self.session, system=system,
                                         user=self.cattle, amount=self.args.amount)
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
        self.log.info('DROP %s - Sucessfully dropped %d at %s.',
                      self.duser.display_name, self.args.amount, system.name)

        response = system.display()
        if system.is_fortified:
            response += self.finished(system)
        await self.bot.send_message(self.msg.channel,
                                    self.bot.emoji.fix(response, self.msg.guild))


class Fort(Action):
    """
    Provide information on and manage the fort sheet.
    """
    def find_missing(self, left):
        """ Show systems with 'left' remaining. """
        lines = ['__Systems Missing {} Supplies__'.format(left)]

        for system in cogdb.query.fort_get_systems(self.session):
            if not system.is_fortified and not system.skip and system.missing <= left:
                lines.append(system.display(miss=True))

        return '\n'.join(lines)

    def system_summary(self):
        """ Provide a quick summary of systems. """
        states = cogdb.query.fort_get_systems_by_state(self.session)

        total = len(cogdb.query.fort_get_systems(self.session))
        keys = ['cancelled', 'fortified', 'undermined', 'skipped', 'left']
        lines = [
            [key.capitalize() for key in keys],
            ['{}/{}'.format(len(states[key]), total) for key in keys],
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

    async def execute(self):
        manual = ' (Manual Order)' if cogdb.query.fort_order_get(self.session) else ''
        if self.args.summary:
            response = self.system_summary()

        elif self.args.set:
            system_name = ' '.join(self.args.system)
            if ',' in system_name:
                raise cog.exc.InvalidCommandArgs('One system at a time with --set flag')

            system = cogdb.query.fort_find_system(self.session, system_name)
            system.set_status(self.args.set)
            self.session.commit()

            self.payloads += cogdb.scanners.FortScanner.update_system_dict(
                system.sheet_col, system.fort_status, system.um_status
            )
            scanner = get_scanner("hudson_cattle")
            await scanner.send_batch(self.payloads)

            response = system.display()

        elif self.args.miss:
            response = self.find_missing(self.args.miss)

        elif self.args.details:
            response = self.system_details()

        elif self.args.order:
            cogdb.query.fort_order_drop(self.session,
                                        cogdb.query.fort_order_get(self.session))
            if self.args.system:
                system_names = process_system_args(self.args.system)
                cogdb.query.fort_order_set(self.session, system_names)
                response = """Fort order has been manually set.
When all systems completed order will return to default.
To unset override, simply set an empty list of systems.
"""
            else:
                response = "Manual fort order unset. Resuming normal order."

        elif self.args.system:
            lines = ['__Search Results__']
            for name in process_system_args(self.args.system):
                lines.append(cogdb.query.fort_find_system(self.session, name).display())
            response = '\n'.join(lines)

        elif self.args.next:
            lines = ['__Next Targets{}__'.format(manual)]
            lines += [system.display() for system in
                      cogdb.query.fort_get_next_targets(self.session, count=self.args.next)]
            response = '\n'.join(lines)

        else:
            lines = ['__Active Targets{}__'.format(manual)]
            lines += [system.display() for system in cogdb.query.fort_get_targets(self.session)]

            lines += ['\n__Next Targets__']
            next_count = self.args.next if self.args.next else 3
            lines += [system.display() for system in
                      cogdb.query.fort_get_next_targets(self.session, count=next_count)]

            defers = cogdb.query.fort_get_deferred_targets(self.session)
            if defers:
                lines += ['\n__Almost Done__'] + [system.display() for system in defers]
            response = '\n'.join(lines)

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
            ['Date (UTC)', datetime.datetime.now(datetime.timezone.utc)],
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
            'For more information do: `{}Command -h`'.format(prefix),
            '       Example: `{}drop -h`'.format(prefix),
            '',
        ])
        lines = [
            ['Command', 'Effect'],
            ['{prefix}admin', 'Admin commands'],
            ['{prefix}bgs', 'Display information related to BGS work'],
            ['{prefix}dist', 'Determine the distance from the first system to all others'],
            ['{prefix}drop', 'Drop forts into the fort sheet'],
            ['{prefix}feedback', 'Give feedback or report a bug'],
            ['{prefix}fort', 'Get information about our fort systems'],
            ['{prefix}hold', 'Declare held merits or redeem them'],
            ['{prefix}kos', 'Manage or search kos list'],
            ['{prefix}near', 'Find things near you.'],
            ['{prefix}recruits', 'Manage recruits on the recruit sheet.'],
            ['{prefix}repair', 'Show the nearest orbitals with shipyards'],
            ['{prefix}route', 'Plot the shortest route between these systems'],
            ['{prefix}scout', 'Generate a list of systems to scout'],
            ['{prefix}status', 'Info about this bot'],
            ['{prefix}time', 'Show game time and time to ticks'],
            ['{prefix}track', 'Track carrier movement by system or id.'],
            ['{prefix}trigger', 'Calculate fort and um triggers for systems'],
            ['{prefix}um', 'Get information about undermining targets'],
            ['{prefix}user', 'Manage your user, set sheet name and tag'],
            ['{prefix}whois', 'Search for commander on inara.cz'],
            ['{prefix}help', 'This help message'],
        ]
        lines = [[line[0].format(prefix=prefix), line[1]] for line in lines]

        response = overview + cog.tbl.format_table(lines, header=True)[0]
        await self.bot.send_ttl_message(self.msg.channel, response)
        await self.msg.delete()


class Hold(Action):
    """
    Update a user's held merits.
    """
    async def set_hold(self):
        """ Set the hold on a system. """
        if not self.args.system:
            raise cog.exc.InvalidCommandArgs("You forgot to specify a system to update.")

        system = cogdb.query.um_find_system(self.session, ' '.join(self.args.system))
        self.log.info('HOLD %s - Matched system name %s: \n%s.',
                      self.duser.display_name, self.args.system, system)
        hold = cogdb.query.um_add_hold(self.session, system=system,
                                       user=self.undermine, held=self.args.amount)

        if self.args.set:
            system.set_status(self.args.set)
            self.payloads += cogdb.scanners.UMScanner.update_systemum_dict(
                system.sheet_col, system.progress_us,
                system.progress_them, system.map_offset
            )

        self.log.info('Hold %s - After update, hold: %s\nSystem: %s.',
                      self.duser.display_name, hold, system)

        response = hold.system.display()
        if hold.system.is_undermined:
            response += '\n\nSystem is finished with held merits. Type `!um` for more targets.'
            response += '\n\n**{}** Have a :skull: for completing {}. Don\'t forget to redeem.'.format(
                self.duser.display_name, system.name)

        return ([hold], response)

    @check_mentions
    @check_sheet('hudson_undermine', 'um_user', UMUser)
    async def execute(self):
        self.log.info('HOLD %s - Matched self.duser with id %s and sheet name %s.',
                      self.duser.display_name, self.duser.id, self.undermine)

        if self.args.died:
            holds = cogdb.query.um_reset_held(self.session, self.undermine)
            self.log.info('HOLD %s - User reset merits.', self.duser.display_name)
            response = 'Sorry you died :(. Held merits reset.'

        elif self.args.redeem:
            holds, redeemed = cogdb.query.um_redeem_merits(self.session, self.undermine)
            self.log.info('HOLD %s - Redeemed %d merits.', self.duser.display_name, redeemed)

            response = '**Redeemed Now** {}\n\n__Cycle Summary__\n'.format(redeemed)
            lines = [['System', 'Hold', 'Redeemed']]
            lines += [[merit.system.name, merit.held, merit.redeemed] for merit
                      in self.undermine.merits if merit.held + merit.redeemed > 0]
            response += cog.tbl.format_table(lines, header=True)[0]

        elif self.args.redeem_systems:
            system_strs = " ".join(self.args.redeem_systems).split(",")
            holds, redeemed = cogdb.query.um_redeem_systems(self.session, self.undermine, system_strs)

            response = '**Redeemed Now** {}\n\n__Cycle Summary__\n'.format(redeemed)
            lines = [['System', 'Hold', 'Redeemed']]
            lines += [[merit.system.name, merit.held, merit.redeemed] for merit
                      in self.undermine.merits if merit.held + merit.redeemed > 0]
            response += cog.tbl.format_table(lines, header=True)[0]

        else:  # Default case, update the hold for a system
            holds, response = await self.set_hold()

        self.session.commit()

        for hold in holds:
            self.payloads += cogdb.scanners.UMScanner.update_hold_dict(
                hold.system.sheet_col, hold.user.row, hold.held, hold.redeemed)

        scanner = get_scanner("hudson_undermine")
        await scanner.send_batch(self.payloads)

        await self.bot.send_message(self.msg.channel, response)


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
        faction = ' '.join(self.args.faction)
        reason = ' '.join(self.args.reason) + " -{}".format(self.msg.author.name)
        is_friendly = self.args.is_friendly
        await self.msg.channel.send('CMDR {} has been reported for moderation.'.format(cmdr))

        # Request approval
        chan = self.msg.guild.get_channel(cog.util.get_config("carrier_channel"))
        sent = await chan.send(
            embed=cog.inara.kos_report_cmdr_embed(
                self.msg.author.name, cmdr, faction, reason, is_friendly,
            )
        )
        yes_emoji = cog.util.get_config('emojis', '_yes')
        await sent.add_reaction(yes_emoji)
        await sent.add_reaction(cog.util.get_config('emojis', '_no'))

        def check(_, user):
            try:
                return cogdb.query.get_admin(self.session, user)
            except cog.exc.NoMatch:
                return False
        react, _ = await self.bot.wait_for('reaction_add', check=check)

        # Approved update
        response = "No change to KOS made."
        if str(react) == yes_emoji:
            scanner = get_scanner('hudson_kos')
            await scanner.update_cells()
            payload = scanner.add_report_dict(
                cmdr, faction, reason, is_friendly
            )
            await scanner.send_batch(payload)
            cogdb.query.kos_add_cmdr(self.session, cmdr, faction, reason, is_friendly)
            self.session.commit()
            response = "CMDR has been added to KOS."

        await chan.send(response)

    async def execute(self):
        msg = 'KOS: Invalid subcommand'

        if self.args.subcmd == 'report':
            await self.report()
            msg = None

        elif self.args.subcmd == 'pull':
            await self.bot.loop.run_in_executor(
                None, get_scanner('hudson_kos').parse_sheet
            )
            msg = 'KOS list refreshed from sheet.'

        elif self.args.subcmd == 'search':
            msg = 'Searching for "{}" against known CMDRs\n\n'.format(self.args.term)
            cmdrs = cogdb.query.kos_search_cmdr(self.session, self.args.term)
            if cmdrs:
                lines = [['CMDR Name', 'Faction', 'Is Friendly?', 'Reason']]
                lines += [[x.cmdr, x.faction, x.friendly, x.reason] for x in cmdrs]
                msg += cog.tbl.format_table(lines, header=True)[0]
            else:
                msg += "No matches!"

        if msg:
            await self.bot.send_message(self.msg.channel, msg)


class Near(Action):
    """
    Handle the KOS command.
    """
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
                centre_name=centre.name, power=self.args.power
            )
        )

        lines = [['System', 'Distance']] + [[x.name, "{:.2f}".format(x.dist_to(centre))] for x in systems[:10]]
        return "__Closest 10 Controls__\n\n" + \
            cog.tbl.format_table(lines, header=True)[0]

    async def ifactors(self, eddb_session):
        """
        Find nearest suitable interstellar factors.
        """
        sys_name = ' '.join(self.args.system)
        centre = cogdb.eddb.get_systems(eddb_session, [sys_name])[0]
        stations = await self.bot.loop.run_in_executor(
            None,
            functools.partial(
                cogdb.eddb.get_nearest_ifactors, eddb_session,
                centre_name=centre.name, include_medium=self.args.medium
            )
        )

        stations = [["System", "Distance", "Station", "Arrival"]] + stations
        return cog.tbl.format_table(
            stations, header=True, prefix="__Nearby Interstellar Factors__\n",
            suffix="[L] Large pads.\n[M] M pads only."
        )[0]

    async def execute(self):
        msg = 'Invalid near sub command.'
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            if self.args.subcmd == 'control':
                msg = await self.control(eddb_session)
            elif self.args.subcmd == 'if':
                msg = await self.ifactors(eddb_session)

        await self.bot.send_message(self.msg.channel, msg)


class Pin(Action):
    """
    Create an objetives pin.
    """
    # TODO: Incomplete, expect bot to manage pin entirely. Left undocumented.
    async def execute(self):
        systems = cogdb.query.fort_get_targets(self.session)
        systems.reverse()
        systems += cogdb.query.fort_get_next_targets(self.session, count=5)
        for defer in cogdb.query.fort_get_deferred_targets(self.session):
            if defer.name != "Othime":
                systems += [defer]

        lines = [":Fortifying: {} {}".format(
            sys.name, "**{}**".format(sys.notes) if sys.notes else "") for sys in systems]
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
    async def execute(self):
        try:
            cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms("{} You are not an admin!".format(self.msg.author.mention)) from exc

        r_scanner = get_scanner('hudson_recruits')
        await r_scanner.update_cells()
        r_scanner.update_first_free()

        cmdr = " ".join(self.args.cmdr)
        discord_name = " ".join(self.args.discord_name) if self.args.discord_name else cmdr
        notes = " ".join(self.args.notes)
        if not re.match(r'.*-\s*\S+$', notes):
            notes += " -{}".format(self.msg.author.name)

        await r_scanner.send_batch(r_scanner.add_recruit_dict(
            cmdr=cmdr,
            discord_name=discord_name,
            rank=self.args.rank,
            platform=self.args.platform,
            pmf=" ".join(self.args.pmf),
            notes=notes,
        ))

        response = "CMDR {} has been added to row: {}".format(cmdr, r_scanner.first_free - 1)
        await self.bot.send_message(self.msg.channel, response)


class Repair(Action):
    """
    Find a nearby station with a shipyard.
    """
    async def execute(self):
        if self.args.distance > 30:
            raise cog.exc.InvalidCommandArgs("Searching beyond **30**ly would produce too long a list.")

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

            lines = ["__Route Plotted__", "Total Distance: **{}**ly".format(round(result[0])), ""]
            lines += [sys.name for sys in result[1]]

        await self.bot.send_message(self.msg.channel, "\n".join(lines))


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

            now = datetime.datetime.now(datetime.timezone.utc)
            lines = SCOUT_TEMPLATE.format(
                round(result[0], 2), now.strftime("%B"),
                now.day, now.year + 1286, system_list)

        await self.bot.send_message(self.msg.channel, lines)


class Status(Action):
    """
    Display the status of this bot.
    """
    async def execute(self):
        lines = [
            ['Created By', 'GearsandCogs'],
            ['Uptime', self.bot.uptime],
            ['Version', '{}'.format(cog.__version__)],
            ['Contributors:', ''],
            ['    Shotwn', 'Inara search'],
            ['    Prozer', 'Various Contributions'],
        ]

        await self.bot.send_message(self.msg.channel, cog.tbl.format_table(lines)[0])


class Time(Action):
    """
    Provide in game time and time to import in game ticks.

    Shows the time ...
    - In game
    - To daily BGS tick
    - To weekly tick
    """
    async def execute(self):
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        today = now.replace(hour=0, minute=0, second=0)  # pylint: disable=unexpected-keyword-arg

        weekly_tick = today + datetime.timedelta(hours=7)
        while weekly_tick < now or weekly_tick.strftime('%A') != 'Thursday':
            weekly_tick += datetime.timedelta(days=1)

        try:
            with cogdb.session_scope(cogdb.SideSession) as side_session:
                tick = await self.bot.loop.run_in_executor(
                    None, cogdb.side.next_bgs_tick, side_session, now)
        except (cog.exc.NoMoreTargets, cog.exc.RemoteError) as exc:
            tick = exc.reply()
        lines = [
            'Game Time: **{}**'.format(now.strftime('%H:%M:%S')),
            tick,
            'Cycle Ends in **{}**'.format(weekly_tick - now),
            'All Times UTC',
        ]

        await self.bot.send_message(self.msg.channel, '\n'.join(lines))


class Track(Action):
    """
    Manage the ability to track what carriers are doing.
    """
    async def add(self):
        """ Subcmd add for track command. """
        system_names = process_system_args(self.args.systems)
        cogdb.query.track_add_systems(self.session, system_names, self.args.distance)

        to_add = []
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            for sys_name in system_names:
                to_add += cogdb.eddb.get_systems_around(eddb_session, sys_name, self.args.distance)
            to_add = {x.name for x in to_add}

        new_systems = sorted([x.system for x in cogdb.query.track_systems_computed_update(self.session, to_add)])
        response = "__Systems Added To Tracking__\n\nSystems added: {} First few follow ...\n\n".format(len(new_systems))
        return response + ", ".join(new_systems[:TRACK_LIMIT])

    async def remove(self):
        """ Subcmd remove for track command. """
        system_names = process_system_args(self.args.systems)
        cogdb.query.track_remove_systems(self.session, system_names)
        remaining = cogdb.query.track_get_all_systems(self.session)

        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            computed = []
            for remain in remaining:
                computed += cogdb.eddb.get_systems_around(eddb_session, remain.system, remain.distance)
            to_keep = {x.name for x in computed}

        removed = sorted(cogdb.query.track_systems_computed_remove(self.session, to_keep))
        response = "__Systems Removed From Tracking__\n\nSystems added: {} First few follow ...\n\n".format(len(removed))
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
            for msg in cogdb.query.track_ids_show(self.session):
                await self.bot.send_message(self.msg.channel, msg)

        return response

    async def show(self):
        """ Subcmd show for track command. """
        for msg in cogdb.query.track_show_systems(self.session):
            await self.bot.send_message(self.msg.channel, msg)

    async def channel(self):
        """ Subcmd channel for track command. """
        cog.util.update_config(self.msg.channel.id, "carrier_channel")
        return "Channel set to: {}".format(self.msg.channel.name)

    async def scan(self):
        """ Subcmd scan for track command. """
        scanner = get_scanner("hudson_carriers")
        await scanner.update_cells()
        await self.bot.loop.run_in_executor(None, scanner.parse_sheet)

        return "Scan finished."

    async def execute(self):
        try:
            cogdb.query.get_admin(self.session, self.duser)
        except cog.exc.NoMatch as exc:
            raise cog.exc.InvalidPerms("{} You are not an admin!".format(self.msg.author.mention)) from exc

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
                "Power: {}".format(power[0]),
                "Power HQ: {}\n".format(power[1])
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
            now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
            today = now.replace(hour=0, minute=0, second=0)  # pylint: disable=unexpected-keyword-arg
            weekly_tick = today + datetime.timedelta(hours=7)
            while weekly_tick < now or weekly_tick.strftime('%A') != 'Thursday':
                weekly_tick += datetime.timedelta(days=1)

            prefix = "**Held Merits**\n\n{}\n".format('DEADLINE **{}**'.format(weekly_tick - now))
            response = cog.tbl.format_table(cogdb.query.um_all_held_merits(self.session), header=True, prefix=prefix)[0]

        elif self.args.system:
            system = cogdb.query.um_find_system(self.session, ' '.join(self.args.system))

            if self.args.offset:
                system.map_offset = self.args.offset
            if self.args.set:
                system.set_status(self.args.set)
            if self.args.set or self.args.offset:
                self.session.commit()

                self.payloads += cogdb.scanners.UMScanner.update_systemum_dict(
                    system.sheet_col, system.progress_us,
                    system.progress_them, system.map_offset
                )
                scanner = get_scanner("hudson_undermine")
                await scanner.send_batch(self.payloads)

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
            SPLIT_POS = 5

            embed1 = discord.Embed(title="Undermining Ships")
            for power in UM_NPC_TABLE[1:SPLIT_POS]:
                embed1.add_field(name=power[0], value="Power", inline=False)
                embed1.add_field(name=power[1], value="Fighter", inline=True)
                embed1.add_field(name=power[2], value="Transport", inline=True)
                embed1.add_field(name=power[3], value="Expansion", inline=True)
            await self.bot.send_message(self.msg.channel, embed=embed1)

            embed2 = discord.Embed(title="Undermining Ships (cont.)")
            for power in UM_NPC_TABLE[SPLIT_POS:]:
                embed2.add_field(name=power[0], value="Power", inline=False)
                embed2.add_field(name=power[1], value="Fighter", inline=True)
                embed2.add_field(name=power[2], value="Transport", inline=True)
                embed2.add_field(name=power[3], value="Expansion", inline=True)
            await self.bot.send_message(self.msg.channel, embed=embed2)

            return

        else:
            systems = cogdb.query.um_get_systems(self.session)
            response = '__Current UM Targets__\n\n' + '\n'.join(
                [system.display() for system in systems])

        await self.bot.send_message(self.msg.channel, response)


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
            if self.cattle:
                sheet = self.cattle
                self.payloads += cogdb.scanners.FortScanner.update_sheet_user_dict(
                    sheet.row, sheet.cry, sheet.name)
                scanner = get_scanner("hudson_cattle")
                coros += [scanner.send_batch(self.payloads)]

            if self.undermine:
                sheet = self.undermine
                self.payloads += cogdb.scanners.UMScanner.update_sheet_user_dict(
                    sheet.row, sheet.cry, sheet.name)
                scanner = get_scanner("hudson_undermine")
                coros += [scanner.send_batch(self.payloads)]

            await asyncio.gather(*coros)

        msgs = ['\n'.join([
            '__{}__'.format(self.msg.author.display_name),
            'Sheet Name: ' + self.duser.pref_name,
            'Default Cry:{}\n'.format(' ' + self.duser.pref_cry if self.duser.pref_cry else ''),
            '',
        ])]
        if self.cattle:
            prefix = "\n".join([
                '__Fortification__',
                '    Cry: {}'.format(self.cattle.cry),
                '    Total: {}\n'.format(self.cattle.merit_summary()),
            ])
            lines = [['System', 'Amount']]
            lines += [[merit.system.name, merit.amount] for merit in self.cattle.merits
                      if merit.amount > 0]
            msgs += cog.tbl.format_table(lines, header=True, prefix=prefix)
        if self.undermine:
            prefix = "\n".join([
                '\n__Undermining__',
                '    Cry: {}'.format(self.undermine.cry),
                '    Total: {}\n'.format(self.undermine.merit_summary()),
            ])
            lines = [['System', 'Hold', 'Redeemed']]
            lines += [[merit.system.name, merit.held, merit.redeemed] for merit
                      in self.undermine.merits if merit.held + merit.redeemed > 0]
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
            if self.cattle:
                self.cattle.name = new_name
            if self.undermine:
                self.undermine.name = new_name
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

        if self.cattle:
            self.cattle.cry = new_cry
        if self.undermine:
            self.undermine.cry = new_cry
        self.session.commit()

        nduser = cogdb.query.get_duser(self.session, self.duser.id)
        nduser.pref_cry = new_cry
        self.session.commit()


class WhoIs(Action):
    """
    Who is request to Inara for CMDR info.
    """
    async def execute(self):
        cmdr = await cog.inara.api.search_with_api(' '.join(self.args.cmdr), self.msg)
        if cmdr:
            await cog.inara.api.reply_with_api_result(cmdr["req_id"], cmdr["event_data"], self.msg)


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


def init_scanner(name):
    """
    Initialize a scanner based on configuration.
    """
    print("Intializing scanner -> ", name)
    logging.getLogger(__name__).info("Initializing the %s scanner.", name)
    sheet = cog.util.get_config("scanners", name)
    cls = getattr(cogdb.scanners, sheet["cls"])
    scanner = cls(sheet)
    SCANNERS[name] = scanner


def get_scanner(name):
    """
    Store scanners in this module for shared use.
    """
    try:
        return SCANNERS[name]
    except KeyError as exc:
        raise cog.exc.InvalidCommandArgs("The scanners are not ready. Please try again in 15 seconds.") from exc


async def monitor_carrier_events(client, *, next_summary, last_timestamp=None, delay=60):
    """
    Simple async task that just checks for new events every delay.

    Args:
        client: The bot.
        next_summary: Datetime object representing next time to do daily summary.
        last_seen_time: Last known timestamp for a TrackByID.
        delay: The short delay between normal summaries, in seconds.
    """
    start = datetime.datetime.now(datetime.timezone.utc)
    if not last_timestamp:
        last_timestamp = start

    await asyncio.sleep(delay)

    with cogdb.session_scope(cogdb.Session) as session:
        if datetime.datetime.now(datetime.timezone.utc) < next_summary:
            header = "__Fleet Carriers Detected Last {} Seconds__\n".format(delay)
            tracks = await client.loop.run_in_executor(
                None, cogdb.query.track_ids_newer_than, session, last_timestamp
            )
        else:
            header = "__Daily Fleet Carrier Summary For {}__\n".format(next_summary)
            yesterday = next_summary - datetime.timedelta(days=1)
            next_summary = next_summary + datetime.timedelta(days=1)
            tracks = await client.loop.run_in_executor(
                None, cogdb.query.track_ids_newer_than, session, yesterday
            )
        if tracks:
            last_timestamp = tracks[-1].updated_at

        msgs = cog.util.generative_split(tracks, str, header=header)

    # Only send messages if generated and channel set
    chan_id = cog.util.get_config("carrier_channel", default=None)
    if msgs and chan_id and msgs != [header]:
        chan = client.get_channel(chan_id)
        for msg in msgs:
            await client.send_message(chan, msg)

    asyncio.ensure_future(
        monitor_carrier_events(
            client, next_summary=next_summary,
            last_timestamp=last_timestamp, delay=delay
        )
    )


SCANNERS = {}
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
