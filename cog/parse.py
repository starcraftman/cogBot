"""
Everything related to parsing arguements from the received text.

By setting defaults passed on the parser (cmd, subcmd) can differeciate
what action to be invoked.
"""
import argparse
from argparse import RawDescriptionHelpFormatter as RawHelp
import logging

import cog.exc
import cog.util
from cogdb.schema import EUMSheet, EVoteType

PARSERS = []
CMD_MAP = {}  # Maps the command names from Class onto command.
VALID_VOTE_TYPES = ['cons', 'prep']


class ThrowArggumentParser(argparse.ArgumentParser, Exception):
    """
    ArgumentParser subclass that does NOT terminate the program.
    """
    def print_help(self, file=None):  # pylint: disable=redefined-builtin
        formatter = self._get_formatter()
        formatter.add_text(self.description)
        raise cog.exc.ArgumentHelpError(formatter.format_help())

    def error(self, message):
        raise cog.exc.ArgumentParseError(message)

    def exit(self, status=0, message=None):
        """
        Suppress default exit behaviour.
        """
        raise cog.exc.ArgumentParseError(message)


def make_parser(prefix):
    """
    Returns the bot parser.
    """
    parser = ThrowArggumentParser(prog='', description='simple discord bot')

    subs = parser.add_subparsers(title='subcommands',
                                 description='The subcommands of cog')

    for func in PARSERS:
        func(subs, prefix)

    return parser


def register_parser(func):
    """ Simple registration function, use as decorator. """
    PARSERS.append(func)
    return func


@register_parser
def subs_admin(subs, prefix):
    """ Subcommand parsing for admin """
    desc = f"""Admin only commands. Examples:

**{prefix}admin add @GearsandCogs  **
        Add GearsandCogs to the admin group.
**{prefix}admin remove @GearsandCogs**
        Remove GearsandCogs from the admin group.
**{prefix}admin add BGS #hudson_bgs**
        Whitelist bgs command for hudson_bgs channel.
**{prefix}admin remove BGS #hudson_bgs**
        Remove whitelist for bgs command in hudson_bgs.
**{prefix}admin add Drop @FRC Member**
        Whitelist bgs command for members with role mentioned FRC Member.
**{prefix}admin remove Drop FRC Member**
        Remove whitelist for bgs command of users with role "FRC Member".
**{prefix}admin show_rules**
        Show all active rules limiting commands.
**{prefix}admin active -d 90 #hudson_operations #hudson_bgs**
        Generate an activity report on cmdrs in listed channels,
        look back over last 90 days of messages.
**{prefix}admin cast A message here**
        Broadcast a message to all channels.
**{prefix}admin cycle**
        Point bot to new cycle's sheets.
**{prefix}admin deny**
        Toggle command processing.
**{prefix}admin dump**
        Dump the database to console to inspect.
**{prefix}admin halt**
        Shutdown this bot after short delay.
**{prefix}admin scan**
        Pull and parse the latest sheet information.
**{prefix}admin top n**
        Generate a top n summary for the current cycle. Default n = 5.
**{prefix}admin top n --leaders**
        Generate a top n summary for the current cycle. Default n = 5. Include leadership.
**{prefix}admin info @User**
        Information about the mentioned User, DMed to admin.
**{prefix}admin addum Cubeo, Nauo -p Normal -r 50**
        Will add Cubeo and Nauo with 50% reinforced trigger and Normal priority in UM sheet.
**{prefix}admin removeum Nauo**
        Will remove Nauo from the UM sheet. 1 system at a time.
    """
    sub = subs.add_parser(prefix + 'admin', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Admin')
    CMD_MAP['Admin'] = 'admin'
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Admin subcommands', dest='subcmd')
    subcmd = subcmds.add_parser('add', help='Add an admin or permission.')
    subcmd.add_argument('rule_cmds', nargs='*', help='The the command to restrict.')
    subcmd = subcmds.add_parser('remove', help='Remove an admin or permission.')
    subcmd.add_argument('rule_cmds', nargs='*', help='The the command to restrict.')
    subcmds.add_parser('show_rules', help='Show existing command rules.')
    subcmd = subcmds.add_parser('cast', help='Broadcast a message to all channels.')
    subcmd.add_argument('content', nargs='+', help='The message to send, no hyphens.')
    subcmds.add_parser('cycle', help='Roll sheets to new cycle.')
    subcmds.add_parser('deny', help='Toggle command processing.')
    subcmds.add_parser('dump', help='Dump the db to console.')
    subcmds.add_parser('halt', help='Stop accepting commands and halt bot.')
    subcmds.add_parser('scan', help='Scan the sheets for updates.')
    subcmd = subcmds.add_parser('top', help='Generate the top cmdrs of cycle.')
    subcmd.add_argument('limit', nargs='?', type=int, default=5, help='The limit of users per top list.')
    subcmd.add_argument('-l', '--leaders', action='store_true', help='Include leaders in the list.')
    subcmd = subcmds.add_parser('info', help='Get info about discord users.')
    subcmd.add_argument('user', nargs='?', help='The user to get info on.')
    subcmd = subcmds.add_parser('active', help='Get a report on user activity.')
    subcmd.add_argument('-d', '--days', type=int, default=90, nargs='?',
                        help='The number of days to look back.')
    subcmd = subcmds.add_parser('addum', help='Add systems to Undermining sheet.')
    subcmd.add_argument('system', nargs='+', help='System(s) to add to sheet.')
    subcmd.add_argument('-p', '--priority', nargs='?', default='Normal', help='Priority of the new systems.',
                        choices=["Normal", "High", "Low", "Leave for now"])
    subcmd.add_argument('-r', '--reinforced', type=int, default=50, nargs='?',
                        help='Reinforcement %age of the new systems.')
    subcmd = subcmds.add_parser('removeum', help='Remove systems from the Undermining sheet.')
    subcmd.add_argument('system', nargs='+', help='Systems(s) to remove from sheet.')


@register_parser
def subs_bgs(subs, prefix):
    """ Subcommand parsing for bgs """
    desc = f"""BGS related commands. Examples:

**{prefix}bgs age 16 cygni**
        Show exploiteds in 16 Cygni bubble by age.
**{prefix}bgs dash othime**
        Show the bgs state of a bubble's exploited systems.
**{prefix}bgs edmc**
        Present the optimized route to jump and update EDMC for default systems.
**{prefix}bgs edmc Rana, Frey**
        Present the optimized route to jump and update EDMC for Rana, Frey.
**{prefix}bgs edmc --age 2 Rana, Frey**
        Present the optimized route to jump and update EDMC for Rana, Frey.
**{prefix}bgs exp Rana**
        Show all factions in Rana, select one, then show all expansion candidates.
**{prefix}bgs expto Rana**
        Show all factions that could possibly expand to Rana.
**{prefix}bgs faction**
        Show a report for all systems where default factions present.
**{prefix}bgs faction Democrats of Muncheim**
        Show a report for all systems where Democrats of Muncheim present.
**{prefix}bgs find Frey**
        Show all Feudal/Patronage factions near Frey, expanding in 15ly increments.
**{prefix}bgs find Frey --max 50**
        Show all Feudal/Patronage factions near Frey within 50ly.
**{prefix}bgs inf Sol**
        Show the factions and their influence in Sol.
**{prefix}bgs report Sol**
        Show an overall report for one or more bubbles.
**{prefix}bgs sys Frey**
        Show a system overview and all factions.
    """
    sub = subs.add_parser(prefix + 'bgs', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='BGS', system=[])
    CMD_MAP['BGS'] = 'bgs'
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='BGS subcommands', dest='subcmd')
    subcmd = subcmds.add_parser('age', help='Get the age of exploiteds around a control.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')
    subcmd = subcmds.add_parser('dash', help='Dashboard overview of exploiteds around a control.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')
    subcmd = subcmds.add_parser('edmc', help='Create the EDMC update route.')
    subcmd.add_argument('--age', type=int, default=1, help='The results will be this old or more.')
    subcmd.add_argument('system', nargs='*', help='The control systems to update.')
    subcmd = subcmds.add_parser('exp', help='Find expansion candidates from system.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')
    subcmd = subcmds.add_parser('expto', help='Find all possible expansion candidates to system.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')
    subcmd = subcmds.add_parser('faction', help='Show faction report by system.')
    subcmd.add_argument('faction', nargs='*', help='The faction(s) to lookup.')
    subcmd = subcmds.add_parser('find', help='Show favorable factions around a system.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')
    subcmd.add_argument('-m', '--max', type=int, help='The radius to look within.')
    subcmd = subcmds.add_parser('inf', help='Get the influence of factions inside a system.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')
    subcmd = subcmds.add_parser('report', help='Get an overall report of the bubble.')
    subcmd.add_argument('system', nargs='*', default=[], help='The system to lookup.')
    subcmd = subcmds.add_parser('sys', help='Get a complete system overview.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')


@register_parser
def subs_dash(subs, prefix):
    """ Subcommand parsing for dash """
    desc = f"""Show a dashboard of connection to bots external services.
This command is restricted to admin users only.
    Examples:

**{prefix}dash**
        Display the dashboard status of the bot.
**{prefix}dash restart PowerplayMonitor**
        Restart the powerplay monitor, will write information on failure to log.
    """
    sub = subs.add_parser(prefix + 'dash', description=desc, formatter_class=RawHelp)
    CMD_MAP['Dashboard'] = 'dash'
    sub.set_defaults(cmd='Dashboard', subcmd='display')
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Dash subcommands', dest='subcmd')
    subcmd = subcmds.add_parser('restart', help='Restart tasks')
    subcmd.add_argument('task', nargs='+', help='The task to restart.')


@register_parser
def subs_dist(subs, prefix):
    """ Subcommand parsing for dist """
    desc = f"""Determine the distance from the first system to all others.
    The system names must match __exactly__. Match is not case sensitive.
    Examples:

**{prefix}dist sol, frey, Rana**
        Display the distance from Sol to Frey and Rana.
    """
    sub = subs.add_parser(prefix + 'dist', description=desc, formatter_class=RawHelp)
    CMD_MAP['Dist'] = 'dist'
    sub.set_defaults(cmd='Dist')
    sub.add_argument('system', nargs='+', help='The systems in question.')


@register_parser
def subs_donate(subs, prefix):
    """ Subcommand parsing for drop """
    desc = f"""Information on how to donate to the project.

**{prefix}donate**
        Information on how to donate funds to support development and server costs.
    """
    sub = subs.add_parser(prefix + 'donate', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Donate')
    CMD_MAP['Donate'] = 'donate'


@register_parser
def subs_drop(subs, prefix):
    """ Subcommand parsing for drop """
    bound = cog.util.CONF.constants.max_drop
    desc = f"""Update the cattle sheet when you drop at a system.
    Amount dropped must be in range [-{bound}, {bound}]
    Examples:

**{prefix}drop 600 Rana**
        Drop 600 supplies for yourself at Rana.
**{prefix}drop -50 Rana**
        Made a mistake? Subract 50 forts from your drops at Rana.
**{prefix}drop 600 Rana @rjwhite**
        Drop 600 supplies for rjwhite Rana.
**{prefix}drop 600 lala**
        Drop 600 supplies for yourself at Lalande 39866, search used when name is not exact.
**{prefix}drop 600 Rana --set 4560:2000**
**{prefix}drop 600 Rana -s 4560:2000**
        Drop 600 supplies at Rana for yourself, set fort status to 4500 and UM status to 2000.
    """
    sub = subs.add_parser(prefix + 'drop', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Drop')
    CMD_MAP['Drop'] = 'drop'
    sub.add_argument('amount', type=int, help='The amount to drop.')
    sub.add_argument('system', nargs='+', help='The system to drop at.')
    sub.add_argument('-s', '--set',
                     help='Set the fort:um status of the system. Example-> --set 3400:200')


@register_parser
def subs_feedback(subs, prefix):
    """ Subcommand parsing for feedback """
    desc = f"""Give feedback or report a bug. Example:

**{prefix}bug Explain what went wrong ...**
        File a bug report or give feedback.
    """
    sub = subs.add_parser(prefix + 'feedback', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Feedback')
    CMD_MAP['Feedback'] = 'feedback'
    sub.add_argument('content', nargs='+', help='The bug description or feedback.')


@register_parser
def subs_fort(subs, prefix):
    """ Subcommand parsing for fort """
    desc = f"""Show fortification status and targets. Examples:

**{prefix}fort**
        Show current fort objectives.
**{prefix}fort --details Sol**
**{prefix}fort -d Sol**
        Show a detailed view of Sol, including all CMDR merits.
**{prefix}fort --miss 1000**
        Show all systems missing <= 1000 supplies.
**{prefix}fort --next 5**
**{prefix}fort -n 5**
        Show the next 5 fortification targets (excluding Othime and skipped).
**{prefix}fort --order Sol, Adeo, Frey**
        Set the fort order to: Sol -> Adeo -> Frey, then fallback to default.
**{prefix}fort --order**
        Return the fort order to default sheet order.
**{prefix}fort alpha**
        Show the fortification status of Alpha Fornacis.
**{prefix}fort alpha, sol, ran**
        Show the fortification status of Alpha Fornacis, Sol and Rana.
**{prefix}fort Othime --set 7500:2000**
**{prefix}fort Othime -s 7500:2000**
        Set othime to 7500 fort status and 2000 um status.
    """
    sub = subs.add_parser(prefix + 'fort', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Fort')
    CMD_MAP['Fort'] = 'fort'
    sub.add_argument('system', nargs='*', help='Select this system.')
    sub.add_argument('-s', '--set',
                     help='Set the fort:um status of system. Example-> --set 3400:200')
    sub.add_argument('--order', action='store_true',
                     help='Set the fort order. Comma separate list of systems.')
    sub.add_argument('--miss', type=int,
                     help='Show systems missing <= MISS merits.')
    sub.add_argument('-n', '--next', type=int,
                     help='Show the next NUM fort targets after current')
    sub.add_argument('-d', '--details', action='store_true',
                     help='Show details on selected systems (will truncate to 4).')
    sub.add_argument('-p', '--priority', action='store_true',
                     help='Show high priority systems in the fort results.')


@register_parser
def subs_help(subs, prefix):
    """ Subcommand parsing for help """
    sub = subs.add_parser(prefix + 'help', description='Show overall help message.')
    sub.set_defaults(cmd='Help')
    CMD_MAP['Help'] = 'help'


@register_parser
def subs_hold(subs, prefix):
    """ Subcommand parsing for hold """
    desc = f"""Update a user's held or redeemed merits. Examples:

**{prefix}hold 1200 burr**
        Set your held merits at Burr to 1200.
**{prefix}hold 900 af leopris @Memau**
        Set held merits at System AF Leopris to 900 held for Memau.
**{prefix}hold --died**
**{prefix}hold -d**
        Reset your held merits to 0 due to dying.
**{prefix}hold --redeem**
**{prefix}hold -r**
        Move all held merits to redeemed column.
**{prefix}hold --redeem-systems Burr, Rana**
        Redeem only merits for systems listed.
**{prefix}hold 720 burr --set 60000:130**
**{prefix}hold 720 burr -s 60000:130**
        Update held merits to 720 at Burr expansion and set progress to 60000 merits for us
and 130% opposition.
    """
    sub = subs.add_parser(prefix + 'hold', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Hold', sheet_src=EUMSheet.main)
    CMD_MAP['Hold'] = 'hold'
    sub.add_argument('amount', nargs='?', type=int, help='The amount of merits held.')
    sub.add_argument('system', nargs='*', help='The system merits are held in.')
    sub.add_argument('-r', '--redeem', action='store_true', help='Redeem all held merits.')
    sub.add_argument('--redeem-systems', nargs='+', help='Redeem all held merits.')
    sub.add_argument('-d', '--died', action='store_true', help='Zero out held merits.')
    sub.add_argument('-s', '--set',
                     help='Update the galmap progress us:them. Example: --set 3500:200')


@register_parser
def subs_shold(subs, prefix):
    """ Subcommand parsing for shold, snipe variant of hold """
    desc = f"""Update a user's held or redeemed merits.
IMPORTANT: This is the __SNIPE__ sheet.

Examples:

**{prefix}shold 1200 burr**
        Set your held merits at Burr to 1200.
**{prefix}shold 900 af leopris @Memau**
        Set held merits at System AF Leopris to 900 held for Memau.
**{prefix}shold --died**
**{prefix}shold -d**
        Reset your held merits to 0 due to dying.
**{prefix}shold --redeem**
**{prefix}shold -r**
        Move all held merits to redeemed column.
**{prefix}shold --redeem-systems Burr, Rana**
        Redeem only merits for systems listed.
**{prefix}shold 720 burr --set 60000:130**
**{prefix}shold 720 burr -s 60000:130**
        Update held merits to 720 at Burr expansion and set progress to 60000 merits for us
and 130% opposition.
    """
    sub = subs.add_parser(prefix + 'shold', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='SnipeHold', sheet_src=EUMSheet.snipe)
    CMD_MAP['SnipeHold'] = 'shold'
    sub.add_argument('amount', nargs='?', type=int, help='The amount of merits held.')
    sub.add_argument('system', nargs='*', help='The system merits are held in.')
    sub.add_argument('-r', '--redeem', action='store_true', help='Redeem all held merits.')
    sub.add_argument('--redeem-systems', nargs='+', help='Redeem all held merits.')
    sub.add_argument('-d', '--died', action='store_true', help='Zero out held merits.')
    sub.add_argument('-s', '--set',
                     help='Update the galmap progress us:them. Example: --set 3500:200')


@register_parser
def subs_kos(subs, prefix):
    """ Subcommand parsing for kos """
    desc = f"""KOS related commands. Examples:

**{prefix}kos report -c CMDR Name -s The Squad -r Explain why he is reported here.**
        Report an enemy PP user for KOS addition.
**{prefix}kos report --friendly -c CMDR Name -s The Squad -r Explain why he is reported here.**
        Request whitelisting a friendly.
**{prefix}kos search user_name**
        Search for a user, list all possible matching users.
**{prefix}kos pull**
        Pull all changes from sheet. Mainly to be used after modifying list.
    """
    sub = subs.add_parser(prefix + 'kos', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='KOS', system=[])
    CMD_MAP['KOS'] = 'kos'
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='KOS subcommands', dest='subcmd')
    subcmd = subcmds.add_parser('report', help='Report user to KOS.')
    subcmd.add_argument('-c', '--cmdr', nargs='+', help='The cmdr reported.')
    subcmd.add_argument('-s', '--squad', nargs='+', default=['Indy'], help='The squad of the cmdr reported.')
    subcmd.add_argument('-r', '--reason', nargs='+', default=['No reason given, assuming hostile.'], help='The reason reported.')
    subcmd.add_argument('--friendly', dest='is_friendly', default=False, action='store_true', help='Report user to kill.')
    subcmd = subcmds.add_parser('search', help='Search for a user.')
    subcmd.add_argument('term', help='The username to look for.')
    subcmd = subcmds.add_parser('pull', help='Pull data from sheet.')


@register_parser
def subs_near(subs, prefix):
    """ Subcommand parsing for bgs """
    desc = f"""Near related commands. Examples:

**{prefix}near control winters**
        Show the 10 nearest Winters controls to Sol (default system).
**{prefix}near control hudson rana**
        Show the 10 nearest Hudson controls to rana.
**{prefix}near prison sol**
        Show the nearest prison colonies to sol.
**{prefix}near if sol**
        Show the nearest interstellar factors to sol. Large pads only.
**{prefix}near if sol -m**
        Show the nearest interstellar factors to sol. Medium and large pads.
**{prefix}near data sol**
        Show the nearest data material trader to sol. Large pads only.
**{prefix}near raw sol**
        Show the nearest raw material trader to sol. Large pads only.
**{prefix}near manu sol**
        Show the nearest manufactured material trader to sol. Large pads only.
**{prefix}near human sol**
        Show the nearest human technology broker to sol. Large pads only.
**{prefix}near guardian sol**
        Show the nearest guardian technology broker to sol. Large pads only.
    """
    sub = subs.add_parser(prefix + 'near', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Near')
    CMD_MAP['Near'] = 'near'
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Near subcommands', dest='subcmd')

    subcmd = subcmds.add_parser('control', help='Find nearest control of a power.')
    subcmd.add_argument('power', help='A unique substring of power name.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')

    subcmd = subcmds.add_parser('prison', help='Find nearest prison megaship.')
    subcmd.add_argument('system', nargs='+', help='The system to centre on.')

    subcmd = subcmds.add_parser('if', help='Find nearest interstellar factors.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')
    subcmd.add_argument('-m', '--medium', action='store_true', default=False, help='The include mpad only stations.')

    subcmd = subcmds.add_parser('data', help='Find nearest data material trader.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')

    subcmd = subcmds.add_parser('raw', help='Find nearest raw material trader.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')

    subcmd = subcmds.add_parser('manu', help='Find nearest manufactured material trader.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')

    subcmd = subcmds.add_parser('human', help='Find nearest human technology broker.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')

    subcmd = subcmds.add_parser('guardian', help='Find nearest guardian technology broker.')
    subcmd.add_argument('system', nargs='+', help='The system to lookup.')


@register_parser
def subs_pin(subs, prefix):
    """ Subcommand parsing for pin """
    sub = subs.add_parser(prefix + 'pin', description='Make an objectives pin and keep updating.')
    sub.set_defaults(cmd='Pin')
    CMD_MAP['Pin'] = 'pin'


@register_parser
def subs_recruits(subs, prefix):
    """ Subcommand parsing for recruits """
    desc = f"""Manipulate your user settings. Examples:

**{prefix}recruits add CMDR Name Here -r R -p 1 --pmf Some PMF Group --notes Any notes go here**
        Add a single recruit to the sheet complete example. The following will be true:
            Name and discord name will be set to CMDR Name.
            Rank will be R (Recruit). Other values possible: M (Member) or V (Veteran)
            Platform will be 1 (PC). Other values possible: 2 (XBox), 3 (PS4), 1+2, 1+3, 1+2+3
            PMF field will be set to: Some PMF Group
            Notes will have: Any notes go here
**{prefix}recruits add CMDR Name Here**
        Add a single recruit to the sheet. Discord name assumed to be same. Rank will default to 'R', all other fields empty.
        Below optional flags can be added.
**{prefix}recruits add CMDR Name Here -d Discord name is different**
**{prefix}recruits add CMDR Name Here --discord Discord name is different**
        When discord name is different than in game name, use this flag to specify.
**{prefix}recruits add CMDR Name Here -p 1**
**{prefix}recruits add CMDR Name Here --platform 1**
        Will add the commander with platform 1 (PC) by default. Other values possible: 2 (XBox), 3 (PS4), 1+2, 1+3, 1+2+3
**{prefix}recruits add CMDR Name Here -r R**
**{prefix}recruits add CMDR Name Here --rank R**
        Will add the commander with rank R (Recruit). Other values possible: M (Member) or V (Veteran)
**{prefix}recruits add CMDR Name Here --pmf Some PMF Group**
        Will add the commander with PMF field to: Some PMF Group
**{prefix}recruits add CMDR Name Here --notes Any notes go here**
        Will add the commander with notes field to: Any notes go here
**{prefix}recruits add CMDR Similar --force
        Will add the commander that is similar to an existing one, bypasses similarity check.
    """
    sub = subs.add_parser(prefix + 'recruits', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Recruits')
    CMD_MAP['Recruits'] = 'recruits'
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Recruits subcommands', dest='subcmd')
    subcmd = subcmds.add_parser('add', help='Report user to KOS.')
    subcmd.add_argument('cmdr', nargs='+', help='The CMDR name and all other arguments that are optional.')
    subcmd.add_argument('-d', '--discord-name', nargs='+', help='The discord name differs from name.')
    subcmd.add_argument('-r', '--rank', default='R', choices=['R', 'M', 'V'], help='Rank to add as, R M or V.')
    subcmd.add_argument('-p', '--platform', default='1', choices=['1', '2', '3', '1+2', '1+3', '2+3', '1+2+3'], help='The platform, PC=1, Xbox=2, PS4=3. Combine with +')
    subcmd.add_argument('--pmf', nargs='+', default=[''], help='The player minor faction.')
    subcmd.add_argument('--notes', nargs='+', default=[''], help='The notes field.')
    subcmd.add_argument('--force', action='store_true', help='Force a similar user past similarity check.')


@register_parser
def subs_repair(subs, prefix):
    """ Subcommand parsing for repair """
    desc = f"""Find a station with both L pad and shipyard near you.

**{prefix}repair arnemil**
        Show suitable stations <= 15ly from Arnemil with arrival <= 1000ls
**{prefix}repair arnemil --arrival 4000**
**{prefix}repair arnemil -a 4000**
        Show suitable stations <= 15ly from Arnemil with arrival <= 4000ls
**{prefix}repair arnemil --distance 30**
**{prefix}repair arnemil -d 30**
        Show suitable stations <= 30ly from Arnemil with arrival <= 1000ls
**{prefix}repair arnemil -m**
        Show suitable stations to repair at. Include medium pads.
        Note: M pad stations won't have shipyards, will have outfitting though.
    """
    sub = subs.add_parser(prefix + 'repair', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Repair')
    CMD_MAP['Repair'] = 'repair'
    sub.add_argument('system', nargs="+", help='The reference system.')
    sub.add_argument('-a', '--arrival', type=int, default=1000, help='Station must be within arrival ls.')
    sub.add_argument('-d', '--distance', type=int, default=15, help='Max system distance')
    sub.add_argument('-m', '--medium', action='store_true', default=False, help='The include mpad only stations.')


@register_parser
def subs_route(subs, prefix):
    """ Subcommand parsing for route """
    desc = f"""Plot the shortest route between the listed systems.

**{prefix}route rana, sol, nanomam, frey**
        Show the route that minimizes jumps to visit all systems, starting at Rana
**{prefix}route rana, sol, nanomam, frey --optimum**
**{prefix}route rana, sol, nanomam, frey -o**
        Show the route that minimizes jumps to visit all systems, start is not fixed
    """
    sub = subs.add_parser(prefix + 'route', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Route')
    CMD_MAP['Route'] = 'route'
    sub.add_argument('system', nargs="+", help='The systems to plot.')
    sub.add_argument('-o', '--optimum', action="store_true", help="Determine the optimum solution.")


@register_parser
def subs_scout(subs, prefix):
    """ Subcommand parsing for scout """
    desc = f"""Generate a scouting list. Control output with flags.
    Examples:

**{prefix}scout -r 1**
        Generate a scouting list, use first round. Choices are 1, 2 or 3.
**{prefix}scout --custom Atropos, Alpha Fornacis, Rana, Anlave, NLTT 46621, 16 Cygni**
        Generate a scouting list, round the specified systems.
    """
    sub = subs.add_parser(prefix + 'scout', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Scout')
    CMD_MAP['Scout'] = 'scout'
    sub.add_argument('-r', '--round', type=int, help='The round to run.', choices=[1, 2, 3])
    sub.add_argument('-c', '--custom', nargs='+', help='Run a custom scout of systems.')


@register_parser
def subs_scrape(subs, prefix):
    """ Subcommand parsing for scrape """
    desc = f"""Perform manual scraping functions.

**{prefix}scrape power**
        Trigger an update of the sheets based on basic information query.
**{prefix}scrape held FW**
        Get information for all controls of power and update sheets.
        Update systems with held merits older than 7 hours.
**{prefix}scrape held ZH --hours 3**
        Get information for all controls of power and update sheets.
        Update systems with held merits older than 3 hours.
**{prefix}scrape bgs Abi, Rana**
        Scrape all bgs information listed systems and ...
            if a system is a control, scrape all exploited systems as well.
            otherwise just scrape the system as a one off.
    """
    sub = subs.add_parser(prefix + 'scrape', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Scrape')
    CMD_MAP['Scrape'] = 'scrape'
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Scrape subcommands', dest='subcmd')
    subcmd = subcmds.add_parser('bgs', help='Scrape the bgs .')
    subcmd.add_argument('systems', nargs='+', help='The systems to query for information.')
    subcmd = subcmds.add_parser('held', help='Query the held information for controls of a pwoer.')
    subcmd.add_argument('--hours', type=int, default=7, help='Update all systems older than this cutoff.')
    subcmd.add_argument('name', nargs='+', help='The name of the power to track.')
    subcmd = subcmds.add_parser('power', help='Refresh the information and push to sheets.')


@register_parser
def subs_snipe(subs, prefix):
    """ Subcommand parsing for snipe, the snipe variant of um """
    desc = f"""Get undermining targets and update their galmap status.
IMPORTANT: This is the __SNIPE__ sheet.

Examples:

**{prefix}snipe**
        Show current active undermining targets.
**{prefix}snipe burr**
        Show the current status and information on Burr.
**{prefix}snipe afl**
        Show the current status and information on AF Leopris, matched search.
**{prefix}snipe burr --set 60000:130**
**{prefix}snipe burr -s 60000:130**
        Set the galmap status of Burr to 60000 and opposition to 130%.
**{prefix}snipe burr --offset 4000**
**{prefix}snipe burr -o 4000**
        Set the offset difference of cmdr merits and galmap.
**{prefix}snipe --list**
        Show all outstanding merits by users and system.
**{prefix}snipe --npcs**
        List powerplay NPC ships by alligence.
    """
    sub = subs.add_parser(prefix + 'snipe', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Snipe', sheet_src=EUMSheet.snipe)
    CMD_MAP['Snipe'] = 'snipe'
    sub.add_argument('system', nargs='*', help='The system to update or show.')
    sub.add_argument('-s', '--set',
                     help='Set the status of the system, us:them. Example-> --set 3500:200')
    sub.add_argument('-o', '--offset', type=int, help='Set the system galmap offset.')
    sub.add_argument('-l', '--list', action='store_true', help='Show all outstanding merits on sheet.')
    sub.add_argument('--cycle', help='Set the cycle for snipe. Admins only.')
    sub.add_argument('--npcs', action='store_true', help='List powerplay NPC ships by alligence.')


@register_parser
def subs_status(subs, prefix):
    """ Subcommand parsing for status """
    sub = subs.add_parser(prefix + 'status', description='Info about this bot.')
    sub.set_defaults(cmd='Status')
    CMD_MAP['Status'] = 'status'


@register_parser
def subs_token(subs, prefix):
    """ Subcommand parsing for dist """
    desc = f"""Update the API token.

**{prefix}token <TOKEN>**
        Update the token for the api endpoint.
    """
    sub = subs.add_parser(prefix + 'token', description=desc, formatter_class=RawHelp)
    CMD_MAP['Token'] = 'token'
    sub.set_defaults(cmd='Token')
    sub.add_argument('token', help='The token in question.')


@register_parser
def subs_time(subs, prefix):
    """ Subcommand parsing for time """
    sub = subs.add_parser(prefix + 'time', description='Time in game and to ticks.')
    sub.set_defaults(cmd='Time')
    CMD_MAP['Time'] = 'time'


@register_parser
def subs_track(subs, prefix):
    """ Subcommand parsing for track """
    desc = f"""Track the locations of fleet carriers.

    **{prefix}track add -d 10 Rhea, Nanomam**
        Set up tracking to monitor systems within 10ly of Rhea and Nanomam.
    **{prefix}track remove -d 10 Rhea, Nanomam**
        Remove tracking of all systems in these bubbles, unless another overlpas.
    **{prefix}track show**
        Show all systems selected for tracking and the distances selected (exclude those auto included).
    **{prefix}track ids -a ABC-123, DCG-332**
        Track these ids LAST LOCATION, this tracking ignores the systems under active monitoring.
    **{prefix}track ids -r ABC-123, DCG-332**
        Remove tracking for these ids.
    **{prefix}track ids -s**
        Show currently tracked ids.
    **{prefix}track channel #mention_channel**
        Set channel to notify major events on an ongoing basis.
    **{prefix}track scan**
        Scan the ids from sheet for updates.
    """
    sub = subs.add_parser(prefix + 'track', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Track')
    CMD_MAP['Track'] = 'track'
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Track subcommands', dest='subcmd')
    subcmd = subcmds.add_parser('channel', help='Set the channel to send notifications to of important changes.')
    subcmd = subcmds.add_parser('scan', help='Manually scan the IDs for carriers from sheet.')
    subcmd = subcmds.add_parser('show', help='Show a summary of systems set to tracking.')
    subcmd = subcmds.add_parser('add', help='Add the systems given to tracking..')
    subcmd.add_argument('systems', nargs='+', help='The systems to track.')
    subcmd.add_argument('-d', '--distance', type=int, default=15, help='The max distance to consider.')
    subcmd = subcmds.add_parser('remove', help='Remove the systems from tracking.')
    subcmd.add_argument('systems', nargs='+', help='The systems to track.')
    subcmd = subcmds.add_parser('ids', help='Track specific ids that are reported.')
    subcmd.add_argument('-a', '--add', nargs='+', help='The ids to add.')
    subcmd.add_argument('-r', '--remove', nargs='+', help='The ids to remove.')
    subcmd.add_argument('-q', '--squad', nargs='+', default=[''], help='The squad of the carrier ids.')
    subcmd.add_argument('-s', '--show', action='store_true', help='Show all ids in tracking.')


@register_parser
def subs_trigger(subs, prefix):
    """ Subcommand parsing for trigger """
    desc = f"""Predict the expected triggers for the requested systems.

**{prefix}trigger Rana, Arnemil**
        Calculate triggers for requested systems from Hudson HQ.
**{prefix}trigger Rana, Arnemil --power winters**
**{prefix}trigger Rana, Arnemil -p winters**
        Calculate triggers for requested systems from Winters HQ.
    """
    sub = subs.add_parser(prefix + 'trigger', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Trigger')
    CMD_MAP['Trigger'] = 'trigger'
    sub.add_argument('system', nargs='+', help='The system(s) to calculate triggers for.')
    sub.add_argument('-p', '--power', nargs='+', default=["hudson"], help='The power to calculate from.')


@register_parser
def subs_um(subs, prefix):
    """ Subcommand parsing for um """
    desc = f"""Get undermining targets and update their galmap status. Examples:

**{prefix}um**
        Show current active undermining targets.
**{prefix}um burr**
        Show the current status and information on Burr.
**{prefix}um afl**
        Show the current status and information on AF Leopris, matched search.
**{prefix}um burr --set 60000:130**
**{prefix}um burr -s 60000:130**
        Set the galmap status of Burr to 60000 and opposition to 130%.
**{prefix}um burr --offset 4000**
**{prefix}um burr -o 4000**
        Set the offset difference of cmdr merits and galmap.
**{prefix}um burr --priority Medium**
**{prefix}um burr -p Medium**
        Set the priority for the undermining system.
**{prefix}um --list**
        Show all outstanding merits by users and system.
**{prefix}um --npcs**
        List powerplay NPC ships by alligence.
    """
    sub = subs.add_parser(prefix + 'um', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='UM', sheet_src=EUMSheet.main)
    CMD_MAP['UM'] = 'um'
    sub.add_argument('system', nargs='*', help='The system to update or show.')
    sub.add_argument('-s', '--set',
                     help='Set the status of the system, us:them. Example-> --set 3500:200')
    sub.add_argument('-o', '--offset', type=int, help='Set the system galmap offset.')
    sub.add_argument('-p', '--priority', nargs='+', help='The priority to set for the system.')
    sub.add_argument('-l', '--list', action='store_true', help='Show all outstanding merits on sheet.')
    sub.add_argument('--npcs', action='store_true', help='List powerplay NPC ships by alligence.')


@register_parser
def subs_user(subs, prefix):
    """ Subcommand parsing for user """
    desc = f"""Manipulate your user settings. Examples:

**{prefix}user**
        Show your sheet name, crys and merits per sheet.
**{prefix}user --name Not Gears**
        Set your name to 'Not Gears'.
**{prefix}user --cry The bots are invading!**
        Set your battle cry to "The bots are invading!".
    """
    sub = subs.add_parser(prefix + 'user', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='User')
    CMD_MAP['User'] = 'user'
    sub.add_argument('--cry', nargs='+', help='Set your tag/cry in the sheets.')
    sub.add_argument('--name', nargs='+', help='Set your name in the sheets.')


@register_parser
def subs_vote(subs, prefix):
    """ Subcommand parsing for vote """
    desc = f"""Cast a vote.

**{prefix}vote**
        Show the current vote instructions.
**{prefix}vote cons 5**
        Vote consolidation with a strength of 5.
**{prefix}vote prep 15**
        Vote preparation with strength 15.
        In this case 3 accounts that have 5 vote strength each.
**{prefix}vote --set 50**
**{prefix}vote -s 50**
        Set the vote goal to 50.
**{prefix}vote --display**
        Toggle the manual display of vote action.
**{prefix}vote --summary**
        Create a summary of all votes this cycle and return in a text file.
    """
    sub = subs.add_parser(prefix + 'vote', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Voting')
    CMD_MAP['Voting'] = 'vote'
    sub.add_argument('vote_tuple', nargs='*', help='format either: cons 1 or 5 prep')
    sub.add_argument('--set', '-s', type=int, help='Set vote goal.')
    sub.add_argument('--display', '-d', action='store_true', help='Display the current vote goal.')
    sub.add_argument('--summary', action='store_true', help='Display the admin summary of votes this cycle.')


@register_parser
def subs_whois(subs, prefix):
    """ Subcommand parsing for whois """
    desc = f"""Lookup information for a commander on Inara.cz

**{prefix}whois GearsandCogs**
        Find out who this GearsandCogs fellow is ...
**{prefix}whois gears**
        Search for all CMDRs with 'gears' in their name
**{prefix}whois \\\\--hypehnCmdr--**
        Search for a CMDR with a hyphen at the front of their name.
    """
    sub = subs.add_parser(prefix + 'whois', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='WhoIs')
    CMD_MAP['WhoIs'] = 'whois'
    sub.add_argument('cmdr', nargs='+', help='Commander name.')


@register_parser
def subs_sum(subs, prefix):
    """ Subcommand parsing for sum """
    desc = f"""Display the sum of all fort data in our sheets

**{prefix}sum**
        Display the command. Warning Veteran rank minimum required.
    """
    sub = subs.add_parser(prefix + 'sum', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Summary')
    CMD_MAP['Summary'] = 'sum'


def parse_vote_tuple(vote_tuple):
    """
    Parse the vote tuple, it was requested position be ignored.
    Validate both input values.

    Returns: (vote_type, amount)
        vote_type: The enum EVoteType that matches user invocation.
        amount: The integer value of vote.

    Raises:
        ThrowArggumentParser: Anything went wrong with parsing.
    """
    try:
        if len(vote_tuple) != 2:
            raise ValueError("Tuple should be of length 2.")

        if vote_tuple[0] in VALID_VOTE_TYPES:
            vote_type, amount = vote_tuple
        elif vote_tuple[1] in VALID_VOTE_TYPES:
            amount, vote_type = vote_tuple
        else:
            logging.getLogger(__name__).error("Invalid tuple for voting: %s", str(vote_tuple))
            raise ValueError("Values in tuple were invalid.")

        vote_type = EVoteType.cons if vote_type == 'cons' else EVoteType.prep
        amount = int(amount)
    except ValueError as exc:
        raise ThrowArggumentParser from exc

    return vote_type, amount
