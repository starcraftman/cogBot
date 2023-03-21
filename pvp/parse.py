"""
Parsing only for pvp bot.
See cog.parse for more info.
"""
from argparse import RawDescriptionHelpFormatter as RawHelp

import cogdb.achievements
import cog.parse

PARSERS = []
CMD_MAP = cog.parse.CMD_MAP
REUSE_SUBS = ['dist', 'donate', 'feedback', 'near', 'repair', 'route', 'status', 'time', 'trigger', 'whois']
PVP_EVENTS = ['PVPDeath', 'PVPKill', 'PVPInterdiction', 'PVPInterdicted', 'PVPEscapedInterdicted']


def make_parser(prefix):
    """
    Returns the bot parser.
    """
    parser = cog.parse.ThrowArggumentParser(prog='', description='pvp discord bot')

    subs = parser.add_subparsers(title='subcommands',
                                 description='The subcommands of pvp')

    for func in PARSERS:
        func(subs, prefix)

    return parser


def register_parser(func):
    """ Simple registration function, use as decorator. """
    PARSERS.append(func)
    return func


@register_parser
def subs_achieve(subs, prefix):
    """ Subcommand parsing for cmdr """
    desc = f"""Manage the pvp related achievements.

**{prefix}achieve add_age 100 --role_name 100 Days --role_colour b20000 --role_description You made it 100 days.***
        Add an achievement that will be unlocked after being on server for 100 days.
        The role_name, role_colour and role_description are required.
        If required, the bot will create the associated role with the given name and colour.
        Role will be assigned after 100 days.
**{prefix}achieve add_event PVPKill Bad Guy --role_name Smoked Bad Guy --role_colour b20000 --role_description You made it 100 days.***
        Add an achievement that will be unlocked after a specific event involving a specific CMDR.
        The role_name, role_colour and role_description are required.
        If required, the bot will create the associated role with the given name and colour.
        Role will be assigned after reporting a log with kill of CMDR Bad Guy.
**{prefix}achieve add_stat kills 12 --role_name Dozen Kills --role_colour b20000 --role_description You got a dozen kills.***
        Add an achievement that will be unlocked after getting 12 confirmed kills by log.
        The role_name, role_colour and role_description are required.
        If required, the bot will create the associated role with the given name and colour.
        Role will be assigned after reporting a log and the CMDR's statistic matches requirement.
**{prefix}achieve remove Role Name**
        Remove the achievement with the associated Role Name.
**{prefix}achieve update Role Name --role_name New Role Name --role_colour b40000 --role_description A new role description.**
        Update the role named by Role Name.
        If --role_name present, set Role Name to New Role Name.
        If --role_colour present, set the colour.
        If --role_description present, set the description.
    """
    sub = subs.add_parser(prefix + 'achieve', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Achievement')
    CMD_MAP['Achievement'] = 'achieve'
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Achievement subcommands', dest='subcmd')

    subcmd = subcmds.add_parser('add_age', help='Add a new stat check.')
    subcmd.add_argument('days_required', type=int, help='The days required for the role.')
    subcmd.add_argument('--role_name', nargs='+', help='The the name of the role.')
    subcmd.add_argument('--role_colour', help='The the colour of the role.')
    subcmd.add_argument('--role_description', nargs='+', help='The the description of the role.')

    subcmd = subcmds.add_parser('add_event', help='Add a new stat check.')
    subcmd.add_argument('event', choices=PVP_EVENTS, help='The the PVP event to check.')
    subcmd.add_argument('cmdr', nargs='+', help='The the CMDR to look for.')
    subcmd.add_argument('--role_name', nargs='+', help='The the name of the role.')
    subcmd.add_argument('--role_colour', help='The the colour of the role.')
    subcmd.add_argument('--role_description', nargs='+', help='The the description of the role.')

    subcmd = subcmds.add_parser('add_stat', help='Add a new stat check.')
    subcmd.add_argument('stat', choices=cogdb.achievements.VALID_STATS, help='The the statistic to check.')
    subcmd.add_argument('amount', type=int, help='The the amount to assign at.')
    subcmd.add_argument('--role_name', nargs='+', help='The the name of the role.')
    subcmd.add_argument('--role_colour', help='The the colour of the role.')
    subcmd.add_argument('--role_description', nargs='+', help='The the description of the role.')

    subcmd = subcmds.add_parser('remove', help='Update an existing achievement.')
    subcmd.add_argument('name', nargs='+', help='The the name of the existing role.')

    subcmd = subcmds.add_parser('update', help='Update an existing achievement.')
    subcmd.add_argument('name', nargs='+', help='The the name of the existing role.')
    subcmd.add_argument('--role_name', nargs='+', help='The the name of the role.')
    subcmd.add_argument('--role_colour', help='The the colour of the role.')
    subcmd.add_argument('--role_description', nargs='+', help='The the description of the role.')


@register_parser
def subs_admin(subs, prefix):
    """ Subcommand parsing for admin """
    desc = f"""Admin only commands. Examples:

**{prefix}admin add @GearsandCogs  **
        Add GearsandCogs to the admin group.
**{prefix}admin remove @GearsandCogs**
        Remove GearsandCogs from the admin group.
**{prefix}admin filter**
        Rerun the filtering stage on all past uploads.
        Filtered versions of uploads will be created and uploaded to archive channel.
**{prefix}admin kos**
        Update the KOS list then update PVPKills based on current KOS targets.
**{prefix}admin regenerate**
        Use only when log corruption expected or change in log parsing.
        Reparses all uploaded logs to regenerate database.
**{prefix}admin prune #mention_channel_1 #mention_channel_2**
        Delete all messages in mentioned channels.
**{prefix}admin prune_bulk #mention_channel_1 #mention_channel_2**
        Delete all messages in mentioned channels.
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
    subcmd = subcmds.add_parser('filter', help='Filter all existing log uploads for events of interest.')
    subcmd = subcmds.add_parser('kos', help='Update KOS list and PVPKills for KOS.')
    subcmd = subcmds.add_parser('regenerate', help='Regenerate the PVP database.')
    subcmd = subcmds.add_parser('prune', help='Delete messages in one or more mentioned channels.')
    subcmd = subcmds.add_parser('prune_bulk', help='Delete messages in bulk for one or more mentioned channels.')


@register_parser
def subs_cmdr(subs, prefix):
    """ Subcommand parsing for cmdr """
    desc = f"""Manage your PVP cmdr registration.

**{prefix}cmdr
        Rerun the first time cmdr setup.
    """
    sub = subs.add_parser(prefix + 'cmdr', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Cmdr')
    CMD_MAP['Cmdr'] = 'cmdr'


@register_parser
def subs_help(subs, prefix):
    """ Subcommand parsing for help """
    sub = subs.add_parser(prefix + 'help', description='Show overall help message.')
    sub.set_defaults(cmd='Help')
    CMD_MAP['Help'] = 'help'


@register_parser
def subs_log(subs, prefix):
    """ Subcommand parsing for log """
    desc = f"""To see the complete log of your PVP events.
Order is based on earliest first.

**{prefix}log**
        Get files that list ALL events recorded inthe database, most recent first.
**{prefix}log kills locations**
        Get files that list ALL location and kills recorded inthe database, most recent first.
**{prefix}log -l 10**
**{prefix}log --limit 10**
        Get files that list the last 10 most recent events.
**{prefix}log --cmdr BadGuy**
        Get files that list ALL events recorded in the database.
        Where an event involves a second commander (Kill, Death, Interdiction), only select those with BadGuy involved.
        Where an event doesn't involve a second commander (Location), show those as normal.
**{prefix}log --after 2022-06-10T14:31:00**
        Get files that list ALL events AFTER the date specified. Format required.
    """
    sub = subs.add_parser(prefix + 'log', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Log')
    CMD_MAP['Log'] = 'log'
    sub.add_argument('events', nargs='*', default=[], help='The log events to put in file.')
    sub.add_argument('--after', nargs='?', help='Show events after this date.')
    sub.add_argument('--cmdr', nargs='*', help='Limit query to CMDR named.')
    sub.add_argument('-l', '--limit', nargs='?', type=int, help='Limit to the most recent num events.')


@register_parser
def subs_recent(subs, prefix):
    """ Subcommand parsing for recent """
    desc = f"""To see a limited subset of recent logs (or ones after a date).
Order is based on most recent first.

**{prefix}recent**
        Show a combined list of all recent events, most recent first.
**{prefix}recent kills locations**
        Show a list of all recent location and kill events, most recent first.
**{prefix}recent -l 10**
**{prefix}recent --limit 10**
        Show a combined list of the 10 most recent events, most recent first.
**{prefix}recent --cmdr BadGuy**
        Show a combined list of the most recent events.
        Where an event involves a second commander (Kill, Death, Interdiction), only select those with BadGuy involved.
        Where an event doesn't involve a second commander (Location), show those.
**{prefix}recent --after 2022-06-10T14:31:00**
        Show a combined list of the recent events after the date specified.
    """
    sub = subs.add_parser(prefix + 'recent', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Recent')
    CMD_MAP['Recent'] = 'recent'
    sub.add_argument('events', nargs='*', default=[], help='The log events to put in file.')
    sub.add_argument('--after', nargs='?', help='Show events after this date.')
    sub.add_argument('--cmdr', nargs='*', help='Limit query to CMDR named.')
    sub.add_argument('-l', '--limit', nargs='?', default=50, type=int, help='Limit to the most recent num events.')


@register_parser
def subs_privacy(subs, prefix):
    """ Subcommand parsing for privacy """
    desc = f"""Provide the privacy statement to user.

**{prefix}privacy**
        Print the latest privacy statement for user to view.
**{prefix}privacy --delete**
        Upon confirmation perform a complete deletion of all information stored about you in bot.
**{prefix}privacy version 1.0**
        Print the privacy statement requested.
**{prefix}privacy version**
        Print the available privacy versions
    """
    sub = subs.add_parser(prefix + 'privacy', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Privacy')
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Privacy subcommands', dest='subcmd')
    subcmd = subcmds.add_parser('delete', help='Delete all CMDR info.')
    subcmd = subcmds.add_parser('version', help='Display a selected version.')
    subcmd.add_argument('num', nargs='?', type=float, help='The version number.')


@register_parser
def subs_r(subs, prefix):
    """ Subcommand parsing for log """
    desc = f"""To register for a pvp event setup in the channel.

**{prefix}r**
        Join the existing pvp event in this channel.
    """
    sub = subs.add_parser(prefix + 'r', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Match', subcmd='join')


@register_parser
def subs_match(subs, prefix):
    """ Subcommand parsing for match """
    desc = f"""To prepare a match.

**{prefix}match**
        Show the state of the match.
**{prefix}match setup num_players**
**{prefix}match setup**
        Setup a match with num_players as maximum (>=2).
        If no number provided, default max is 20.
**{prefix}match join**
        Join yourself to the current match.
**{prefix}match leave**
        Leave yourself from the current match.
**{prefix}match add Gears or @Prozer**
        Add Gears and Prozer to the list of players.
**{prefix}match remove Gears or @Prozer**
        Remove Gears and Prozer to the list of players.
**{prefix}match start**
        Create teams and start the match. Useless if player limit reached.
**{prefix}match cancel**
        Cancel current pending match.
**{prefix}match reroll**
        Reroll current teams match.
**{prefix}match win Gears or @Prozer**
        The started match concluded with a victory from the Gears' / Prozer's Team.
    """

    sub = subs.add_parser(prefix + 'match', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Match')
    CMD_MAP['Match'] = 'match'
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Match subcommands', dest='subcmd')
    subcmd = subcmds.add_parser('add', help='Add the mentionned discord user(s) to the current match.')
    subcmd.add_argument('players', nargs='*', default=[], help='The player to add.')
    subcmd = subcmds.add_parser('remove', help='Remove the mentionned discord user(s) to the current match.')
    subcmd.add_argument('players', nargs='*', default=[], help='The player to remove.')
    subcmd = subcmds.add_parser('win', help='Terminate a match by giving a win to a team.')
    subcmd.add_argument('player', nargs='*', default=[], help='The player within the winning team.')
    subcmd = subcmds.add_parser('setup', help='Create a new match.')
    subcmd.add_argument('limit', nargs='?', type=int, default=20,
                        help='The total player limit. Default: 20.')
    subcmds.add_parser('start', help='Start the match.')
    subcmds.add_parser('cancel', help='Cancel the match.')
    subcmds.add_parser('reroll', help='Reroll teams.')
    subcmds.add_parser('join', help='Join the current match.')
    subcmds.add_parser('leave', help='Leave the current match.')


@register_parser
def subs_stats(subs, prefix):
    """ Subcommand parsing for stats """
    desc = f"""To see your statistics.

**{prefix}stats**
        See your stats.
**{prefix}stats prozer**
        See CMDR prozer's stats. This is not case sensitive.
    """
    sub = subs.add_parser(prefix + 'stats', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Stats')
    CMD_MAP['Stats'] = 'stats'
    sub.add_argument('name', nargs='*', default=None, help='The cmdr name to look up.')


@register_parser
def subs_squadstats(subs, prefix):
    """ Subcommand parsing for squadron stats """
    desc = f"""To see squadron statistics

**{prefix}squad**
        See the combined stats of your squadron.
**{prefix}squad Best Squad Ever**
        See the stats for the named squadron. This is not case sensitive.
    """
    sub = subs.add_parser(prefix + 'squad', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='SquadStats')
    CMD_MAP['SquadStats'] = 'squad'
    sub.add_argument('name', nargs='*', default=None, help='The squadron name to look up.')


def reuse_parsers():
    """
    Register for PVP parsing all subcommands that can be shared from cog bot.
    """
    for suffix in REUSE_SUBS:
        register_parser(getattr(cog.parse, f'subs_{suffix}'))


# On import register here all reused commands from cog
if cog.parse.subs_dist not in PARSERS:
    reuse_parsers()
