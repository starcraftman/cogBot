"""
Parsing only for pvp bot.
See cog.parse for more info.
"""
from argparse import RawDescriptionHelpFormatter as RawHelp

import cog.parse

PARSERS = []
CMD_MAP = cog.parse.CMD_MAP
REUSE_SUBS = ['dist', 'donate', 'feedback', 'near', 'repair', 'route', 'status', 'time', 'trigger', 'whois']


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
    subcmds.add_parser('show_rules', help='Show existing command rules.')

    subcmds.add_parser('filter', help='Filter all existing log uploads for events of interest.')
    subcmds.add_parser('kos', help='Update KOS list and PVPKills for KOS.')
    subcmds.add_parser('regenerate', help='Regenerate the PVP database.')
    subcmds.add_parser('prune', help='Delete messages in one or more mentioned channels.')
    subcmds.add_parser('prune_bulk', help='Delete messages in bulk for one or more mentioned channels.')


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

**{prefix}match start player1, player2 @player3**
        Create a match with the following players.
        Mentioned players will be added by their display name.
**{prefix}match start --limit 4 player1, player2 @player3**
        Create a match with 4 player limit and add the following players.
**{prefix}match add player2 @player3**
        Add player2 and @player3 to match, will update teams if rolled.
**{prefix}match remove player2 @player3**
        Remove player2 and @player3 from match, will update teams if rolled.
    """

    sub = subs.add_parser(prefix + 'match', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='MatchShort', subcmd='start')
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Match subcommands', dest='subcmd')

    subcmd = subcmds.add_parser('start', help='Create the initial match.')
    subcmd.add_argument('--limit', default=8, type=int, help='Limit of players in match.')
    subcmd.add_argument('players', nargs='*', default=[], help='The player to add.')

    subcmd = subcmds.add_parser('add', help='Add the mentioned commanders.')
    subcmd.add_argument('id', type=int, help='The match id.')
    subcmd.add_argument('players', nargs='*', default=[], help='The player to add.')

    subcmd = subcmds.add_parser('remove', help='Remove the mentioned commanders.')
    subcmd.add_argument('id', type=int, help='The match id.')
    subcmd.add_argument('players', nargs='*', default=[], help='The player to add.')

    CMD_MAP['MatchShort'] = 'match'


def subs_match_old(subs, prefix):
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
