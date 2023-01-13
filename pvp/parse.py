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
**{prefix}admin filter**
        Rerun the filtering stage on all past uploads.
        Filtered versions of uploads will be created and uploaded to archive channel.
**{prefix}admin filter attachment_name**
        Rerun the filtering stage on all past uploads.
        Resume AFTER the provided attachment seen.
**{prefix}admin regenerate**
        Use only when log corruption expected or change in log parsing.
        Reparses all uploaded logs to regenerate database.
**{prefix}admin stats**
        Recreate the pvp stats table and recompute all stats.
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
    subcmd.add_argument('attachment', nargs='*', help='The name of the attachment to start AFTER..')
    subcmd = subcmds.add_parser('regenerate', help='Regenerate the PVP database.')
    subcmd = subcmds.add_parser('stats', help='Regenerate the PVPStats for all CMDRs.')
    subcmd = subcmds.add_parser('prune', help='Delete messages in one or more mentioned channels.')
    subcmd = subcmds.add_parser('prune_bulk', help='Delete messages in bulk for one or more mentioned channels.')


@register_parser
def subs_help(subs, prefix):
    """ Subcommand parsing for help """
    sub = subs.add_parser(prefix + 'help', description='Show overall help message.')
    sub.set_defaults(cmd='Help')
    CMD_MAP['Help'] = 'help'


@register_parser
def subs_log(subs, prefix):
    """ Subcommand parsing for log """
    desc = f"""To see the log of recent PVP tracked events.

**{prefix}log**
        See the last 10 pvp events bot tracked.
    """
    sub = subs.add_parser(prefix + 'log', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Log')
    CMD_MAP['Log'] = 'log'


@register_parser
def subs_match(subs, prefix):
    """ Subcommand parsing for match """
    desc = f"""To prepare a match.

**{prefix}match**
        Join an organized match.
**{prefix}match list**
        See current list of registred player.
**{prefix}match setup 4,6,8,10**
**{prefix}match setup**
        Setup a match with limits if given, else any amount of players will be accepted.
**{prefix}match add Gears, Prozer**
        Add Gears and Prozer to the list of players.
**{prefix}match remove Gears, Prozer**
        Remove Gears and Prozer to the list of players.
**{prefix}match start**
        Create teams and start the match. Useless if player limits reached.
    """
    
    sub = subs.add_parser(prefix + 'match', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Match')
    CMD_MAP['Match'] = 'match'
    subcmds = sub.add_subparsers(title='subcommands',
                                 description='Match subcommands', dest='subcmd')
    subcmd = subcmds.add_parser('add', help='Add the mentionned discord user(s) to the current match.')
    subcmd.add_argument('players', nargs='+', help='The player to add.')
    subcmd = subcmds.add_parser('remove', help='Remove the mentionned discord user(s) to the current match.')
    subcmd.add_argument('players', nargs='+', help='The player to remove.')
    subcmd = subcmds.add_parser('setup', help='Create a new match.')
    subcmd.add_argument('limits', nargs='*', type=int, default= 20, 
                        help='The total player limit. Default : 20.')
    subcmds.add_parser('start', help='Start the match.')
    subcmds.add_parser('list', 
                     help='Give the list of all registrer players for the current match.')


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


def reuse_parsers():
    """
    Register for PVP parsing all subcommands that can be shared from cog bot.
    """
    for suffix in REUSE_SUBS:
        register_parser(getattr(cog.parse, f'subs_{suffix}'))


# On import register here all reused commands from cog
if cog.parse.subs_dist not in PARSERS:
    reuse_parsers()
