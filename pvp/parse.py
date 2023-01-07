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
**{prefix}admin reparse**
        Use only when log corruption expected or change in log parsing.
        Reparses all uploaded logs to regenerate database.
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
    subcmd = subcmds.add_parser('regenerate', help='Regenerate the PVP database.')


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
if not PARSERS:
    reuse_parsers()


@register_parser
def subs_help(subs, prefix):
    """ Subcommand parsing for help """
    sub = subs.add_parser(prefix + 'help', description='Show overall help message.')
    sub.set_defaults(cmd='Help')
    CMD_MAP['Help'] = 'help'
