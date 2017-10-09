"""
Common functions.
"""
from __future__ import absolute_import, print_function
import logging
import os
import sys

import argparse
from argparse import RawDescriptionHelpFormatter as RawHelp

import cog.exc
import cog.sheets
import cog.tbl
from cog.util import ROOT_DIR


class ThrowArggumentParser(argparse.ArgumentParser):
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


class ModFormatter(logging.Formatter):
    """
    Add a relmod key to record dict.
    This key tracks a module relative this project' root.
    """
    def format(self, record):
        relmod = record.__dict__['pathname'].replace(ROOT_DIR + os.path.sep, '')
        record.__dict__['relmod'] = relmod[:-3]
        return super().format(record)


def make_parser(prefix):
    """
    Returns the bot parser.
    """
    parser = ThrowArggumentParser(prog='', description='simple discord bot')

    subs = parser.add_subparsers(title='subcommands',
                                 description='The subcommands of cog')

    sub = subs.add_parser(prefix + 'help', description='Show overall help message.')
    sub.set_defaults(cmd='Help')

    desc = """Give feedback or report a bug. Example:

    {prefix}bug Explain what went wrong ...\n          File a bug report or give feedback.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'feedback', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Feedback')
    sub.add_argument('content', nargs='+', help='The bug description or feedback.')

    sub = subs.add_parser(prefix + 'status', description='Info about this bot.')
    sub.set_defaults(cmd='Status')

    sub = subs.add_parser(prefix + 'time', description='Time in game and to ticks.')
    sub.set_defaults(cmd='Time')

    for suffix in ['admin', 'bgs', 'drop', 'fort', 'hold', 'um', 'user']:
        func = getattr(sys.modules[__name__], 'subs_' + suffix)
        func(subs, prefix)

    return parser


def subs_admin(subs, prefix):
    """ Subcommand parsing for admin """
    desc = """Admin only commands. Examples:

    {prefix}admin add @GearsandCogs\n          Add GearsandCogs to the admin group.
    {prefix}admin remove @GearsandCogs\n          Remove GearsandCogs from the admin group.
    {prefix}admin add BGS #hudson_bgs\n          Whitelist bgs command for hudson_bgs channel.
    {prefix}admin remove BGS #hudson_bgs\n          Remove whitelist for bgs command in hudson_bgs.
    {prefix}admin add Drop FRC Member\n          Whitelist bgs command for members with role "FRC Member".
    {prefix}admin remove Drop FRC Member\n          Remove whitelist for bgs command of users with role "FRC Member".
    {prefix}admin cast A message here\n          Broadcast a message to all channels.
    {prefix}admin deny\n          Toggle command processing.
    {prefix}admin dump\n          Dump the database to console to inspect.
    {prefix}admin halt\n          Shutdown this bot after short delay.
    {prefix}admin scan\n          Pull and parse the latest sheet information.
    {prefix}admin info @User\n          Information about the mentioned User, DMed to admin.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'admin', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Admin')
    admin_subs = sub.add_subparsers(title='subcommands',
                                    description='Admin subcommands', dest='subcmd')
    admin_sub = admin_subs.add_parser('add', help='Add an admin or permission.')
    admin_sub.add_argument('rule_cmd', nargs='?', help='The the command to restrict.')
    admin_sub.add_argument('role', nargs='*', help='The role name, if a role restriction.')
    admin_sub = admin_subs.add_parser('remove', help='Remove an admin or permission.')
    admin_sub.add_argument('rule_cmd', nargs='?', help='The the command to restrict.')
    admin_sub.add_argument('role', nargs='*', help='The role name, if a role restriction.')
    admin_sub = admin_subs.add_parser('cast', help='Broadcast a message to all channels.')
    admin_sub.add_argument('content', nargs='+', help='The message to send, no hyphens.')
    admin_subs.add_parser('deny', help='Toggle command processing.')
    admin_subs.add_parser('dump', help='Dump the db to console.')
    admin_subs.add_parser('halt', help='Stop accepting commands and halt bot.')
    admin_subs.add_parser('scan', help='Scan the sheets for updates.')
    admin_sub = admin_subs.add_parser('info', help='Get info about discord users.')
    admin_sub.add_argument('user', nargs='?', help='The user to get info on.')


def subs_bgs(subs, prefix):
    """ Subcommand parsing for bgs """
    desc = """BGS related commands. Examples:

    {prefix}bgs age 16 cygni\n          Show exploiteds in 16 Cygni bubble by age.
    {prefix}bgs inf Sol\n          Show the factions and their influence in Sol.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'bgs', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='BGS')
    bgs_subs = sub.add_subparsers(title='subcommands',
                                  description='BGS subcommands', dest='subcmd')
    bgs_sub = bgs_subs.add_parser('age', help='Get the age of exploiteds around a control.')
    bgs_sub.add_argument('system', nargs='+', help='The system to lookup.')
    bgs_sub = bgs_subs.add_parser('inf', help='Get the influence of factions inside a system.')
    bgs_sub.add_argument('system', nargs='+', help='The system to lookup.')


def subs_drop(subs, prefix):
    """ Subcommand parsing for drop """
    desc = """Update the cattle sheet when you drop at a system.
    Amount dropped must be in range [-800, 800]
    Examples:

    {prefix}drop 600 Rana\n           Drop 600 supplies for yourself at Rana.
    {prefix}drop -50 Rana\n           Made a mistake? Subract 50 forts from your drops at Rana.
    {prefix}drop 600 Rana @rjwhite\n           Drop 600 supplies for rjwhite Rana.
    {prefix}drop 600 lala\n           Drop 600 supplies for yourself at Lalande 39866, search used when name is not exact.
    {prefix}drop 600 Rana --set 4560:2000\n           Drop 600 supplies at Rana for yourself, set fort status to 4500 and UM status to 2000.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'drop', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Drop')
    sub.add_argument('amount', type=int, help='The amount to drop.')
    sub.add_argument('system', nargs='+', help='The system to drop at.')
    sub.add_argument('--set',
                     help='Set the fort:um status of the system. Example-> --set 3400:200')


def subs_fort(subs, prefix):
    """ Subcommand parsing for fort """
    desc = """Show fortification status and targets. Examples:

    {prefix}fort\n           Show current fort objectives.
    {prefix}fort --miss 1000\n          Show all systems missing <= 1000 supplies.
    {prefix}fort --next 5\n           Show the next 5 fortification targets (excluding Othime and skipped).
    {prefix}fort --summary\n           Show a breakdown by states of our systems.
    {prefix}fort alpha\n           Show the fortification status of Alpha Fornacis.
    {prefix}fort alpha, sol, ran\n           Show the fortification status of Alpha Fornacis, Sol and Rana.
    {prefix}fort Othime --set 7500:2000\n           Set othime to 7500 fort status and 2000 um status.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'fort', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Fort')
    sub.add_argument('system', nargs='*', help='Select this system.')
    sub.add_argument('--set',
                     help='Set the fort:um status of system. Example-> --set 3400:200')
    sub.add_argument('--summary', action='store_true',
                     help='Provide an overview of the fort systems.')
    sub.add_argument('--miss', type=int,
                     help='Show systems missing <= MISS merits.')
    # sub.add_argument('-l', '--long', action='store_true', help='Show systems in table format')
    sub.add_argument('-n', '--next', type=int,
                     help='Show the next NUM fort targets after current')


def subs_hold(subs, prefix):
    """ Subcommand parsing for hold """
    desc = """Update a user's held or redeemed merits. Examples:

    {prefix}hold 1200 burr\n           Set your held merits at Burr to 1200.
    {prefix}hold 900 af leopris @Memau\n           Set held merits at System AF Leopris to 900 held for Memau.
    {prefix}hold --died\n           Reset your held merits to 0 due to dying.
    {prefix}hold --redeem\n           Move all held merits to redeemed column.
    {prefix}hold 720 burr --set 60000:130\n           Update held merits to 720 at Burr expansion and set progress to 60000 merits and 130% opposition.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'hold', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='Hold')
    sub.add_argument('amount', nargs='?', type=int, help='The amount of merits held.')
    sub.add_argument('system', nargs='*', help='The system merits are held in.')
    sub.add_argument('--redeem', action='store_true', help='Redeem all held merits.')
    sub.add_argument('--died', action='store_true', help='Zero out held merits.')
    sub.add_argument('--set', help='Update the galmap progress us:them. Example: --set 3500:200')


def subs_um(subs, prefix):
    """ Subcommand parsing for um """
    desc = """Get undermining targets and update their galmap status. Examples:

    {prefix}um\n           Show current active undermining targets.
    {prefix}um burr\n           Show the current status and information on Burr.
    {prefix}um afl\n           Show the current status and information on AF Leopris, matched search.
    {prefix}um burr --set 60000:130\n           Set the galmap status of Burr to 60000 and opposition to 130%.
    {prefix}um burr --offset 4000\n           Set the offset difference of cmdr merits and galmap.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'um', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='UM')
    sub.add_argument('system', nargs='*', help='The system to update or show.')
    sub.add_argument('--set',
                     help='Set the status of the system, us:them. Example-> --set 3500:200')
    sub.add_argument('--offset', type=int, help='Set the system galmap offset.')


def subs_user(subs, prefix):
    """ Subcommand parsing for user """
    desc = """Manipulate your user settings. Examples:

    {prefix}user\n           Show your sheet name, crys and merits per sheet.
    {prefix}user --name Not Gears\n           Set your name to 'Not Gears'.
    {prefix}user --cry The bots are invading!\n           Set your battle cry to "The bots are invading!".
    {prefix}user --hudson\n           Switch to Hudson's sheets.
    {prefix}user --winters\n           Switch to Winters' sheets.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'user', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='User')
    sub.add_argument('--cry', nargs='+', help='Set your tag/cry in the sheets.')
    sub.add_argument('--name', nargs='+', help='Set your name in the sheets.')
    sub.add_argument('--winters', action='store_true',
                     help='Set yourself to use the Winters sheets.')
    sub.add_argument('--hudson', action='store_true',
                     help='Set yourself to use the Hudson sheets.')
