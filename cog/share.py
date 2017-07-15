"""
Common functions.
"""
from __future__ import absolute_import, print_function
import logging
import logging.handlers
import logging.config
import os

import argparse
from argparse import RawDescriptionHelpFormatter as RawHelp
import tempfile
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import cog.exc
import cog.sheets
import cog.tbl


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
YAML_FILE = os.path.join(ROOT_DIR, 'data', 'config.yml')


class ThrowArggumentParser(argparse.ArgumentParser):
    """
    ArgumentParser subclass that does NOT terminate the program.
    """
    def print_help(self, file=None):  # pylint: disable=redefined-builtin
        raise cog.exc.ArgumentHelpError(self.format_help())

    def error(self, message):
        raise cog.exc.ArgumentParseError(message, self.format_usage())

    def exit(self, status=0, message=None):
        """
        Suppress default exit behaviour.
        """
        raise cog.exc.ArgumentParseError(message, self.format_usage())


class ModFormatter(logging.Formatter):
    """
    Add a relmod key to record dict.
    This key tracks a module relative this project' root.
    """
    def format(self, record):
        relmod = record.__dict__['pathname'].replace(ROOT_DIR + os.path.sep, '')
        record.__dict__['relmod'] = relmod[:-3]
        return super(ModFormatter, self).format(record)


def rel_to_abs(*path_parts):
    """
    Convert an internally relative path to an absolute one.
    """
    return os.path.join(ROOT_DIR, *path_parts)


def get_config(*keys):
    """
    Return keys straight from yaml config.
    """
    with open(YAML_FILE) as conf:
        conf = yaml.load(conf, Loader=Loader)

    for key in keys:
        conf = conf[key]

    return conf


def init_logging():
    """
    Initialize project wide logging. The setup is described best in config file.

     - On every start the file logs are rolled over.
     - This should be first invocation on startup to set up logging.
    """
    log_folder = os.path.join(tempfile.gettempdir(), 'cog')
    try:
        os.makedirs(log_folder)
    except OSError:
        pass
    print('LOGGING FOLDER:', log_folder)

    with open(rel_to_abs(get_config('paths', 'log_conf'))) as fin:
        log_conf = yaml.load(fin, Loader=Loader)
    logging.config.dictConfig(log_conf)

    for handler in logging.getLogger('cog').handlers + logging.getLogger('cogdb').handlers:
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            handler.doRollover()


def make_parser(prefix):
    """
    Returns the bot parser.
    """
    parser = ThrowArggumentParser(prog='', description='simple discord bot')

    subs = parser.add_subparsers(title='subcommands',
                                 description='The subcommands of cog')

    desc = """Update the cattle sheet with your drops at a system. Examples:

    {prefix}drop 600\n          - Drop 600 supplies for yourself at the main fortification target.
    {prefix}drop 600 @Shepron\n          - Drop 600 supplies for Shepron at the main fortification target.
    {prefix}drop 600 othime\n           - Drop 600 supplies for yourself at current main fortification target.
    {prefix}drop 600 othime @rjwhite\n           - Drop 600 supplies for rjwhite at current main fortification target.
    {prefix}drop --set 4560:2000 600 Othime\n           - Drop 600 supplies at Othime for yourself, set Othime fort status to 4500 and UM status to 2000.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'drop', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='drop')
    sub.add_argument('amount', type=int, help='The amount to drop.')
    sub.add_argument('system', nargs='*',
                     help='The system to drop at.')
    sub.add_argument('--set',
                     help='Set the fort:um status of the system. Example-> --set 3400:200')

    sub = subs.add_parser(prefix + 'dump', description='Admin only. Dump db.')
    sub.set_defaults(cmd='dump')

    desc = """Show fortification status and targets. Examples:

    {prefix}fort\n            - Show current Large fort target and Othime if not finished.
    {prefix}fort --next\n           - Show the next fortification target (excludes Othime and skipped).
    {prefix}fort --nextn 3\n           - Show the next 3 fortification targets (excludes Othime and skipped).
    {prefix}fort --summary\n            - Show a breakdown by states of our systems.
    {prefix}fort Othime --set 7500:2000\n           - Set othime to 7500 fort status and 200 um status.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'fort', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='fort')
    sub.add_argument('system', nargs='*',
                     help='Select this system that matches.')
    sub.add_argument('--set',
                     help='Set the fort:um status of system. Example-> --set 3400:200')
    sub.add_argument('--summary', action='store_true',
                     help='Provide an overview of the fort systems.')
    sub.add_argument('-l', '--long', action='store_true',
                     help='Show detailed stats')
    sub.add_argument('--nextn', type=int,
                     help='Show the next NUM systems after current')
    sub.add_argument('-n', '--next', action='store_true',
                     help='Show the next fort target')

    desc = """Update a user's held or redeemed merits. Examples:

    {prefix}hold 1200 burr\n            - Set my held merits at System Burr to 1200 held.
    {prefix}hold --died\n           - Reset held merits to 0 due to dying.
    {prefix}hold --redeem\n         -Move all held merits to redeemed column.
    {prefix}hold --set 60000:130 burr\n         -Update Burr expansion to 60000 merits and 130% opposition.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'hold', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='hold')
    sub.add_argument('amount', nargs='?', type=int, help='The amount of merits held.')
    sub.add_argument('system', nargs='*', help='The system being undermined.')
    sub.add_argument('--redeem', action='store_true', help='Redeem all held merits.')
    sub.add_argument('--died', action='store_true', help='Zero out held merits on death.')
    sub.add_argument('--set', help='Update the galmap progress us:them.')

    sub = subs.add_parser(prefix + 'info', description='Get information on things.')
    sub.set_defaults(cmd='info')
    sub.add_argument('user', nargs='?',
                     help='Display information about user.')

    sub = subs.add_parser(prefix + 'scan', description='Parse the sheets for new information.')
    sub.set_defaults(cmd='scan')

    sub = subs.add_parser(prefix + 'time', description='Time in game and to ticks.')
    sub.set_defaults(cmd='time')

    desc = """Get undermining targets and update their galmap status. Examples:

    {prefix}um\n            - Show current active undermining targets.
    {prefix}um burr\n           - Show the current status and information on Burr.
    {prefix}um --set 60000:130\n            - Set the galmap status of Burr to 60000 and opposition to 130%.
    {prefix}um --offset 4000\n          - Set the offset difference of cmdr merits and galmap.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'um', description='', formatter_class=RawHelp)
    sub.set_defaults(cmd='um')
    # sub.add_argument('system', nargs='?', help='The system to update/show.')
    sub.add_argument('system', nargs='*',
                     help='The system to update or show.')
    sub.add_argument('--set',
                     help='Set the System status of system, us:them. Example-> --set 3400:200')
    sub.add_argument('--offset', type=int,
                     help='Set the System galmap offset.')

    desc = """Manipulate your user settings. Examples:

    {prefix}user\n          - Show your sheet name, crys and merits per sheet.
    {prefix}um --name NotGears\n            - Set your name to NotGears.
    {prefix}um --cry The bots are invading!\n           - Set your battle cry to "The bots are invading!".
    {prefix}um --hudson\n           - Switch to Hudson's sheets.
    {prefix}um --winters\n          - Switch to Winters' sheets.
    """.format(prefix=prefix)
    sub = subs.add_parser(prefix + 'user', description=desc, formatter_class=RawHelp)
    sub.set_defaults(cmd='user')
    sub.add_argument('--cry', nargs='+',
                     help='Set your tag in the sheets.')
    sub.add_argument('--name', nargs='+',
                     help='Set your name in the sheets.')
    sub.add_argument('--winters', action='store_true',
                     help='Set yourself to use the Winters sheets.')
    sub.add_argument('--hudson', action='store_true',
                     help='Set yourself to use the Hudson sheets.')

    sub = subs.add_parser(prefix + 'help', description='Show overall help message.')
    sub.set_defaults(cmd='help')
    return parser


def dict_to_columns(data):
    """
    Transform the dict into columnar form with keys as column headers.
    """
    lines = []
    header = []

    for col, key in enumerate(sorted(data)):
        header.append('{} ({})'.format(key, len(data[key])))

        for row, item in enumerate(data[key]):
            try:
                lines[row]
            except IndexError:
                lines.append([])
            while len(lines[row]) != col:
                lines[row].append('')
            lines[row].append(item)

    return [header] + lines
