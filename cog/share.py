"""
Common functions.
"""
from __future__ import absolute_import, print_function
import logging
import logging.handlers
import logging.config
import os

import argparse
import tempfile
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import cogdb.query
import cog.sheets
import cog.tbl


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
YAML_FILE = os.path.join(ROOT_DIR, '.secrets', 'config.yaml')


class ArgumentParseError(Exception):
    """ Error raised instead of exiting on argparse error. """
    pass


class ThrowArggumentParser(argparse.ArgumentParser):
    def error(self, message=None):
        """
        Suppress default exit after error.
        """
        raise ArgumentParseError()

    def exit(self, status=0, message=None):
        """
        Suppress default exit behaviour.
        """
        raise ArgumentParseError()


def rel_to_abs(path):
    """
    Convert an internally relative path to an absolute one.
    """
    return os.path.join(ROOT_DIR, path)


def get_config(*keys):
    """
    Return keys straight from yaml config.
    """
    with open(YAML_FILE) as conf:
        conf = yaml.load(conf, Loader=Loader)

    for key in keys:
        conf = conf[key]

    return conf


def make_parser():
    """
    Returns the bot parser.
    """
    parser = ThrowArggumentParser(prog='cog', description='simple discord bot')

    subs = parser.add_subparsers(title='subcommands',
                                 description='The subcommands of cog')

    sub = subs.add_parser('fort', description='Show next fort target.')
    sub.add_argument('-l', '--long', action='store_true', default=False,
                     help='show detailed stats')
    sub.add_argument('-n', '--next', action='store_true', default=False,
                     help='show NUM systems after current')
    sub.add_argument('num', nargs='?', type=int, default=5,
                     help='number of systems to display')
    sub.set_defaults(func=parse_fort)

    sub = subs.add_parser('user', description='Manipulate sheet users.')
    sub.add_argument('-a', '--add', action='store_true', default=False,
                     help='Add a user to table if not present.')
    sub.add_argument('-q', '--query', action='store_true', default=False,
                     help='Return username and row if exists.')
    sub.add_argument('user', nargs='?',
                     help='The user to interact with.')
    sub.set_defaults(func=parse_user)

    sub = subs.add_parser('drop', description='Drop forts for user at system.')
    sub.add_argument('system', help='The system to drop at.')
    sub.add_argument('user', help='The user to drop for.')
    sub.add_argument('amount', type=int, help='The amount to drop.')
    sub.set_defaults(func=parse_drop)

    sub = subs.add_parser('help', description='Show overall help message.')
    sub.set_defaults(func=parse_help)
    return parser


def parse_help(_):
    """
    Simply prints overall help documentation.
    """
    lines = [
        ['Command', 'Effect'],
        ['!fort', 'Show current fort target.'],
        ['!fort -l', 'Show current fort target\'s status.'],
        ['!fort -n NUM', 'Show the next NUM targets. Default NUM = 5.'],
        ['!fort -nl NUM', 'Show status of the next NUM targets.'],
        ['!user -a USER', 'Add a USER to table.'],
        ['!user -q USER', 'Check if user is in table.'],
        ['!drop SYSTEM USER AMOUNT', 'Increase by AMOUNT forts for USER at SYSTEM'],
        ['!info', 'Display information on user.'],
        ['!help', 'This help message.'],
    ]
    return cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))


def parse_fort(args):
    table = cogdb.query.FortTable(cog.sheets.get_sheet())

    if args.next:
        systems = table.next_targets(args.num)
    else:
        systems = table.targets()

    if args.long:
        lines = [systems[0].__class__.header] + [system.data_tuple for system in systems]
        msg = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))
    else:
        msg = '\n'.join([system.name for system in systems])

    return msg


def parse_user(args):
    table = cogdb.query.FortTable(cog.sheets.get_sheet())
    user = table.find_user(args.user)

    if user:
        if args.query or args.add:
            msg = "User '{}' already present in row {}.".format(user.sheet_name,
                                                                user.sheet_row)
    else:
        if args.add:
            new_user = table.add_user(args.user)
            msg = "Added '{}' to row {}.".format(new_user.sheet_name,
                                                 new_user.sheet_row)
        else:
            msg = "User '{}' not found.".format(args.user)

    return msg


def parse_drop(args):
    table = cogdb.query.FortTable(cog.sheets.get_sheet())
    msg = table.add_fort(args.system, args.user, args.amount)
    try:
        lines = [msg.__class__.header, msg.data_tuple]
        return cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))
    except cog.exc.InvalidCommandArgs as exc:
        return str(exc)


def init_logging():
    """
    Initialize project wide logging. The setup is described best in config file.

    IMPORTANT: On every start the file logs are rolled over.
    """
    # TODO: Possibly, subclass Formatter, substring replace dict pathname to cut out absolute root
    log_folder = os.path.join(tempfile.gettempdir(), 'cog')
    try:
        os.path.exists(log_folder)
    except OSError:
        pass
    print('LOGGING FOLDER:', log_folder)

    with open(rel_to_abs('log.yaml')) as fin:
        log_conf = yaml.load(fin, Loader=Loader)
    logging.config.dictConfig(log_conf)

    for handler in logging.getLogger('cog').handlers + logging.getLogger('cogdb').handlers:
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            handler.doRollover()
