"""
Common functions.
"""
from __future__ import absolute_import, print_function
import os
import sys

import argparse
import logging
import logging.handlers
import tempfile
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import cogdb
import cog.fort
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


def get_sheet():
    """
    Temporary hack, get a sheet.
    """
    sheet_id = cog.share.get_config('hudson', 'cattle', 'id')
    secrets = cog.share.get_config('secrets', 'sheets')
    return cog.sheets.GSheet(sheet_id, cog.share.rel_to_abs(secrets['json']),
                             cog.share.rel_to_abs(secrets['token']))


def init_db():
    """
    Scan sheet and fill database if empty.
    """
    session = cogdb.Session()

    if not session.query(cogdb.System).all():
        scanner = cog.fort.SheetScanner(get_sheet(), 11, 'F')
        systems = scanner.systems()
        users = scanner.users()
        session.add_all(systems + users)
        session.commit()

        forts = scanner.forts(systems, users)
        session.add_all(forts)
        session.commit()


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
    table = cog.fort.FortTable(cogdb.Session(), get_sheet())

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
    table = cog.fort.FortTable(cogdb.Session(), get_sheet())
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
    table = cog.fort.FortTable(cogdb.Session(), get_sheet())
    msg = table.add_fort(args.system, args.user, args.amount)
    if isinstance(msg, type('')):
        return msg
    else:
        lines = [msg.__class__.header, msg.data_tuple]
        return cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))


def init_logging():
    """
    Initialize project wide logging.
      - 'discord' logger is used by the discord.py framework.
      - 'gbot' logger will be used to log anything in this project.

    Both loggers will:
      - Send all messsages >= WARN to STDERR.
      - Send all messages >= INFO to rotating file log in /tmp.

    IMPORTANT: On every start the logs are rolled over. 5 runs kept max.
    """
    log_folder = os.path.join(tempfile.gettempdir(), 'gbot')
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    discord_file = os.path.join(log_folder, 'discordpy.log')
    gbot_file = os.path.join(log_folder, 'gbot.log')
    print('discord.py log ' + discord_file)
    print('gbot log: ' + gbot_file)
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(msg)s')

    d_logger = logging.getLogger('discord')
    d_logger.setLevel(logging.INFO)
    handler1 = logging.handlers.RotatingFileHandler(discord_file,
                                                    backupCount=5, encoding='utf-8')
    handler1.setLevel(logging.DEBUG)
    handler1.setFormatter(fmt)
    handler1.doRollover()
    d_logger.addHandler(handler1)

    g_logger = logging.getLogger('gbot')
    g_logger.setLevel(logging.INFO)
    handler2 = logging.handlers.RotatingFileHandler(gbot_file,
                                                    backupCount=5, encoding='utf-8')
    handler2.setLevel(logging.DEBUG)
    handler2.setFormatter(fmt)
    handler2.doRollover()
    g_logger.addHandler(handler2)

    handler3 = logging.StreamHandler(sys.stderr)
    handler3.setLevel(logging.WARNING)
    handler3.setFormatter(fmt)
    d_logger.addHandler(handler3)
    g_logger.addHandler(handler3)