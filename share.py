"""
Common functions.
"""
from __future__ import absolute_import, print_function

import os
import argparse

import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import fort
import sheets
import tbl


THIS_DIR = os.path.dirname(os.path.abspath(__file__))
YAML_FILE = os.path.join(THIS_DIR, '.secrets', 'config.yaml')


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


def get_config(*keys):
    """
    Return keys straight from yaml config.
    """
    with open(YAML_FILE) as conf:
        conf = yaml.load(conf, Loader=Loader)

    for key in keys:
        conf = conf[key]

    return conf


def get_fort_table():
    """
    Return a fort table object.
    """
    sheet_id = get_config('hudson', 'cattle', 'id')
    secrets = get_config('secrets', 'sheets')
    sheet = sheets.GSheet(sheet_id, secrets['json'], secrets['token'])
    result = sheet.get('!F1:BM10', dim='COLUMNS')

    return fort.FortTable(result)


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
    sub.add_argument('num', nargs='?', type=int,
                     help='number of systems to display')
    sub.set_defaults(func=parse_fort)

    sub = subs.add_parser('help', description='Show overall help message.')
    sub.set_defaults(func=parse_help)
    return parser


def parse_help(_):
    """
    Simply prints overall help documentation.
    """
    lines = [
        'Available commands:',
        '!fort           Show current fort target.',
        '!fort -l        Show current fort target\'s status.',
        '!fort -n NUM    Show the next NUM targets. Default NUM = 5.',
        '!fort -nl NUM   Show status of the next NUM targets.',
        '!info           Display information on user.',
        '!help           This help message.',
    ]
    return '\n'.join(lines)


def parse_fort(args):
    table = get_fort_table()

    if args.next and args.long:
        msg = tbl.wrap_markdown(table.next_systems_long(args.num))
    elif args.next:
        msg = table.next_systems(args.num)
    elif args.long:
        msg = tbl.wrap_markdown(table.current_long())
    else:
        msg = table.current()

    return msg
