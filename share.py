"""
Common functions.
"""
import argparse
try:
    from urllib.request import urlopen
except ImportError:
    from urllib import urlopen

import yaml

import fort
import tbl


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


def get_config(key):
    """
    Return keys straight from yaml config.
    """
    with open('yaml.private') as conf:
        return yaml.load(conf)[key]


def get_fort_table():
    """
    Return a fort table object.
    """
    with urlopen(get_config('url_cattle')) as fin:
        lines = str(fin.read()).split(r'\r\n')

    lines = [line.strip() for line in lines]
    systems, data = fort.parse_csv(lines)

    return fort.FortTable(systems, data)


def make_parser():
    """
    Returns a parser.
    """
    parser = ThrowArggumentParser(prog='', description='simple discord bot')

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

    sub = subs.add_parser('help', description='Show available commands.')
    sub.set_defaults(func=parse_help)

    return parser


def parse_help(args):
    print('Placeholder help message.')


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
