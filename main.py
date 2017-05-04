#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stub version of bot, doesn't connect to discord.
Instead parses CLI to test commands/outputs locally.
"""
import argparse
from argparse import RawDescriptionHelpFormatter as RawDescriptionHelp
import sys

import cog
import fort


def make_parser():
    """
    Returns a parser.
    """
    parser = argparse.ArgumentParser(prog='cog', description='simple discord bot',
                                     formatter_class=RawDescriptionHelp)

    subs = parser.add_subparsers(title='subcommands',
                                 description='The subcommands of cog')

    sub = subs.add_parser('fort', description='Show next fort target.')
    sub.add_argument('-l', '--long', action='store_true', default=False)
    sub.add_argument('-n', '--next', action='store_true', default=False)
    sub.add_argument('num', nargs='?', type=int)
    sub.set_defaults(func=parse_fort)

    sub = subs.add_parser('help', description='Show available commands.')
    sub.set_defaults(func=parse_help)

    return parser


def parse_help(args):
    print('Placeholder help message.')


def parse_fort(args):
    table = cog.get_fort_table()

    if args.next and args.long:
        msg = table.next_objectives_status(args.num)
    elif args.next:
        msg = table.next_objectives(args.num)
    elif args.long:
        msg = table.objectives()
    else:
        msg = table.current()

    print(msg)


def main():
    try:
        parser = make_parser()
        while True:
            line = sys.stdin.readline().rstrip()
            args = parser.parse_args(line.split(' '))
            args.func(args)
    except KeyboardInterrupt:
        print('\nTerminating loop. Thanks for testing.')


if __name__ == "__main__":
    main()
