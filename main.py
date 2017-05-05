#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stub version of bot, doesn't connect to discord.
Instead parses CLI to test commands/outputs locally.
"""
import sys

import share


def main():
    try:
        parser = share.make_parser()
        while True:
            try:
                line = sys.stdin.readline().rstrip()
                args = parser.parse_args(line.split(' '))
                print(args.func(args))
            except share.ArgumentParseError:
                print('Please try again.')
    except KeyboardInterrupt:
        print('\nTerminating loop. Thanks for testing.')


if __name__ == "__main__":
    main()
