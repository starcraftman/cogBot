#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Stub version of bot, doesn't connect to discord.
Instead parses CLI to test commands/outputs locally.
"""
from __future__ import absolute_import, print_function
import logging
import sys

import mock
import cog.share


# Simple background coroutine example for later, adapt to refresh csv.
# async def my_task():
    # await client.wait_until_ready()
    # counter = 0
    # while not client.is_closed:
        # counter += 1
        # print('Counter', counter)
        # await asyncio.sleep(5)
# client.loop.create_task(my_task())


def main():
    """
    Simply operate the bot locally by command line input.
    """
    cog.share.init_logging()
    cog.share.init_db(cog.share.get_config('hudson', 'cattle', 'id'))
    logging.getLogger('cog.local').error('Local loop is ready.')

    try:
        parser = cog.share.make_parser()

        while True:
            try:
                line = sys.stdin.readline().rstrip()
                args = parser.parse_args(line.split(' '))
                response = args.func(msg=mock.Mock(), args=args)
                if response:
                    print(response.replace('```', ''))
            except cog.share.ArgumentParseError:
                print('Invalid command:', line)
    except KeyboardInterrupt:
        print('\nTerminating loop. Thanks for testing.')


if __name__ == "__main__":
    main()
