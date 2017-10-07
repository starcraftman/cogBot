"""
Example of what to put in bot, handle post and update in a separate process.
Can be used for testing. This is a simple zmq subscriber.
"""
from __future__ import absolute_import, print_function

import asyncio
import random

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print('Setting uvloop as asyncio default event loop.')
except ImportError:
    pass
import zmq
import zmq.asyncio
zmq.asyncio.install()


async def sub():
    ctx = zmq.asyncio.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.bind('tcp://127.0.0.1:9000')
    sock.subscribe(b'')

    while True:
        data = await sock.recv_json()
        print(data)

        # Handle request to parse sheet.


async def rand_print(ident):
    while True:
        print('Task {} woke up.'.format(ident))
        await asyncio.sleep(random.random() * 3)


def main():
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    loop.run_until_complete(asyncio.gather(sub(),
                                           rand_print(1), rand_print(2),
                                           rand_print(3), rand_print(4)))


if __name__ == '__main__':
    main()
