"""
Prototype of uvloop + aiozmq publish-subscribe connection.
"""
from __future__ import absolute_import, print_function
import asyncio
import atexit
import random
import time

import aiozmq
import aiozmq.rpc
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    print('Setting uvloop as asyncio default event loop.')
except ImportError:
    pass

ADDR = 'tcp://127.0.0.1:9000'


class PubHandler(aiozmq.rpc.AttrHandler):
    """
    The RPC glue that binds the pub and sub.
    """
    def __init__(self):
        self.con = None
        self.connected = False

    @aiozmq.rpc.method
    def remote_func(self, step, arg1: int, arg2: int):
        """ Remote function to be executed. """
        self.connected = True
        print("HANDLER", step, arg1, arg2)

    def close(self):
        """ Properly close pubsub connection on termination. """
        if self.con and self.connected:
            self.con.close()
            time.sleep(1)


async def pub():
    """ Create publisher and begin sending. """
    publisher = await aiozmq.rpc.connect_pubsub(connect=ADDR)
    for step in range(20):
        await publisher.publish('posts').remote_func(step, 1, 2)
        await asyncio.sleep(0.5)

    print("Finished publishing messages.")
    publisher.close()
    await publisher.wait_closed()


async def sub():
    """ Register the subscriber. """
    hand = PubHandler()
    hand.con = await aiozmq.rpc.serve_pubsub(
        hand, subscribe='POSTs', bind=ADDR, log_exceptions=True)
    atexit.register(hand.close)
    print("SERVE", ADDR)


async def rand_print(ident):
    """ Simple random print to ensure loop working. """
    while True:
        print('Task {} woke up.'.format(ident))
        await asyncio.sleep(random.random() * 3)


def main():
    """ The main loop. """
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    loop.run_until_complete(asyncio.gather(sub(),
                                           rand_print(1), rand_print(2),
                                           rand_print(3), rand_print(4)))


if __name__ == '__main__':
    main()
