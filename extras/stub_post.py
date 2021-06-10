"""
This is a stub of web/app.py it exists to send a dummy command as programmed for testing.
"""
import asyncio
import atexit
import datetime
import functools
import sys
import time

import aiozmq
import aiozmq.rpc
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    print("Falling back to default python loop.")

import cog.util

ADDR = 'tcp://127.0.0.1:{}'.format(cog.util.get_config('ports', 'zmq'))


def pub_close(pub):
    """ Simple atexit hook. """
    pub.close()
    time.sleep(0.5)


async def send_post(scanner):
    """
    Simple stub to fake POSTs into the scheduler for testing purposes.
    """
    pub = await aiozmq.rpc.connect_pubsub(bind=ADDR)
    atexit.register(functools.partial(pub_close, pub))

    while input():
        await pub.publish('POSTs').remote_func(scanner, datetime.datetime.utcnow())


def main():
    """ Just run the tests on the loop. """
    scanner = 'hudson_cattle'
    if len(sys.argv) == 2:
        scanner = sys.argv[1]

    asyncio.new_event_loop().run_until_complete(send_post(scanner))


if __name__ == "__main__":
    main()
