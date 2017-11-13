#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Simple zmq publisher of fake data for testing.
"""
from __future__ import absolute_import, print_function
import asyncio

import aiozmq

import web.azmq


async def pub():
    """ Create publisher and begin sending. """
    publisher = await aiozmq.rpc.connect_pubsub(connect=web.azmq.ADDR)
    for step in range(20):
        await publisher.publish('POSTs').remote_func(step, 1, 2)
        await asyncio.sleep(2)

    print("Finished publishing messages.")
    publisher.close()
    await publisher.wait_closed()


def main():
    asyncio.get_event_loop().run_until_complete(pub())


if __name__ == "__main__":
    main()
