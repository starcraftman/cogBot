#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A simple logger to look at eddn messages passing.

Terminate with a simple Ctlr+C
"""
from __future__ import absolute_import, print_function
import pprint
import sys
import time
import zlib

import zmq
try:
    import simplejson as json
except ImportError:
    import json


EDDN_ADDR = "tcp://eddn.edcd.io:9500"
TIMEOUT = 600000


def get_msgs(sub, fout, foutl):
    """ Continuously receive messages and log them. """
    while True:
        msg = sub.recv()

        if not msg:
            raise zmq.ZMQError("Sub problem.")

        msg = json.loads(zlib.decompress(msg).decode())
        msg_str = json.dumps(msg, indent=2, sort_keys=True)
        pprint.pprint(msg_str)
        fout.write(msg_str)
        foutl.write(json.dumps(msg, separators=(',', ':')) + "\n")


def connect_loop(sub, fout, foutl):
    """
    Continuously connect and get messages until user cancels.
    All messages logged to file and printed.
    """
    while True:
        try:
            sub.connect(EDDN_ADDR)
            get_msgs(sub, fout, foutl)
        except zmq.ZMQError as exc:
            print("ZMQ Socket error. Reconnecting soon.\n", exc)
            sub.discconect(EDDN_ADDR)
            time.sleep(5)


def main():
    if len(sys.argv) != 2:
        print("{} path/to/log".format(sys.argv[0]))
        sys.exit(1)

    sub = zmq.Context().socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b'')
    sub.setsockopt(zmq.RCVTIMEO, TIMEOUT)

    try:
        with open(sys.argv[1], 'a') as fout, open(sys.argv[1] + '_line', 'a') as foutl:
            connect_loop(sub, fout, foutl)
    except KeyboardInterrupt:
        msg = """Terminating ZMQ connection.
    {fname} contains all messages pretty printed.
    {fname}_line contains all messages compact one per line."""
        print(msg.format(fname=sys.argv[1]))


if __name__ == "__main__":
    main()
