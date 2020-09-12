#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A simple logger to look at eddn messages passing.

Terminate with a simple Ctlr+C
"""
import os
import sys
import time
import zlib

import argparse
import zmq
try:
    import rapidjson as json
except ImportError:
    import json


EDDN_ADDR = "tcp://eddn.edcd.io:9500"
TIMEOUT = 600000
SCHEMA_MAP = {
    "https://eddn.edcd.io/schemas/commodity/3": "commodity",
    "https://eddn.edcd.io/schemas/journal/1": "journal",
    "https://eddn.edcd.io/schemas/outfitting/2": "outfitting",
    "https://eddn.edcd.io/schemas/shipyard/2": "shipyard",
    None: "unknown",
}


def get_msgs(sub, args):
    """ Continuously receive messages and log them. """
    while True:
        msg = sub.recv()

        if not msg:
            raise zmq.ZMQError("Sub problem.")

        msg = json.loads(zlib.decompress(msg).decode())
        msg_str = json.dumps(msg, indent=2, sort_keys=True)

        if args.print:
            print(msg_str)
        try:
            fname = SCHEMA_MAP[msg["$schemaRef"]]
        except KeyError:
            fname = SCHEMA_MAP[None]

        with open(os.path.join(args.log_d, fname) + '.json', 'a') as fout:
            fout.write(msg_str + ',')
        with open(os.path.join(args.log_d, fname + '.jsonl'), 'a') as fout:
            fout.write(json.dumps(msg, indent=None, sort_keys=True) + ",\n")


def connect_loop(sub, args):
    """
    Continuously connect and get messages until user cancels.
    All messages logged to file and printed.
    """
    while True:
        try:
            sub.connect(EDDN_ADDR)
            get_msgs(sub, args)
        except zmq.ZMQError as exc:
            print("ZMQ Socket error. Reconnecting soon.\n", exc)
            sub.discconect(EDDN_ADDR)
            time.sleep(5)

def parser():
    parser = argparse.ArgumentParser(description="EDDN Logger")
    parser.add_argument('log_d', help='The folder to log files to.')
    parser.add_argument('--no-print', dest='print', default=True,
                        action='store_false', help='Do not print to stdout')

    return parser

def main():
    args = parser().parse_args()
    sub = zmq.Context().socket(zmq.SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b'')
    sub.setsockopt(zmq.RCVTIMEO, TIMEOUT)

    try:
        for key in SCHEMA_MAP:
            with open(os.path.join(args.log_d, SCHEMA_MAP[key]) + '.json', 'w') as fout:
                fout.write('[\n')
            with open(os.path.join(args.log_d, SCHEMA_MAP[key]) + '.jsonl', 'w') as fout:
                fout.write('[\n')

        connect_loop(sub, args)
    except KeyboardInterrupt:
        for key in SCHEMA_MAP:
            with open(os.path.join(args.log_d, SCHEMA_MAP[key]) + '.json', 'a') as fout:
                fout.write(']')
            with open(os.path.join(args.log_d, SCHEMA_MAP[key]) + '.jsonl', 'a') as fout:
                fout.write(']')

        msg = """Terminating ZMQ connection.

    {fname} contains all messages sorted into files by schema.
    Files ending in .json contains all messages compact one per line.
    Files ending in .jsonl contains all messages pretty printed."""
        print(msg.format(fname=sys.argv[1]))


if __name__ == "__main__":
    main()
