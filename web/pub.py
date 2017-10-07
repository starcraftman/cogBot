#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Simple zmq publisher of fake data for testing.
"""
from __future__ import absolute_import, print_function
import sys
import time

import zmq


def sub(ident):
    print("Sub connecting on: 127.0.0.1:9000")

    try:
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect("tcp://127.0.0.1:9000")
        socket.setsockopt_string(zmq.SUBSCRIBE, 'sheet')

        while True:
            print(ident, socket.recv_string())
    finally:
        socket.close()


def main():
    """ Just for testing server. """
    print("Pub bind on: 127.0.0.1:9000")

    try:
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.connect("tcp://127.0.0.1:9000")

        while True:
            print('pub')
            socket.send_json({'Hello': 'world'})
            time.sleep(3)
    finally:
        socket.close()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    else:
        sub(sys.argv[1])
