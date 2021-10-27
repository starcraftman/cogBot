#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module simply provides a means to extract all possible values from
the fields of the dicts in eddb data. It also spits out the max str len to
hold the field in question.
"""
import argparse
import pprint
import sys

import ijson


def parser():
    parser = argparse.ArgumentParser(description="JSON Analyzer")
    parser.add_argument('input', help='The input json to analyze.')
    parser.add_argument('log', nargs="?", help='The output log file to report to.')

    return parser


def examine_json(fname):
    len_tracker = {}
    tracker = {}

    with open(fname, 'rb') as fin:
        for prefix, the_type, value in ijson.parse(fin):
            if the_type in ("string", "number"):
                try:
                    if the_type == 'string' and len(value) > len_tracker[prefix]:
                        len_tracker[prefix] = len(value)
                except KeyError:
                    len_tracker[prefix] = len(value)
                try:
                    tracker[prefix].add(value)
                except KeyError:
                    tracker[prefix] = set([value])

    for key in tracker:
        tracker[key] = sorted(tracker[key])

    return tracker, len_tracker


def main():
    args = parser().parse_args()

    tracker, len_tracker = examine_json(sys.argv[1])
    print("## Summary of Lengths ##")
    for key in sorted(len_tracker.keys()):
        print(key, "=>", len_tracker[key])

    if args.log:
        with open(args.log, 'w') as fout:
            pprint.pprint(tracker, stream=fout, indent=2)


if __name__ == "__main__":
    main()
