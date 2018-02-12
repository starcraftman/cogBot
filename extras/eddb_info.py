#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module simply provides a means to extract all possible values from
the fields of the dicts in eddb data. It also spits out the max str len to
hold the field in question.
"""
from __future__ import absolute_import, print_function
import pprint
import sys

try:
    import simplejson as json
except ImportError:
    import json


def walk_list(data_list, *keys):
    """ Just walk the list. """
    for val in data_list:
        if isinstance(val, type({})):
            gen = walk_dict(val, *keys)
            try:
                while True:
                    yield next(gen)
            except StopIteration:
                pass
        else:
            yield [keys, val]


def walk_dict(data, *keys):
    """ Walk the dictionary. """
    keys = list(keys)

    for key in data:
        if isinstance(data[key], type({})) or isinstance(data[key], type([])):
            try:
                gfunc = walk_dict if isinstance(data[key], type({})) else walk_list
                gen = gfunc(data[key], *keys, key)
                while True:
                    yield next(gen)
            except StopIteration:
                pass
        else:
            yield [keys + [key], data[key]]


def parse_json(fname):
    with open(fname) as fin:
        all_data = json.load(fin)

    tracker = {}
    for pair in walk_list(all_data):
        new_key = '//'.join(pair[0])
        try:
            tracker[new_key].add(pair[1])
        except KeyError:
            tracker[new_key] = set([pair[1]])

    return tracker


def main():
    if len(sys.argv) < 2:
        print("{} path/to/file.jsonl [path/to/write/vals]".format(sys.argv[0]))
        sys.exit(1)

    summary = parse_json(sys.argv[1])
    for key in sorted(summary.keys()):
        max_len = 0
        for value in summary[key]:
            field_len = len(str(value))
            if field_len > max_len:
                max_len = field_len
        print(key, "=>", max_len)

    try:
        with open(sys.argv[2], 'w') as fout:
            fout.write(pprint.pformat(summary))
    except IndexError:
        pass


if __name__ == "__main__":
    main()
