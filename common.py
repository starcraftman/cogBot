#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Common or catch all functions for the bot.
"""


def wrap_markdown(text):
    """
    Wraps text in multiline markdown quotes.
    """
    return '```' + text + '```'


def format_table(lines, sep=' | ', center=False):
    """
    This function formats a table that fits all data evenly.
    It will go down columns and choose spacing that fits largest data.

    args:
        lines: Each top level element is a line composed of data in a list.
        sep: String to separate data with.
        center: Center the entry, otherwise left aligned.
    """
    # Guarantee all strings
    lines = [[str(data) for data in line] for line in lines]
    pads = [0 for ent in lines[0]]

    for line in lines:
        for ind, data in enumerate(line):
            len_d = len(data)
            if len_d > pads[ind]:
                pads[ind] = len_d

    ret_line = ''
    for line in lines:
        ret_line += format_line(line, sep=sep, pads=pads, center=center) + '\n'

    return ret_line[:-1]


# Formatting ref: https://pyformat.info/#string_pad_align
def format_line(entries, sep=' | ', pads=None, center=False):
    """
    Format data for use in a simple table output to text.

    args:
        entries: List of data to put in table, left to right.
        sep: String to separate data with.
        pad: List of numbers, pad each entry as you go with this number.
        center: Center the entry, otherwise left aligned.
    """
    line = ''

    if pads is None:
        pads = ['' for ent in entries]
    else:
        pads = [str(pad) for pad in pads]

    if center:
        pads = ['^' + pad for pad in pads]

    ents = []
    for ind, ent in enumerate(entries):
        fmt = '{:%s}' % pads[ind]
        ents += [fmt.format(str(ent))]

    line = sep.join(ents)

    return line.rstrip()
