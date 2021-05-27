#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Any logic having to do with formatting an ASCII table.

String formatting reference:
  https://pyformat.info/#string_pad_align
"""
from cog.util import MSG_LIMIT
TABLE_LIMIT = MSG_LIMIT - 150  # Allow room for text around tables


def wrap_markdown(text):
    """
    Wraps text in multiline markdown quotes.
    """
    return '```' + text + '```'


def max_col_width(lines):
    """
    Iterate all lines and entries.

    Returns: A list of numbers, the max width required for each
             column given the data.
    """
    lens = [[] for _ in lines[0]]

    for line in lines:
        for ind, data in enumerate(line):
            lens[ind].append(len(data))

    return [max(len_list) for len_list in lens]


def format_header(line, sep=' | ', pads=None, center=True):
    """
    Format a simple table header and return as string.
    """
    header = format_line(line, sep=sep, pads=pads, center=center)
    divider = format_line(['-' * pad for pad in pads], sep=sep)
    return header + '\n' + divider + '\n'


def format_table(lines, *, sep=' | ', center=False, header=False, limit=TABLE_LIMIT,
                 prefix=None, suffix=None, wrap_msgs=True):
    """
    This function formats a table that fits all data evenly.
    It will go down columns and choose spacing that fits largest data.
    The table will automatically be wrapped in markdown to force code block.

    args:
        lines: Each top level element is a line composed of data in a list.
        sep: String to separate data with.
        center: Center the entry, otherwise left aligned.
        header: If true, format first line as pretty header.
        limit: A limit to split the table into parts
        prefix: Text before the first table.
        suffix: Text to follow the last table.

    Returns: A list of tables formatted in markdown for easy viewing, will be under limit.
    """
    # Guarantee all strings
    lines = [[str(data) for data in line] for line in lines]
    pads = max_col_width(lines)
    wrap_part = wrap_markdown if wrap_msgs else lambda x: x

    cur_part = ''
    header_str = ''
    if header:
        header_str = format_header(lines[0], sep=sep, pads=pads, center=True)
        lines = lines[1:]
        cur_part += header_str

    cur_len = len(cur_part)
    parts = []
    for line in lines:
        temp = format_line(line, sep=sep, pads=pads, center=center) + '\n'

        if cur_len + len(temp) > limit:
            parts += [wrap_part(cur_part)]
            cur_part = header_str
            cur_len = len(cur_part)

        cur_part += temp
        cur_len += len(temp)
    if cur_part:
        parts += [wrap_part(cur_part.rstrip())]

    if prefix and parts:
        parts[0] = prefix + parts[0]
    if suffix:
        parts[-1] = parts[-1] + suffix

    return parts


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
    align = '^' if center else ''

    if pads:
        pads = [align + str(pad) for pad in pads]
    else:
        pads = [align for ent in entries]

    ents = []
    for ind, ent in enumerate(entries):
        fmt = '{:%s}' % pads[ind]
        ents += [fmt.format(str(ent))]

    line = sep.join(ents)

    return line.rstrip()
