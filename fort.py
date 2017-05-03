#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module that handles:
    - Parsing the csv
    - Analysing data in the csv
"""
from __future__ import absolute_import, print_function

from functools import partial
try:
    from urllib.request import urlopen
except ImportError:
    from urllib import urlopen

import common

TABLE_HEADER = ['System', 'Trigger', 'Missing', 'UM', 'Notes']


class NoMoreTokens(Exception):
    """
    Exception thrown when tokenizer has run out.
    """
    pass

class CSVLineTokenizer(object):
    """
    Tokenize a csv line into individual tokens one at a time.
    """
    def __init__(self, line):
        self.line = line.strip()

    def has_more_tokens(self):
        """
        Are there more tokens?
        """
        return bool(self.line)

    def next_token(self):
        """
        Parse the line for next token. Modifies the line.

        Return:
            The parsed token.
        """
        token = ''

        if not self.has_more_tokens():
            raise NoMoreTokens('Out of tokens!')

        # If has quote, data goes until next quote, comma may be inside quotes
        if self.line[0] == '"':
            end = self.line.find('"', 1) + 1
            token = self.line[1:end-1]
            self.line = self.line[end+1:]
        elif self.line.find(',') != -1:
            end = self.line.find(',')
            token = self.line[0:end]
            self.line = self.line[end+1:]
        else:
            token = self.line
            self.line = ''

        return token

class FortSystem(object):
    """
    Simple little object that transforms data for output or use.

    args:
        system: Name of system.
        data: List to be unpacked: ump, trigger, cmdr_merits, status, notes):

        Note: ump is a string like: 17%
        Note: trigger, cmdr_merits & status are strings like: 3,444
    """
    def __init__(self, system, data):
        self.name = system
        self.um_percent = parse_int(data[0][:-1])
        self.fort_trigger = parse_int(data[1])
        cmdr_merits = parse_int(data[2])
        status = parse_int(data[3])
        self.fort_status = max(cmdr_merits, status)
        self.notes = data[4]

    def __str__(self):
        """ Format for output """
        return common.format_line(self.data_tuple)

    @property
    def data_tuple(self):
        """ Return a list of important data for tables """
        if self.is_fortified:
            fort_status = ':fortified:'
        else:
            fort_status = '{:>4}/{:4} ({:2}%)'.format(self.fort_status,
                                                      self.fort_trigger,
                                                      self.completion)

        if self.skip:
            missing = 'Please do not fortify!'
        else:
            missing = '{:>4}'.format(self.missing)

        data = [self.name, fort_status, missing, '{:2}%'.format(self.um_percent)]
        if self.notes:
            data += [self.notes]

        return data

    @property
    def skip(self):
        return 'Leave' in self.notes

    @property
    def is_fortified(self):
        """ The remaining supplies to fortify """
        return self.fort_status >= self.fort_trigger

    @property
    def is_undermined(self):
        """ The system has been undermined """
        return self.um_percent >= 99

    @property
    def missing(self):
        """ The remaining supplies to fortify """
        return max(0, self.fort_trigger - self.fort_status)

    @property
    def completion(self):
        """ The fort completion percentage """
        try:
            comp_cent = self.fort_status / self.fort_trigger * 100
        except ZeroDivisionError:
            comp_cent = 0

        return '{:.1f}'.format(comp_cent)

class FortTable(object):
    """
    Represents the fort sheet
        -> Goal: Minimize unecessary operations to data on server.
    """
    def __init__(self, systems, data):
        """
        Parse data from table and initialize.
        """
        self.systems = systems
        self.data = data

    def objectives(self):
        """
        Print out the current objectives to fortify and their status.
        """
        othime = None
        target = None

        # Seek targets in systems list
        for (i, system) in enumerate(self.systems):
            system = FortSystem(system, self.data[i])

            if system.name == 'Othime':
                othime = system

            if not target and system.name != 'Othime' and \
                    not system.is_fortified and not system.skip:
                target = system

            if othime and target:
                break

        lines = [TABLE_HEADER, target.data_tuple]
        if not othime.is_fortified:
            lines += [othime.data_tuple]
        return common.wrap_markdown(common.format_table(lines, sep='|', header=True))

    def next_objectives(self, num=5):
        """
        Return next 5 regular fort targets.
        """
        targets = []

        for (i, system) in enumerate(self.systems):
            system = FortSystem(system, self.data[i])

            if system.name != 'Othime' and not system.is_fortified and not system.skip:
                targets.append(system.name)

            if len(targets) == num + 1:
                break

        return '\n'.join(targets[1:])

    def next_objectives_status(self, num=5):
        """
        Return next 5 regular fort targets.
        """
        targets = []

        for (i, system) in enumerate(self.systems):
            system = FortSystem(system, self.data[i])

            if system.name != 'Othime' and not system.is_fortified and not system.skip:
                targets.append(system.data_tuple)

            if len(targets) == num + 1:
                break

        return common.wrap_markdown(common.format_table([TABLE_HEADER] + targets[1:], sep='|',
                                                        header=True))

    def totals(self):
        """
        Print running total of fortified, undermined systems.
        """
        undermined = 0
        fortified = 0

        for (i, system) in enumerate(self.systems):
            system = FortSystem(system, self.data[i])
            if system.is_fortified:
                fortified += 1
            if system.is_undermined:
                undermined += 1

        return 'Fortified {}/{tot}, Undermined: {}/{tot}'.format(fortified, undermined,
                                                                 tot=len(self.systems))

def tokenize(line, start=None, end=None):
    """
    Apply tokenize to a line and return the tokens.

    args:
        line: A line of csv format
        start: First useful column from csv
        end: Last useful column of csv
    """
    tokens = []
    tok = CSVLineTokenizer(line)

    while tok.has_more_tokens():
        tokens.append(tok.next_token())

    if start and end:
        tokens = tokens[start:end]
    elif start and end is None:
        tokens = tokens[start:]

    return tokens

def parse_int(num):
    """
    Parse an integer that was stored in a csv.
    Specifically change strings like 3,459 or 7% into ints.
    Empty strings treated as 0.
    """
    if not num:
        return 0

    for char in [',', '%']:
        while char in num:
            num = num.replace(char, '')

    return int(num)

def usable_range(systems):
    """
    Determines the usable columns of the csv
    """
    start = 0
    end = len(systems) - 1

    while systems[start] != 'Frey':
        start = start + 1

    while systems[end] == '':
        end = end - 1

    return (start, end)

def parse_csv(lines):
    """
    This function tokenizes all the useful lines in the csv for later use.
    Some columns at the beginning and end must be ignored to focus on useful data.

    Useful rows of the csv:
        0 -> um %
        2 -> fort trigger
        4 -> cmdr merits
        5 -> fort status
        8 -> notes (i.e. Leave for grinders)
        9 -> system name
    """
    systems = tokenize(lines[9])
    start, end = usable_range(systems)
    systems = systems[start:end]

    make_list = partial(tokenize, start=start, end=end)
    um_status = make_list(lines[0])
    fort_triggers = make_list(lines[2])
    cmdr_merits = make_list(lines[4])
    fort_status = make_list(lines[5])
    notes = make_list(lines[8])

    data = list(zip(um_status, fort_triggers, cmdr_merits, fort_status, notes))

    return (systems, data)

def main():
    """
    Main function, tests with local csv file.
    """
    with open('csv.private') as fin:
        lines = fin.readlines()

    lines = [line.strip() for line in lines]
    systems, data = parse_csv(lines)

    table = FortTable(systems, data)
    print(table.objectives())
    print(table.next_objectives())
    print('\n'.join(table.next_objectives(True)))

if __name__ == "__main__":
    main()
