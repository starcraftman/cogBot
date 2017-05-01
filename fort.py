#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

from functools import partial
try:
    from urllib.request import urlopen
except ImportError:
    from urllib import urlopen

import common

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
        return len(self.line) != 0

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
        self.skip = 'Leave' in data[4]

    def __str__(self):
        """ Format for output """
        return common.line_format(self.data_tuple)

    @property
    def data_tuple(self):
        if self.is_fortified:
            fort_status = ':fortified:'
        else:
            fort_status = '{:>4}/{:4} ({:2}%)'.format(self.fort_status,
                                                       self.fort_trigger,
                                                       self.completion)

        if self.skip:
            missing = 'Please do not fortify!'
        else:
            missing = 'missing: {:>4}'.format(self.missing)

        return [self.name, fort_status, missing, 'um: {:2}%'.format(self.um_percent)]

    @property
    def is_fortified(self):
        """ The remaining supplies to fortify """
        return self.fort_status >= self.fort_trigger

    @property
    def is_undermined(self):
        """ The system has been undermined. """
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
        S/M ships fort Othime until done, then direct to L target.
        L target starts at first system and slides down.
        """
        othime_found = False
        target_found = False

        # Seek L target
        for (i, system) in enumerate(self.systems):
            system = FortSystem(system, self.data[i])

            if system.name == 'Othime':
                othime_found = True

                omsg = ['S/M Ships']
                if system.is_fortified:
                    omsg += ['Othime fortified! Fort target below!']
                else:
                    omsg += system.data_tuple

            if not target_found and system.name != 'Othime' and \
                    not system.is_fortified and not system.skip:
                target_found = True
                lmsg = ['L ships'] + system.data_tuple

            if othime_found and target_found:
                break

        return common.table_format([omsg, lmsg])

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

        return common.table_format(targets[1:])

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

        return (fortified, undermined, len(self.systems))

def tokenize(line, start=None, end=None):
    tokens = []
    tok = CSVLineTokenizer(line)

    while tok.has_more_tokens():
        tokens.append(tok.next_token())

    if start and end:
        tokens = tokens[start:end]
    elif start and end is None:
        tokens = tokens[start:]

    return tokens

def parse_int(num_str):
    """
    Parse an integer that was stored in a csv.
    Specifically change strings like 3,459 or 7% into ints.
    """
    num = num_str

    if len(num) > 0:
        for char in [',', '%']:
            while char in num:
                num = num.replace(char, '')
    else:
        num = 0

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
