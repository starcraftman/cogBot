#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module that handles:
    - Parsing the csv
    - Analysing data in the csv
"""
from __future__ import absolute_import, print_function

import share
import sheets
import tbl

TABLE_HEADER = ['System', 'Trigger', 'Missing', 'UM', 'Notes']


# TODO: Don't iterate Systems every query


def parse_int(word):
    if word == '':
        word = 0
    return int(word)


def parse_float(word):
    if word == '':
        word = '0.0'
    return float(word)


class FortSystem(object):
    """
    Simple little object that transforms data for output or use.

    args:
        system: Name of system.
        data: List to be unpacked: ump, trigger, cmdr_merits, status, notes):

        Note: ump is a string like: 17%
        Note: trigger, cmdr_merits & status are strings like: 3,444
    """
    def __init__(self, data):
        self.name = data[9]
        self.um_percent = parse_float(data[0])
        self.fort_trigger = parse_int(data[2])
        cmdr_merits = parse_int(data[4])
        status = parse_int(data[5])
        self.fort_status = max(cmdr_merits, status)
        self.notes = data[8]

    def __str__(self):
        """ Format for output """
        return tbl.format_line(self.data_tuple)

    @property
    def data_tuple(self):
        """ Return a list of important data for tables """
        fort_status = '{:>4}/{:4} ({:2}%)'.format(self.fort_status,
                                                  self.fort_trigger,
                                                  self.completion)

        if self.skip:
            missing = 'Please do not fortify!'
        else:
            missing = '{:>4}'.format(self.missing)

        return [self.name, fort_status, missing, '{:2}%'.format(self.um_percent), self.notes]

    @property
    def skip(self):
        """ The system should be skipped. """
        notes = self.notes.lower()
        return 'leave' in notes or 'skip' in notes

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
    def __init__(self, data):
        """
        Parse data from table and initialize.
        """
        self.data = data

    def current(self):
        """
        Print out the current system to forify.
        """
        target = None

        # Seek targets in systems list
        for datum in self.data:
            system = FortSystem(datum)

            if not target and system.name != 'Othime' and \
                    not system.is_fortified and not system.skip:
                target = system

            if target:
                break

        return target.name

    def current_long(self):
        """
        Print out the current objectives to fortify and their status.
        """
        othime = None
        target = None

        # Seek targets in systems list
        for datum in self.data:
            system = FortSystem(datum)

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
        return tbl.format_table(lines, sep='|', header=True)

    def next_systems(self, num=None):
        """
        Return next 5 regular fort targets.
        """
        targets = []
        if not num:
            num = 5

        for datum in self.data:
            system = FortSystem(datum)

            if system.name != 'Othime' and not system.is_fortified and not system.skip:
                targets.append(system.name)

            if len(targets) == num + 1:
                break

        return '\n'.join(targets[1:])

    def next_systems_long(self, num=None):
        """
        Return next 5 regular fort targets.
        """
        targets = []
        if not num:
            num = 5

        for datum in self.data:
            system = FortSystem(datum)

            if system.name != 'Othime' and not system.is_fortified and not system.skip:
                targets.append(system.data_tuple)

            if len(targets) == num + 1:
                break

        return tbl.format_table([TABLE_HEADER] + targets[1:], sep='|', header=True)

    def totals(self):
        """
        Print running total of fortified, undermined systems.
        """
        undermined = 0
        fortified = 0

        for datum in self.data:
            system = FortSystem(datum)

            if system.is_fortified:
                fortified += 1
            if system.is_undermined:
                undermined += 1

        return 'Fortified {}/{tot}, Undermined: {}/{tot}'.format(fortified, undermined,
                                                                 tot=len(self.data))


def main():
    """
    Main function, does simple fort table test.
    """
    sheet_id = share.get_config('hudson', 'cattle', 'id')
    secrets = share.get_config('secrets', 'sheets')
    sheet = sheets.GSheet(sheet_id, secrets['json'], secrets['token'])

    result = sheet.get('!F1:BM10', dim='COLUMNS')
    # for data in result:
        # print(str(FortSystem(data)))
    table = FortTable(result)
    print(table.current())
    print(table.next_systems_long())

if __name__ == "__main__":
    main()
