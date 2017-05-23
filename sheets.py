"""
Interface with the google sheets api.

Tutorial and reference available at:
    https://developers.google.com/sheets/api/quickstart/python
"""
from __future__ import absolute_import, print_function
import math
import os

import argparse
import httplib2
try:
    from apiclient import discovery
    from oauth2client import client, tools
    from oauth2client.file import Storage
except ImportError:
    print('Please run: pip install google-api-python-client')

import share

APPLICATION_NAME = 'GearBot'
# Requires read and write access to user's account
REQ_SCOPE = 'https://www.googleapis.com/auth/spreadsheets'


class MissingConfigFile(Exception):
    """
    Thrown if a config isn't set properly.
    """
    pass


class ColOverflow(Exception):
    """
    Raise when a column has reached end, increment next column.
    """
    pass


class IncompleteData(Exception):
    """
    Raise when data no longer contains useful information.
    """
    pass


class ColCnt(object):
    """
    Simple counter that resets and prints its character.
    """
    def __init__(self, char='A'):
        self.char = char
        self.stop_char = chr(ord('Z') + 1)

    def __repr__(self):
        return "<ColCnt(char='{}', stop_char='{}')>".format(self.char, self.stop_char)

    def __str__(self):
        return self.char

    def next(self):
        """
        Move to next character.

        Raises:
            ColOverflow: When the counter exceeds its bounds. Counter is reset before throwing.
        """
        self.char = chr(ord(self.char) + 1)
        if self.char == self.stop_char:
            self.reset()
            raise ColOverflow

    def reset(self):
        """
        Reset to first character.
        """
        self.char = 'A'


class Column(object):
    """
    Model a column in an excel sheet of form A-Z, AA, AB ... AZ, BA ....
    """
    def __init__(self, init_col='A'):
        """
        Access the current column string by using str().

        IMPORTANT: Counters are stored backwards, lease significant counter at index 0.

        Args:
            init_col: An string representing an excel column of A-Z, AA, AB, etc ...
        """
        self.counters = []

        for char in init_col:
            self.counters.insert(0, ColCnt(char))

    def __str__(self):
        msg = ''

        for counter in self.counters:
            msg = str(counter) + msg

        return msg

    def next(self):
        """
        Add exactly 1 to the column counters.

        Returns: The new column string.
        """
        add_counter = True
        for counter in self.counters:
            try:
                counter.next()
                add_counter = False
                break
            except ColOverflow:
                pass

        if add_counter:
            self.counters.append(ColCnt())

        return self.__str__()

    def offset(self, offset):
        """
        Increment counters by offset.

        Returns: The new column string.
        """
        while offset:
            self.next()
            offset -= 1

        return self.__str__()


class GSheet(object):
    """
    Class to wrap the sheet api and provide convenience methods.

    Results are returned or sent via api in following form:
    {
        "range": string,
        "majorDimension": enum(Dimension),
        "values": [
            array
        ],
    }
    """
    def __init__(self, sheet_id, json_secret, sheet_token):
        """
        Args:
            sheet_id: The id of the google sheet to interact with.
            json_secret: Path to the secret json api for client.
            sheet_token: Path to store token authorizing api.
        """
        self.sheet_id = sheet_id
        self.credentials = get_credentials(json_secret, sheet_token)
        http = self.credentials.authorize(httplib2.Http())
        discovery_url = ('https://sheets.googleapis.com/$discovery/rest?'
                         'version=v4')
        self.service = discovery.build('sheets', 'v4', http=http,
                                       discoveryServiceUrl=discovery_url)

    def get(self, cell_range, dim='ROWS'):
        """
        Args:
            service: The service returned by the sheets api.
            sheet_id: The sheet identifier.
            cell_range: An A1 range that describes a single area to return.

        Returns: A list of rows in the area.
        """
        # Default returns by row, use majorDimension = 'COLUMNS' to flip.
        values = self.service.spreadsheets().values()
        result = values.get(spreadsheetId=self.sheet_id, range=cell_range,
                            majorDimension=dim,
                            valueRenderOption='UNFORMATTED_VALUE').execute()
        return result.get('values', [])

    def update(self, cell_range, n_vals, dim='ROWS'):
        """
        Set a whole range of values in the sheet.

        Args:
            service: The service returned by the sheets api.
            sheet_id: The sheet identifier.
            range: An A1 range that describes a single area to return.
            n_vals: New values to fit into range, list of lists.
        """
        body = {
            'majorDimension': dim,
            'values': n_vals
        }
        values = self.service.spreadsheets().values()
        values.update(spreadsheetId=self.sheet_id, range=cell_range,
                      valueInputOption='RAW', body=body).execute()

    def batch_get(self, cell_ranges, dim='ROWS'):
        """
        Similar to get_range except take a list of cell_ranges.
        """
        values = self.service.spreadsheets().values()
        result = values.batchGet(spreadsheetId=self.sheet_id, ranges=cell_ranges,
                                 majorDimension=dim,
                                 valueRenderOption='UNFORMATTED_VALUE').execute()

        return [ent.get('values', []) for ent in result['valueRanges']]
        # return result['valueRanges']

    def batch_update(self, cell_ranges, n_vals, dim='ROWS'):
        """
        Similar to set_range except take a list of lists for ranges and n_vals.
        """
        body = {
            'valueInputOption': 'RAW',
            'data': [],
        }
        for cell_range, n_val in zip(cell_ranges, n_vals):
            body['data'].append({
                'range': cell_range,
                'majorDimension': dim,
                'values': n_val,
            })
        values = self.service.spreadsheets().values()
        values.batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()


def get_credentials(json_secret, sheets_token):
    """
    Get credentials from OAuth process.

    Args:
        json_secret: The json secret file downloaded from Google Api.
        sheets_token: Store the authorization in this file.

    Returns: Credentials obtained from oauth process.
    """
    if not os.path.exists(json_secret):
        raise MissingConfigFile('Missing: ' + json_secret)

    store = Storage(sheets_token)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(json_secret, REQ_SCOPE)
        flow.user_agent = APPLICATION_NAME

        parser = argparse.ArgumentParser(parents=[tools.argparser])
        flags = parser.parse_args(['--noauth_local_webserver'])
        credentials = tools.run_flow(flow, store, flags)

        print('Storing credentials to ' + sheets_token)

    return credentials


def parse_int(word):
    if word == '':
        word = 0
    return int(word)


def parse_float(word):
    if word == '':
        word = 0.0
    return float(word)


def system_result_dict(lines, order, column):
    """
    Map the json result from systems request into kwargs to initialize the system with.

    lines: A list of the following
        0   - undermine % (comes as float 0.0 - 1.0)
        1   - completion % (comes as float 0.0 - 1.0)
        2   - fortification trigger
        3   - missing merits
        4   - merits dropped by commanders
        5   - status updated manually (defaults to '', map to 0)
        6   - undermine updated manually (defaults to '', map to 0)
        7   - distance from hq (float, always set)
        8   - notes (defaults '')
        9   - system name
    order: The order of this data set relative others.
    column: The column string this data belongs in.
    """
    try:
        if lines[9] == '':
            raise IncompleteData
    except IndexError:
        raise IncompleteData

    return {
        'undermine': parse_float(lines[0]),
        'trigger': parse_int(lines[2]),
        'cmdr_merits': lines[4],
        'fort_status': parse_int(lines[5]),
        'notes': lines[8],
        'name': lines[9],
        'sheet_col': column,
        'sheet_order': order,
    }


def main():
    """
    Simple main func for quick tests.
    """
    # Dummy sheet that can be manipulated at will.
    sheet_id = share.get_config('hudson', 'cattle', 'id')
    secrets = share.get_config('secrets', 'sheets')
    sheet = GSheet(sheet_id, secrets['json'], secrets['token'])

    print(sheet.get('!B16:B16'))
    # sheet.update('!B16:B16', [['Shepron']])


if __name__ == "__main__":
    main()
