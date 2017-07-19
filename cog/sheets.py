"""
Interface with the google sheets api.

Tutorial and reference available at:
    https://developers.google.com/sheets/api/quickstart/python
"""
from __future__ import absolute_import, print_function
import os

import argparse
import httplib2
try:
    from apiclient import discovery
    from oauth2client import client, tools
    from oauth2client.file import Storage
except ImportError:
    print('Please run: pip install google-api-python-client')

import cog.exc


APPLICATION_NAME = 'CogBot'
# Requires read and write access to user's account
REQ_SCOPE = 'https://www.googleapis.com/auth/spreadsheets'


class ColCnt(object):
    """
    Simple counter that resets and prints its character.
    """
    def __init__(self, char, low='A', high='Z'):
        self.char = ord(char)
        self.low_bound = ord(low) - 1
        self.high_bound = ord(high) + 1

    def __repr__(self):
        keys = ['char', 'low_bound', 'high_bound']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "ColCnt({})".format(', '.join(kwargs))

    def __str__(self):
        return chr(self.char)

    def next(self):
        """
        Move to next character.

        Raises:
            ColOverflow: When the counter exceeds its bounds. Counter is reset before throwing.
        """
        self.char = self.char + 1
        if self.char == self.high_bound:
            self.reset()
            raise cog.exc.ColOverflow

    def prev(self):
        """
        Move to previous character.

        Raises:
            ColOverflow: When the counter exceeds its bounds. Counter is reset before throwing.
        """
        self.char = self.char - 1
        if self.char == self.low_bound:
            self.reset(overflow=False)
            raise cog.exc.ColOverflow

    def reset(self, overflow=True):
        """
        Reset to first character.
        """
        if overflow:
            self.char = ord('A')
        else:
            self.char = ord('Z')


class Column(object):
    """
    Model a column in an excel sheet of form A-Z, AA, AB ... AZ, BA ....
    """
    def __init__(self, init_col='A'):
        """
        Access the current column string by using str().

        IMPORTANT: Counters are stored backwards, lease significant counter at index 0.

        Args:
            init_col: A string representing an excel column of A-Z, AA, AB, etc ...
        """
        self.counters = []

        for char in init_col:
            self.counters.insert(0, ColCnt(char))

    def __repr__(self):
        return "Column({}={!r})".format('counters', self.counters)

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
            except cog.exc.ColOverflow:
                pass

        if add_counter:
            self.counters.append(ColCnt('A'))

        return self.__str__()

    def prev(self):
        """
        Subtract exactly 1 from the column counters.

        Returns: The new column string.
        """
        sub_counter = True
        for counter in self.counters:
            try:
                counter.prev()
                sub_counter = False
                break
            except cog.exc.ColOverflow:
                pass

        if sub_counter:
            self.counters = self.counters[:-1]

        return self.__str__()

    def offset(self, offset):
        """
        Increment counters by offset.

        Returns: The new column string.
        """
        if offset > 0:
            call = self.next
        else:
            call = self.prev
            offset = offset * -1

        while offset:
            call()
            offset -= 1

        return self.__str__()


class GSheet(object):
    """
    Class to wrap the sheet api and provide convenience methods.

    Important: The A1 range specified is inclusive on both sides.
    For example sheet.get('!A1:B2') fetches cells [[A1, A2], [B1, B2]]

    Results are returned or sent via api in following form:
    {
        "range": string,
        "majorDimension": enum(Dimension),
        "values": [
            array
        ],
    }
    """
    def __init__(self, sheet, json_secret, sheet_token):
        """
        Args:
            sheet_id: The id of the google sheet to interact with.
            json_secret: Path to the secret json api for client.
            sheet_token: Path to store token authorizing api.
        """
        self.sheet_id = sheet['id']
        self.page = "'{}'".format(sheet['page'])
        self.credentials = get_credentials(json_secret, sheet_token)
        http = self.credentials.authorize(httplib2.Http())
        discovery_url = ('https://sheets.googleapis.com/$discovery/rest?'
                         'version=v4')
        self.service = discovery.build('sheets', 'v4', http=http,
                                       discoveryServiceUrl=discovery_url)

    @property
    def values(self):
        """
        Alias to service.spreadsheets().values()
        """
        return self.service.spreadsheets().values()  # pylint: disable=no-member

    def get(self, cell_range, dim='ROWS'):
        """
        Args:
            service: The service returned by the sheets api.
            sheet_id: The sheet identifier.
            cell_range: An A1 range that describes a single area to return.

        Returns: A list of rows in the area.
        """
        # Default returns by row, use majorDimension = 'COLUMNS' to flip.
        result = self.values.get(spreadsheetId=self.sheet_id,
                                 range=self.page + cell_range,
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
        self.values.update(spreadsheetId=self.sheet_id, range=self.page + cell_range,
                           valueInputOption='RAW', body=body).execute()

    def batch_get(self, cell_ranges, dim='ROWS'):
        """
        Similar to get_range except take a list of cell_ranges.
        """
        cell_ranges = [self.page + crange for crange in cell_ranges]
        result = self.values.batchGet(spreadsheetId=self.sheet_id, ranges=cell_ranges,
                                      majorDimension=dim,
                                      valueRenderOption='UNFORMATTED_VALUE').execute()

        return [ent.get('values', []) for ent in result['valueRanges']]

    def batch_update(self, cell_ranges, n_vals, dim='ROWS'):
        """
        Similar to set_range except take a list of lists for ranges and n_vals.
        """
        cell_ranges = [self.page + crange for crange in cell_ranges]
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
        self.values.batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()

    def get_with_formatting(self, cell_range):
        """
        Get cells with formatting information.
        """
        sheets = self.service.spreadsheets()  # pylint: disable=no-member
        return sheets.get(spreadsheetId=self.sheet_id, ranges=self.page + cell_range,
                          includeGridData=True).execute()

    def whole_sheet(self, dim='COLUMNS'):
        """
        Simple alias to fetch a whole sheet, simply request far beyond
        possible column range.

        Returns: 2D list of sheet.
        """
        return self.get('!A:ZZ', dim=dim)


def column_to_index(col_str):
    """
    Convert a column string to an index in sheet cells.
    """
    cnt = 0
    column = cog.sheets.Column('A')

    while str(column) != col_str:
        column.next()
        cnt += 1

    return cnt


def get_credentials(json_secret, sheets_token):
    """
    Get credentials from OAuth process.

    Args:
        json_secret: The json secret file downloaded from Google Api.
        sheets_token: Store the authorization in this file.

    Returns: Credentials obtained from oauth process.
    """
    if not os.path.exists(json_secret):
        raise cog.exc.MissingConfigFile('Missing: ' + json_secret)

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
