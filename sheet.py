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

import share

APPLICATION_NAME = 'GearBot'
# Requires read and write access to user's account
REQ_SCOPE = 'https://www.googleapis.com/auth/spreadsheets'


class MissingConfigFile(Exception):
    """
    Thrown if a config isn't set properly.
    """
    pass


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


class SheetApi(object):
    """
    Class to wrap the sheet api and provide convenience methods.
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

    def get_range(self, cell_range, dim='ROWS'):
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

    def set_range(self, cell_range, n_vals, dim='ROWS'):
        """
        Set a whole range of values in the sheet.

        Args:
            service: The service returned by the sheets api.
            sheet_id: The sheet identifier.
            range: An A1 range that describes a single area to return.
            n_vals: New values to fit into range, list of lists.
        """
        body = {'values': n_vals}
        values = self.service.spreadsheets().values()
        values.update(spreadsheetId=self.sheet_id, range=cell_range, majorDimension=dim,
                      valueInputOption='RAW', body=body).execute()

    def batch_get(self, cell_ranges, dim='ROWS'):
        """
        Similar to get_range except take a list of cell_ranges.
        """
        values = self.service.spreadsheets().values()
        result = values.batchGet(spreadsheetId=self.sheet_id, ranges=cell_ranges,
                                 majorDimension=dim,
                                 valueRenderOption='UNFORMATTED_VALUE').execute()

        return result['valueRanges']

    def batch_set(self, cell_ranges, n_vals, dim='ROWS'):
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


def main():
    """
    Simple main func for quick tests.
    """
    # Dummy sheet that can be manipulated at will.
    sheet_id = share.get_config('hudson', 'cattle_id')
    secrets = share.get_config('secrets', 'sheets')
    sheet = SheetApi(sheet_id, secrets['json'], secrets['token'])
    print(sheet.get_range('!A11:B20'))
    # data_range = ['!B11:B17', '!F11:G17']


if __name__ == "__main__":
    main()
