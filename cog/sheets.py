"""
Interface with the google sheets api.

Underlying API and model
    https://developers.google.com/sheets/api/quickstart/python
Gspread base library
    https://gspread.readthedocs.io/en/latest/
New asyncio wrapper library
    https://gspread-asyncio.readthedocs.io/en/latest/

Note on value_render_option to explain difference:
    https://developers.google.com/sheets/api/reference/rest/v4/ValueRenderOption
"""
from __future__ import absolute_import, print_function
import asyncio
import functools
import logging
import os

import argparse
try:
    import gspread_asyncio
    import oauth2client as o2c
    import oauth2client.file as o2cf
except ImportError:
    print('Please run: pip install google-api-python-client oauth2client')

import cog.exc
import cog.util


APPLICATION_NAME = 'CogBot'
# Requires read and write access to user's account
REQ_SCOPE = 'https://www.googleapis.com/auth/spreadsheets'
AGCM = None  # Rate limiting by this manager


class ColCnt():
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

    def fwd(self):
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


class Column():
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

    def fwd(self):
        """
        Add exactly 1 to the column counters.

        Returns: The new column string.
        """
        add_counter = True
        for counter in self.counters:
            try:
                counter.fwd()
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
            call = self.fwd
        else:
            call = self.prev
            offset = offset * -1

        while offset:
            call()
            offset -= 1

        return self.__str__()


class AsyncGSheet():
    """
    Class to provide access to the sheet required by gspread_asyncio.
    All methods must be used inside of asynchronous context.

    Important:
        Sheet is 1 indexed, if any method calls for index consider that.

        A1 ranges specified is inclusive on both sides.
        For example fetching 'A1:B2' gets cells [[A1, A2], [B1, B2]]
    """
    def __init__(self, sheet_id, sheet_page):
        """
        Args:
            sheet_id: The document id to retrieve.
            sheet_page: The page or tab within the document to work with.
        """
        self.sheet_id = sheet_id
        self.sheet_page = sheet_page
        self.worksheet = None
        # These are stored 1 index, same as google sheets operate.
        self.last_col = None
        self.last_row = None

    @property
    def last_col_a1(self):
        """
        Get the A1 format last column in the current worksheet.
        """
        return index_to_column(self.last_col)

    async def init_sheet(self):
        """
        IMPORTANT: Call this before any other calls made to sheet.
        """
        sclient = await AGCM.authorize()
        document = await sclient.open_by_key(self.sheet_id)
        self.worksheet = await document.worksheet(self.sheet_page)
        self.last_col = len(await self.values_row(1))
        self.last_row = len(await self.values_col(1))
        logging.getLogger('cog.sheets').info("GSHEET Setup Complete for %s", self.sheet_page)

    async def batch_get(self, cells, dim='ROWS', value_render='UNFORMATTED_VALUE'):
        """
        This is exactly the same as the batch_get available via gspread.

        Args:
            cells: List of A1 format like [A1:B2, C12, C4:D8, ...]
            dim: Major dimension is ROWS by default. Possible choices: ROWS or COLUMNS
            value_render: The rendering option to format the data in.
                          By default, unformatted will only return the data.
        """
        await AGCM.authorize()
        return await self.worksheet.batch_get(cells, major_dimension=dim,
                                              value_render_option=value_render)

    async def batch_update(self, data, input_opt='RAW', value_render='UNFORMATTED_VALUE'):
        """
        This is exactly the same as the batch_get available via gspread.

        Args:
            data: List of dicts of form: [
                {'range': 'A1:B1', 'values': [[22, 53]]},
                ...
            ]
            input_opt: Default is 'RAW', store data as it exists.
                       'USER_ENTERED' would parse data as if entered in sheet.
            value_render: The rendering option to format the data in.
                          By default, unformatted will only return the data.
        """
        await AGCM.authorize()
        await self.worksheet.batch_update(data, value_input_option=input_opt,
                                          value_render_option=value_render)

    async def values_col(self, col_index, value_render='UNFORMATTED_VALUE'):
        """
        Return all values in a column.

        Args:
            col: The integer of the column to fetch, starts with 1.
            value_render: The rendering option to format the data in.
                          By default, unformatted will only return the data.
        """
        await AGCM.authorize()
        return await self.worksheet.col_values(col_index, value_render_option=value_render)

    async def values_row(self, row_index, value_render='UNFORMATTED_VALUE'):
        """
        Return all values in a row.

        Args:
            row: The integer of the row to fetch, starts with 1.
            value_render: The rendering option to format the data in.
                          By default, unformatted will only return the data.
        """
        await AGCM.authorize()
        return await self.worksheet.row_values(row_index, value_render_option=value_render)

    async def cells_get_range(self, a1_range):
        """
        Fetch a series of cells in an A1 range.

        Args:
            range: A1 range to fetch from the worksheet.

        Returns: A list of cells.
        """
        await AGCM.authorize()
        return await self.worksheet.range(a1_range)

    async def cells_update(self, cells, input_opt='RAW'):
        """
        Update a batch of GSpread cells that were previously fetched and changed.

        Args:
            cells: A flat list of cells to update.
            input_opt: Default is 'RAW', store data as it exists.
                       'USER_ENTERED' would parse data as if entered in sheet.
        """
        await AGCM.authorize()
        await self.worksheet.update_cells(cells, value_input_option=input_opt)

    async def whole_sheet(self):  # pragma: no cover
        """
        Fetch and return the entire sheet as a list of lists of strings of cell values.
        The cells will be in a 2d list that is row major.
        """
        # TODO: Fetch only part needed, we can trim by last_col_a1 and last_row
        await AGCM.authorize()
        return await self.worksheet.get_all_values()

    # TODO: Deprecated, prefer batch_get
    async def get(self, a1_range, dim='ROWS', value_render='UNFORMATTED_VALUE'):
        """
        Args:
            service: The service returned by the sheets api.
            sheet_id: The sheet identifier.
            cell_range: An A1 range that describes a single area to return.

        Returns: A list of rows in the area.
        """
        logging.getLogger('cog.sheets').info('SHEETS - Get Start')
        values = await self.batch_get([a1_range], dim)
        print("GET", values)
        logging.getLogger('cog.sheets').info('SHEETS - Get End')
        return values[0]

    # TODO: Deprecated, prefer batch_update
    #  def update(self, cell_range, n_vals, dim='ROWS'):
    async def update(self, a1_range, n_vals, input_opt='RAW', value_render='UNFORMATTED_VALUE'):
        """
        Set a whole range of values in the sheet.

        Args:
            service: The service returned by the sheets api.
            sheet_id: The sheet identifier.
            range: An A1 range that describes a single area to return.
            n_vals: New values to fit into range, list of lists.
        """
        logging.getLogger('cog.sheets').info('SHEETS - Update Start')
        await self.batch_update([{'range': a1_range, 'values': n_vals}])
        logging.getLogger('cog.sheets').info('SHEETS - Update End')


def get_credentials(json_secret, sheets_token):  # pragma: no cover
    """
    Get credentials from OAuth process.

    Args:
        json_secret: The json secret file downloaded from Google Api.
        sheets_token: Store the authorization in this file.

    Returns: Credentials obtained from oauth process.
    """
    if not os.path.exists(json_secret):
        raise cog.exc.MissingConfigFile('Missing JSON Secret for OAUTH. Expected at: %s'
                                        % json_secret)

    store = o2cf.Storage(sheets_token)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = o2c.client.flow_from_clientsecrets(json_secret, REQ_SCOPE)
        flow.user_agent = APPLICATION_NAME

        parser = argparse.ArgumentParser(parents=[o2c.tools.argparser])
        flags = parser.parse_args(['--noauth_local_webserver'])
        credentials = o2c.tools.run_flow(flow, store, flags)

        print('Storing credentials to ' + sheets_token)

    return credentials


def init_agcm(json_secret, sheet_token, loop=None):
    """
    Initialize the global AGCM, share with all sheets.
    Has internal rate limitting but we should do batch updates still to prevent hitting them.

    Args:
        json_secret: The path to the secret json file
        sheet_token: The path to the cached token authorization
        loop: The loop to attach the agcm to, by default with get_event_loop()
    """
    if not loop:
        loop = asyncio.get_event_loop()

    return gspread_asyncio.AsyncioGspreadClientManager(
        functools.partial(get_credentials, json_secret, sheet_token),
        loop=loop
    )


def column_to_index(col_str, zero_index=False):
    """
    Convert a column A1 string to an **1** index in sheet cells.

    Args:
        col_str: The A1 format column part (i.e. AG) string to convert
        zero_index: Set true to get a zero index back.

    Returns:
        0 index offset of the column.
    """
    cnt = 1
    column = Column('A')

    while str(column) != col_str:
        column.fwd()
        cnt += 1

    if zero_index:
        cnt -= 1

    return cnt


def index_to_column(one_index):
    """
    Convert a **1 index** (i.e. starts at 1) to an equivalent A1 column name.

    Returns:
        An A1 compliant string.
    """
    col = Column()
    col.offset(one_index - 1)
    return str(col)


async def test_func():
    paths = cog.util.get_config('paths')
    agcm = init_agcm(paths['json'], paths['token'])
    sclient = await agcm.authorize()
    print('Authorized', sclient)

    sid = '1p75GrSdqjCi_0Y-9aLZHN4maruuSluhzpxjcWtmquGw'
    page = 'TestLive'
    sheet = await sclient.open_by_key(sid)
    print('title', await sheet.get_title())

    wsheet = await sheet.worksheet(page)
    print("All values")
    vals = await wsheet.get_all_values()
    __import__('pprint').pprint(vals[0:4])


async def test_func2():
    paths = cog.util.get_config('paths')
    global AGCM
    AGCM = init_agcm(paths['json'], paths['token'])

    sid = '1p75GrSdqjCi_0Y-9aLZHN4maruuSluhzpxjcWtmquGw'
    page = 'TestLive'
    asheet = AsyncGSheet(sid, page)
    await asheet.init_sheet()

    vals = await asheet.cells_get_range('A10:{}10'.format(asheet.last_col_a1))
    print(vals)


def main():
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.get_event_loop().set_debug(True)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_func2())


if __name__ == "__main__":
    main()
