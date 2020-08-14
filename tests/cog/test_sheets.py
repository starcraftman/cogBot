"""
Test sheets api logic

NOTE: GSheet tests being skipped, they are slow and that code is mostly frozen.
"""
import os

import aiofiles
import gspread
import gspread_asyncio
import pytest

import cog.exc
import cog.sheets
import cog.util
from tests.conftest import SHEET_TEST


# Caching for a library component that I want to share across tests.
PATHS = cog.util.get_config('paths')
AGCM = None
FORT_INPUT = os.path.join(cog.util.ROOT_DIR, 'tests', 'test_input.unit_fort.txt')


@pytest.fixture()
async def f_fort_ws():
    """
    Yield fixture returns fort sheet.
    """
    agcm = cog.sheets.init_agcm(PATHS['json'], PATHS['token'])
    cog.sheets.AGCM = agcm
    await agcm.authorize()

    sheet = cog.util.get_config('tests', 'hudson_cattle')
    asheet = cog.sheets.AsyncGSheet(sheet['id'], sheet['page'])
    await asheet.init_sheet()

    yield asheet


@pytest.fixture()
async def f_um_ws():
    """
    Yield fixture returns fort sheet.
    """
    agcm = cog.sheets.init_agcm(PATHS['json'], PATHS['token'])
    cog.sheets.AGCM = agcm
    await agcm.authorize()

    sheet = cog.util.get_config('tests', 'hudson_undermine')
    asheet = cog.sheets.AsyncGSheet(sheet['id'], sheet['page'])
    await asheet.init_sheet()

    yield asheet


@pytest.fixture()
async def f_fort_reset(f_fort_ws):
    """
    Yield fixture returns fort sheet and cleanups after running.
    Use these cells that are in below payload if you want to update unit sheet.
    """
    payloads = [{
        'range': 'B13:B14',
        'values': [['Shepron'], ['TiddyMun']],
    }, {
        'range': 'F6:G6',
        'values': [[4910, 2671]],
    }]

    yield f_fort_ws

    await f_fort_ws.worksheet.batch_update(payloads)


@SHEET_TEST
@pytest.mark.asyncio
async def test_asheet_init_sheet():
    agcm = cog.sheets.init_agcm(PATHS['json'], PATHS['token'])
    cog.sheets.AGCM = agcm
    await agcm.authorize()

    sheet = cog.util.get_config('tests', 'hudson_cattle')
    asheet = cog.sheets.AsyncGSheet(sheet['id'], sheet['page'])
    await asheet.init_sheet()

    assert isinstance(asheet.worksheet, gspread_asyncio.AsyncioGspreadWorksheet)


@SHEET_TEST
@pytest.mark.asyncio
async def test_asheet_title(f_fort_ws):
    assert await f_fort_ws.title() == 'The Battle Cattle Sheet'


@SHEET_TEST
@pytest.mark.asyncio
async def test_asheet_change_worksheet(f_fort_ws):
    ranges = ['B11:C12']
    new_sheet = 'Cycle 240'
    old_vals = await f_fort_ws.batch_get(ranges)
    old_ws = f_fort_ws.worksheet

    await f_fort_ws.change_worksheet(new_sheet)

    assert await f_fort_ws.batch_get(ranges) != old_vals
    assert f_fort_ws.worksheet != old_ws
    assert f_fort_ws.sheet_page == new_sheet


@SHEET_TEST
@pytest.mark.asyncio
async def test_asheet_batch_get(f_fort_ws):
    result = await f_fort_ws.batch_get(['B13:B13', 'F6:G6'])

    assert result == [[['Shepron']], [[4910, 2671]]]


@SHEET_TEST
@pytest.mark.asyncio
async def test_asheet_batch_update(f_fort_reset):
    payloads = [{
        'range': 'B13:B14',
        'values': [['NotShepron'], ['Grimbald']],
    }, {
        'range': 'F6:G6',
        'values': [[2222, 3333]],
    }]

    await f_fort_reset.batch_update(payloads)

    new_cells = await f_fort_reset.batch_get(['B13:B14', 'F6:G6'])
    assert new_cells == [payloads[0]['values'], payloads[1]['values']]


@SHEET_TEST
@pytest.mark.asyncio
async def test_asheet_values_col(f_fort_ws):
    async with aiofiles.open(FORT_INPUT, 'r') as fin:
        expect = eval(await fin.read())
        expect = cog.util.transpose_table(expect)

    result = await f_fort_ws.values_col(1)

    assert result == expect[0][:25]


@SHEET_TEST
@pytest.mark.asyncio
async def test_asheet_cells_get(f_fort_ws):
    result = await f_fort_ws.cells_get('F6:G6',)

    assert isinstance(result[0], gspread.models.Cell)
    assert result[0].value == '4,910'
    assert result[1].value == '2,671'


@SHEET_TEST
@pytest.mark.asyncio
async def test_asheet_cells_update(f_fort_reset):
    cells = await f_fort_reset.cells_get('F6:G6',)
    cells[0].value = 2222
    cells[1].value = 3333

    await f_fort_reset.cells_update(cells)

    result = await f_fort_reset.cells_get('F6:G6',)
    assert isinstance(result[0], gspread.models.Cell)
    assert result[0].value == '2,222'
    assert result[1].value == '3,333'


@SHEET_TEST
@pytest.mark.asyncio
async def test_asheet_values_row(f_fort_ws):
    async with aiofiles.open(FORT_INPUT, 'r') as fin:
        expect = eval(await fin.read())

    result = await f_fort_ws.values_row(10)

    assert result == expect[9]


@SHEET_TEST
@pytest.mark.asyncio
async def test_asheet_whole_sheet(f_fort_ws):
    async with aiofiles.open(FORT_INPUT, 'r') as fin:
        expect = cog.util.transpose_table(eval(await fin.read()))

    result = cog.util.transpose_table(await f_fort_ws.whole_sheet())

    assert result[0] == expect[0]
    assert result[2] == expect[2]


def test_colcnt__init__():
    col1 = cog.sheets.ColCnt('A')
    assert str(col1) == 'A'
    col2 = cog.sheets.ColCnt('Z')
    assert str(col2) == 'Z'


def test_colcnt__repr__():
    col1 = cog.sheets.ColCnt('A')
    assert repr(col1) == 'ColCnt(char=65, low_bound=64, high_bound=91)'


def test_colcnt_fwd():
    col1 = cog.sheets.ColCnt('A')
    col1.fwd()
    assert str(col1) == 'B'

    col2 = cog.sheets.ColCnt('Z')
    with pytest.raises(cog.exc.ColOverflow):
        col2.fwd()
    assert str(col2) == 'A'


def test_colcnt_back():
    col1 = cog.sheets.ColCnt('B')
    col1.back()
    assert str(col1) == 'A'

    with pytest.raises(cog.exc.ColOverflow):
        col1.back()
    assert str(col1) == 'Z'


def test_colcnt_reset():
    col1 = cog.sheets.ColCnt('Z')
    col1.reset()
    assert str(col1) == 'A'

    col2 = cog.sheets.ColCnt('A')
    col2.reset(False)
    assert str(col2) == 'Z'


def test_column__init__():
    column = cog.sheets.Column('A')
    assert str(column) == 'A'
    assert str(column.counters[0]) == 'A'

    column = cog.sheets.Column('BA')
    assert str(column) == 'BA'
    assert str(column.counters[0]) == 'A'
    assert str(column.counters[1]) == 'B'


def test_column__repr__():
    col1 = cog.sheets.Column('AA')
    assert repr(col1) == "Column(counters=[ColCnt(char=65, low_bound=64, high_bound=91), "\
                         "ColCnt(char=65, low_bound=64, high_bound=91)])"


def test_column_fwd():
    column = cog.sheets.Column('A')
    assert column.fwd() == 'B'

    column = cog.sheets.Column('Z')
    assert column.fwd() == 'AA'
    assert column.fwd() == 'AB'


def test_column_back():
    column = cog.sheets.Column('B')
    assert column.back() == 'A'

    column = cog.sheets.Column('AA')
    assert column.back() == 'Z'
    assert column.back() == 'Y'


def test_column_offset():
    column = cog.sheets.Column('A')
    column.offset(5)
    assert str(column) == 'F'

    column.offset(-5)
    assert str(column) == 'A'


def test_column_to_index():
    column = cog.sheets.Column('A')
    assert cog.sheets.column_to_index(str(column)) == 1

    column2 = cog.sheets.Column('AA')
    assert cog.sheets.column_to_index(str(column2)) == 27


def test_column_to_index_zero_index():
    column = cog.sheets.Column('A')
    assert cog.sheets.column_to_index(str(column), zero_index=True) == 0


def test_index_to_column():
    assert cog.sheets.index_to_column(1) == 'A'

    assert cog.sheets.index_to_column(27) == 'AA'


@pytest.mark.asyncio
async def test_init_agcm():
    sheets = cog.util.get_config('tests', 'hudson_cattle')
    agcm = cog.sheets.init_agcm(sheets['id'], sheets['page'])
    assert isinstance(agcm, gspread_asyncio.AsyncioGspreadClientManager)
