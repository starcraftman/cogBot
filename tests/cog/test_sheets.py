"""
Test sheets api logic

NOTE: GSheet tests being skipped, they are slow and that code is mostly frozen.
"""
from __future__ import absolute_import, print_function

import pytest

import cog.exc
import cog.sheets
import cog.util
from tests.conftest import SHEET_TEST


@pytest.fixture()
def fort_sheet():
    """
    Yield fixture returns fort sheet.
    """
    sheet = cog.util.get_config('tests', 'hudson_cattle')
    paths = cog.util.get_config('paths')
    f_sheet = cog.sheets.GSheet(sheet, paths['json'], paths['token'])

    yield f_sheet


@pytest.fixture()
def fort_sheet_reset():
    """
    Yield fixture returns fort sheet and cleanups after running.

    N.B. Test in cells cleaned in cell_ranges.
    """
    sheet = cog.util.get_config('tests', 'hudson_cattle')
    paths = cog.util.get_config('paths')
    f_sheet = cog.sheets.GSheet(sheet, paths['json'], paths['token'])

    yield f_sheet

    # Ensure scratch cells always reset, stuck in catch22 batch_update must work
    cell_ranges = ['!B13:B14', '!F6:G6']
    n_vals = [[['Shepron'], ['TiddyMun']], [[4910, 2671]]]
    f_sheet.batch_update(cell_ranges, n_vals)


@SHEET_TEST
def test_gsheet_get(fort_sheet):
    assert fort_sheet.get('!B13:B13') == [['Shepron']]


@SHEET_TEST
def test_gsheet_batch_get(fort_sheet):
    assert fort_sheet.batch_get(['!B13:B13', '!F6:G6']) == [[['Shepron']], [[4910, 2671]]]


@SHEET_TEST
def test_ghseet_get_with_formatting(fort_sheet):
    fmt_cells = fort_sheet.get_with_formatting('!F10:F10')
    system_colors = {'red': 0.42745098, 'blue': 0.92156863, 'green': 0.61960787}

    for val in fmt_cells['sheets'][0]['data'][0]['rowData'][0]['values']:
        assert val['effectiveFormat']['backgroundColor'] == system_colors


@SHEET_TEST
def test_gsheet_update(fort_sheet_reset):
    fort_sheet_reset.update('!B13:B13', [['NotShepron']])
    assert fort_sheet_reset.get('!B13:B13') == [['NotShepron']]


@SHEET_TEST
def test_gsheet_batch_update(fort_sheet_reset):
    cell_ranges = ['!B13:B14', '!F6:G6']
    n_vals = [[['NotShepron'], ['Grimbald']], [[2222, 3333]]]
    fort_sheet_reset.batch_update(cell_ranges, n_vals)

    assert fort_sheet_reset.batch_get(cell_ranges) == n_vals


def test_colcnt__init__():
    col1 = cog.sheets.ColCnt('A')
    assert str(col1) == 'A'
    col2 = cog.sheets.ColCnt('Z')
    assert str(col2) == 'Z'


def test_colcnt__repr__():
    col1 = cog.sheets.ColCnt('A')
    assert repr(col1) == 'ColCnt(char=65, low_bound=64, high_bound=91)'


def test_colcnt_next():
    col1 = cog.sheets.ColCnt('A')
    col1.next()
    assert str(col1) == 'B'

    col2 = cog.sheets.ColCnt('Z')
    with pytest.raises(cog.exc.ColOverflow):
        col2.next()
    assert str(col2) == 'A'


def test_colcnt_prev():
    col1 = cog.sheets.ColCnt('B')
    col1.prev()
    assert str(col1) == 'A'

    with pytest.raises(cog.exc.ColOverflow):
        col1.prev()
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


def test_column_next():
    column = cog.sheets.Column('A')
    assert column.next() == 'B'

    column = cog.sheets.Column('Z')
    assert column.next() == 'AA'
    assert column.next() == 'AB'


def test_column_prev():
    column = cog.sheets.Column('B')
    assert column.prev() == 'A'

    column = cog.sheets.Column('AA')
    assert column.prev() == 'Z'
    assert column.prev() == 'Y'


def test_column_offset():
    column = cog.sheets.Column('A')
    column.offset(5)
    assert str(column) == 'F'

    column.offset(-5)
    assert str(column) == 'A'


def test_column_to_index():
    column = cog.sheets.Column('A')
    assert cog.sheets.column_to_index(str(column)) == 0

    column2 = cog.sheets.Column('AA')
    assert cog.sheets.column_to_index(str(column2)) == 26
