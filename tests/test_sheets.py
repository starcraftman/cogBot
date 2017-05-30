"""
Test sheets api logic
"""
from __future__ import absolute_import, print_function

import pytest

import cog.exc
import cog.share
import cog.sheets


@pytest.fixture()
def fort_sheet():
    """
    Yield fixture returns fort sheet.
    """
    sheet_id = cog.share.get_config('hudson', 'cattle', 'id')
    secrets = cog.share.get_config('secrets', 'sheets')
    f_sheet = cog.sheets.GSheet(sheet_id, secrets['json'], secrets['token'])

    yield f_sheet


@pytest.fixture()
def fort_sheet_reset():
    """
    Yield fixture returns fort sheet and cleanups after running.

    N.B. Test in cells cleaned in cell_ranges.
    """
    sheet_id = cog.share.get_config('hudson', 'cattle', 'id')
    secrets = cog.share.get_config('secrets', 'sheets')
    f_sheet = cog.sheets.GSheet(sheet_id, secrets['json'], secrets['token'])

    yield f_sheet

    # Ensure scratch cells always reset, stuck in catch22 batch_update must work
    cell_ranges = ['!B16:B17', '!F6:G6']
    n_vals = [[['Shepron'], ['Grimbald']], [[4910, 4350]]]
    f_sheet.batch_update(cell_ranges, n_vals)


def test_get(fort_sheet):
    assert fort_sheet.get('!B16:B16') == [['Shepron']]


def test_batch_get(fort_sheet):
    assert fort_sheet.batch_get(['!B16:B16', '!F6:G6']) == [[['Shepron']], [[4910, 4350]]]


def test_update(fort_sheet_reset):
    fort_sheet_reset.update('!B16:B16', [['NotShepron']])
    assert fort_sheet_reset.get('!B16:B16') == [['NotShepron']]


def test_batch_update(fort_sheet_reset):
    cell_ranges = ['!B16:B17', '!F6:G6']
    n_vals = [[['NotShepron'], ['Grimbald']], [[2222, 3333]]]
    fort_sheet_reset.batch_update(cell_ranges, n_vals)

    assert fort_sheet_reset.batch_get(cell_ranges) == n_vals


def test_ColCnt_init():
    col1 = cog.sheets.ColCnt()
    assert str(col1) == 'A'
    col2 = cog.sheets.ColCnt('Z')
    assert str(col2) == 'Z'


def test_ColCnt_next():
    col1 = cog.sheets.ColCnt()
    col1.next()
    assert str(col1) == 'B'

    col2 = cog.sheets.ColCnt('Z')
    with pytest.raises(cog.exc.ColOverflow):
        col2.next()
    assert str(col2) == 'A'


def test_ColCnt_reset():
    col2 = cog.sheets.ColCnt('Z')
    col2.reset()
    assert str(col2) == 'A'


def test_Column_init():
    column = cog.sheets.Column()
    assert str(column) == 'A'
    assert str(column.counters[0]) == 'A'
    column = cog.sheets.Column('BA')
    assert str(column) == 'BA'
    assert str(column.counters[0]) == 'A'
    assert str(column.counters[1]) == 'B'


def test_Column_next():
    column = cog.sheets.Column()
    assert column.next() == 'B'

    column = cog.sheets.Column('Z')
    assert column.next() == 'AA'
    assert column.next() == 'AB'


def test_Column_offset():
    column = cog.sheets.Column()
    column.offset(5)
    assert str(column) == 'F'


def test_parse_int():
    assert cog.sheets.parse_int('') == 0
    assert cog.sheets.parse_int('2') == 2
    assert cog.sheets.parse_int(5) == 5


def test_parse_float():
    assert cog.sheets.parse_float('') == 0.0
    assert cog.sheets.parse_float('2') == 2.0
    assert cog.sheets.parse_float(0.5) == 0.5


def test_system_result_dict():
    data = ['', 1, 4910, 0, 4322, 4910, 0, 116.99, '', 'Frey']
    result = cog.sheets.system_result_dict(data, 0, 'F')
    assert result['undermine'] == 0.0
    assert result['trigger'] == 4910
    assert result['cmdr_merits'] == 4322
    assert result['fort_status'] == 4910
    assert result['notes'] == ''
    assert result['name'] == 'Frey'
    assert result['sheet_col'] == 'F'
    assert result['sheet_order'] == 0
