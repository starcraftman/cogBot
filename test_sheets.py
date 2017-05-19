"""
Test sheets api logic
"""
from __future__ import absolute_import, print_function

import pytest

import share
import sheets


@pytest.fixture()
def fort_sheet():
    """
    Yield fixture returns fort sheet.
    """
    sheet_id = share.get_config('hudson', 'cattle', 'id')
    secrets = share.get_config('secrets', 'sheets')
    f_sheet = sheets.GSheet(sheet_id, secrets['json'], secrets['token'])

    yield f_sheet


@pytest.fixture()
def fort_sheet_reset():
    """
    Yield fixture returns fort sheet and cleanups after running.

    N.B. Test in cells cleaned in cell_ranges.
    """
    sheet_id = share.get_config('hudson', 'cattle', 'id')
    secrets = share.get_config('secrets', 'sheets')
    f_sheet = sheets.GSheet(sheet_id, secrets['json'], secrets['token'])

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


def test_col_to_int():
    with pytest.raises(sheets.ConversionException):
        sheets.col_to_int('a')
    with pytest.raises(sheets.ConversionException):
        sheets.col_to_int('0')
    assert sheets.col_to_int('A') == 1
    assert sheets.col_to_int('Z') == 26
    assert sheets.col_to_int('AA') == 27
    assert sheets.col_to_int('BA') == 53


def test_col_to_char():
    with pytest.raises(sheets.ConversionException):
        sheets.col_to_char(-1)
    assert sheets.col_to_char(1) == 'A'
    assert sheets.col_to_char(26) == 'Z'
    assert sheets.col_to_char(27) == 'AA'
    assert sheets.col_to_char(53) == 'BA'

def test_parse_int():
    assert sheets.parse_int('') == 0
    assert sheets.parse_int('2') == 2
    assert sheets.parse_int(5) == 5

def test_parse_float():
    assert sheets.parse_float('') == 0.0
    assert sheets.parse_float('2') == 2.0
    assert sheets.parse_float(0.5) == 0.5

def test_system_result_dict():
    data = ['', 1, 4910, 0, 4322, 4910, 0, 116.99, '', 'Frey']
    result = sheets.system_result_dict(data, 0, 6)
    assert result['undermine'] == 0.0
    assert result['trigger'] == 4910
    assert result['cmdr_merits'] == 4322
    assert result['fort_status'] == 4910
    assert result['notes'] == ''
    assert result['name'] == 'Frey'
    assert result['sheet_col'] == 'F'
    assert result['sheet_order'] == 0
