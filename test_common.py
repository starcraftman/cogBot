from __future__ import absolute_import, print_function

import pytest

import common

def test_line_format_simple():
    data = ['a phrase', 3344, 5553, 'another phrase']
    expect = 'a phrase | 3344 | 5553 | another phrase'
    assert common.line_format(data) == expect

def test_line_format_separator():
    data = ['a phrase', 3344, 5553, 'another phrase']
    expect = 'a phrase$$3344$$5553$$another phrase'
    assert common.line_format(data, sep='$$') == expect

def test_line_format_pad_same():
    data = ['a phrase', 3344, 5553, 'another phrase']
    pads = [10, 10, 10, 10]
    expect = 'a phrase   | 3344       | 5553       | another phrase'
    assert common.line_format(data, pads=pads) == expect

def test_line_format_pad_different():
    data = ['a phrase', 3344, 5553, 'another phrase']
    pads = [15, 7, 7, 20]
    expect = 'a phrase        | 3344    | 5553    | another phrase'
    assert common.line_format(data, pads=pads) == expect

def test_line_format_pad_center():
    data = ['a phrase', 3344, 5553, 'another phrase']
    pads = [15, 7, 7, 20]
    expect = '   a phrase     |  3344   |  5553   |    another phrase'
    assert common.line_format(data, pads=pads, center=True) == expect

def test_table_format():
    lines = [
        ['Name', 'Number 1', 'Num2', 'Notes'],
        ['John', 5831, 5, 'He is good.'],
        ['Fareed', 23752, 322, 'Likes smoking.'],
        ['Rosalie', 34, 7320, 'Bit lazy.'],
    ]
    expect = """Name    | Number 1 | Num2 | Notes
John    | 5831     | 5    | He is good.
Fareed  | 23752    | 322  | Likes smoking.
Rosalie | 34       | 7320 | Bit lazy."""
    assert common.table_format(lines) == expect

def test_table_format_separator():
    lines = [
        ['Name', 'Number 1', 'Num2', 'Notes'],
        ['John', 5831, 5, 'He is good.'],
        ['Fareed', 23752, 322, 'Likes smoking.'],
        ['Rosalie', 34, 7320, 'Bit lazy.'],
    ]
    expect = """Name   !Number 1!Num2!Notes
John   !5831    !5   !He is good.
Fareed !23752   !322 !Likes smoking.
Rosalie!34      !7320!Bit lazy."""
    assert common.table_format(lines, sep='!') == expect
