"""
Test fortification logic
"""
from __future__ import absolute_import, print_function

import pytest

import fort

CSV_LINE = 'A long string,0,"3,447",299,"Another long string"'
CSV_TOKENS = ['A long string', '0', '3,447', '299', 'Another long string']


def test_CSVLineTokenizer_comma():
    tok = fort.CSVLineTokenizer('a,b')
    assert tok.next_token() == 'a'
    assert tok.next_token() == 'b'


def test_CSVLineTokenizer_quotes():
    tok = fort.CSVLineTokenizer('"3,333","4,444"')
    assert tok.next_token() == '3,333'
    assert tok.next_token() == '4,444'


def test_CSVLineTokenizer_mixed():
    tok = fort.CSVLineTokenizer(CSV_LINE)
    cnt = 0
    while tok.has_more_tokens():
        assert tok.next_token() == CSV_TOKENS[cnt]
        cnt += 1


def test_CSVLineTokenizer_exception():
    tok = fort.CSVLineTokenizer('')
    with pytest.raises(fort.NoMoreTokens):
        tok.next_token()


def test_CSVLineTokenizer_more_tokens():
    tok = fort.CSVLineTokenizer('a')
    assert tok.has_more_tokens()
    tok.next_token()
    assert not tok.has_more_tokens()


def test_tokenize_line():
    assert fort.tokenize(CSV_LINE) == CSV_TOKENS


def test_tokenize_start():
    assert fort.tokenize(CSV_LINE, 2) == CSV_TOKENS[2:]


def test_tokenize_start_end():
    assert fort.tokenize(CSV_LINE, 2, 3) == CSV_TOKENS[2:3]


def test_parse_int():
    assert fort.parse_int('34%') == 34
    assert fort.parse_int('3,447,299') == 3447299
