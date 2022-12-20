"""
Tests for pvp.journal_parser
"""
import os
import pathlib as pat
import json

import pytest

import cog.util
import pvp.journal_parser

JOURNAL_PATH = os.path.join(cog.util.ROOT_DIR, 'tests', 'pvp', 'player_journal.jsonl')


@pytest.fixture
def f_pjournal():
    with open(JOURNAL_PATH, 'r', encoding='utf-8') as fin:
        yield fin.read().split('\n')[1:]


def test_load_journal_possible(f_pjournal):
    jsons = pvp.journal_parser.load_journal_possible(JOURNAL_PATH)
    assert jsons[-1]['event'] == 'PVPKill'
