"""
Tests for cogdb.scrape
"""
import datetime
import os

import pytest

import cog.util
import cogdb.scrape


HELD_UNKNOWN = """
<div class="powerplay-result-box"><!--!--><!--!--><div class="accordion-item"><h2 class="accordion-header"><button type="button" aria-expanded="true" data-bs-toggle="collapse" class="accordion-button " _bl_20634afa-abeb-4fd8-811c-ffb4e8e50b71=""><div style="display: table;"><input type="checkbox" class="checkmark"><!--!-->
                    <p style="display: table-cell; vertical-align: middle; padding-left: 1em;">Aowica</p></div></button></h2><!--!-->
<div class="accordion-collapse collapse show" data-blazorstrap="434678de-c20e-4f09-8230-7ef4ac4ce688" _bl_8f7632f8-0a9c-48f1-983f-afdcce3f8793="" style=""><div class="accordion-body"><!--!--><!--!-->Fortification: 4464 / 4247<br>Undermining: 0 / 11598<br><br>Held Merits: unknown.<!--!-->
"""
HELD_RECENT = """
<div class="powerplay-result-box"><!--!--><!--!--><div class="accordion-item"><h2 class="accordion-header"><button type="button" aria-expanded="true" data-bs-toggle="collapse" class="accordion-button " _bl_20634afa-abeb-4fd8-811c-ffb4e8e50b71=""><div style="display: table;"><input type="checkbox" class="checkmark"><!--!-->
                    <p style="display: table-cell; vertical-align: middle; padding-left: 1em;">Aowica</p></div></button></h2><!--!-->
<div class="accordion-collapse collapse show" data-blazorstrap="434678de-c20e-4f09-8230-7ef4ac4ce688" _bl_8f7632f8-0a9c-48f1-983f-afdcce3f8793="" style=""><div class="accordion-body"><!--!--><!--!-->Fortification: 4464 / 4247<br>Undermining: 0 / 11598<br><br>Held Merits: 0 stolen and 0 held (+ 0 = 0 / 11598) as of 11 minutes, 33 seconds ago.<!--!-->
"""
HELD_OLD = """
<div class="powerplay-result-box"><!--!--><!--!--><div class="accordion-item"><h2 class="accordion-header"><button type="button" aria-expanded="true" data-bs-toggle="collapse" class="accordion-button " _bl_20634afa-abeb-4fd8-811c-ffb4e8e50b71=""><div style="display: table;"><input type="checkbox" class="checkmark"><!--!-->
                    <p style="display: table-cell; vertical-align: middle; padding-left: 1em;">Aowica</p></div></button></h2><!--!-->
<div class="accordion-collapse collapse show" data-blazorstrap="434678de-c20e-4f09-8230-7ef4ac4ce688" _bl_8f7632f8-0a9c-48f1-983f-afdcce3f8793="" style=""><div class="accordion-body"><!--!--><!--!-->Fortification: 4464 / 4247<br>Undermining: 0 / 11598<br><br>Held Merits: 0 stolen and 0 held (+ 0 = 0 / 11598) as of 1 hours, 11 minutes, 33 seconds ago.<!--!-->
"""
with open(os.path.join(cog.util.ROOT_DIR, 'tests', 'cogdb', 'whole_page.html')) as fin:
    WHOLE_PAGE = fin.read()


@pytest.fixture
def now():
    yield datetime.datetime.utcfromtimestamp(1662490449)


def test_powerplay_names():
    names = cogdb.scrape.powerplay_names()
    assert len(names) == 11
    assert "Zachary Hudson" in names


def test_parse_date_unknown(now):
    expect = 0
    text = "Held Merits: unknown"

    assert expect == cogdb.scrape.parse_date(text, start=now)


def test_parse_date_all(now):
    expect = "2022-09-05 15:23:31"
    text = "Held Merits: 0 stolen and 0 held (+ 0 = 0 / 11598) as of 1 days, 3 hours, 30 minutes, 38 seconds ago."

    assert expect == str(cogdb.scrape.parse_date(text, start=now))


def test_parse_date_partial(now):
    expect = "2022-09-05 18:24:09"
    text = "Held Merits: 0 stolen and 0 held (+ 0 = 0 / 11598) as of 1 days, 30 minutes ago."

    assert expect == str(cogdb.scrape.parse_date(text, start=now))


def test_check_held_recent_unknown(now):
    assert not cogdb.scrape.check_held_recent(HELD_UNKNOWN, start=now)


def test_check_held_recent_is_old(now):
    assert not cogdb.scrape.check_held_recent(HELD_OLD, start=now)


def test_check_held_recent_is_recent(now):
    assert cogdb.scrape.check_held_recent(HELD_RECENT, start=now)


def test_parse_powerplay_page(now):
    expect_held = {
        'fort': 4464,
        'fort_trigger': 4247,
        'held': 0,
        'held_date': 1662504156,
        'name': 'Aowica',
        'redeemed': 0,
        'um': 0,
        'um_trigger': 11598
    }
    expect_unknown = {
        'fort': 2976,
        'fort_trigger': 2842,
        'held': 0,
        'held_date': 0,
        'name': 'Zhao',
        'redeemed': 0,
        'um': 0,
        'um_trigger': 20067
    }

    parsed = cogdb.scrape.parse_powerplay_page(WHOLE_PAGE, start=now)
    assert expect_held == parsed['Aowica']
    assert expect_unknown == parsed['Zhao']
