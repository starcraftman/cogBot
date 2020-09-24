"""
Test util the grab all module.
"""
import os
import shutil
import tempfile

import pytest

import cog.util


def test_dict_to_columns():
    data = {
        'first': [1, 2, 3],
        'more': [100],
        'second': [10, 30, 50],
        'three': [22, 19, 26, 23],
    }
    expect = [
        ['first (3)', 'more (1)', 'second (3)', 'three (4)'],
        [1, 100, 10, 22],
        [2, '', 30, 19],
        [3, '', 50, 26],
        ['', '', '', 23]
    ]
    assert cog.util.dict_to_columns(data) == expect


def test_get_config():
    assert cog.util.get_config('paths', 'log_conf') == 'data/log.yml'


def test_get_config_default():
    assert cog.util.get_config('zzzzzz', default=100) == 100


def test_get_config_raises():
    with pytest.raises(KeyError):
        cog.util.get_config('zzzzzz')


def test_update_config():
    try:
        tfile = tempfile.mktemp()
        shutil.copyfile(cog.util.YAML_FILE, tfile)
        assert os.path.exists(tfile)

        cog.util.update_config(150, 'scanners', 'hudson_cattle', 'page')

        assert cog.util.get_config('scanners', 'hudson_cattle', 'page') == 150
        found = False
        for line in open(cog.util.YAML_FILE):
            if 'page: 150' in line:
                found = True

        assert found

    finally:
        shutil.copyfile(tfile, cog.util.YAML_FILE)


def test_number_increment():
    assert cog.util.number_increment('C149') == 'C150'
    assert cog.util.number_increment('Cycle 149') == 'Cycle 150'
    assert cog.util.number_increment('Cycle 149 never ends') == 'Cycle 150 never ends'

    with pytest.raises(ValueError):
        cog.util.number_increment('Cycle')


def test_rel_to_abs():
    expect = os.path.join(cog.util.ROOT_DIR, 'data', 'log.yml')
    assert cog.util.rel_to_abs('data', 'log.yml') == expect


def test_substr_ind():
    assert cog.util.substr_ind('ale', 'alex') == [0, 3]
    assert cog.util.substr_ind('ALEX', 'Alexander') == [0, 4]
    assert cog.util.substr_ind('nde', 'Alexander') == [5, 8]

    assert not cog.util.substr_ind('ALe', 'Alexander', ignore_case=False)
    assert not cog.util.substr_ind('not', 'alex')
    assert not cog.util.substr_ind('longneedle', 'alex')

    assert cog.util.substr_ind('16 cyg', '16 c y  gni') == [0, 9]


def test_substr_match():
    assert cog.util.substr_match('ale', 'alex')
    assert cog.util.substr_match('ALEX', 'Alexander')
    assert cog.util.substr_match('nde', 'Alexander')

    assert not cog.util.substr_match('ALe', 'Alexander', ignore_case=False)
    assert not cog.util.substr_match('not', 'alex')
    assert not cog.util.substr_match('longneedle', 'alex')

    assert cog.util.substr_ind('16 cyg', '16 c y  gni') == [0, 9]


def test_complete_block():
    test1 = ["```Test```"]
    assert cog.util.complete_blocks(test1) == test1

    test1 = ["```Test"]
    assert cog.util.complete_blocks(test1) == [test1[0] + "```"]

    test1 = ["```Test", "Test```"]
    assert cog.util.complete_blocks(test1) == [test1[0] + "```", "```" + test1[1]]

    test1 = ["```Test", "Test", "Test```"]
    assert cog.util.complete_blocks(test1) == ["```Test```", "```Test```", "```Test```"]


def test_msg_splitter():
    try:
        old_limit = cog.util.MSG_LIMIT
        cog.util.MSG_LIMIT = 50

        line = "A short message to"  # 19 char line, 20 with \n
        test1 = line + "\n" + line + "\n"
        assert cog.util.msg_splitter(test1) == [test1[:-1]]

        test2 = test1 + "stop here\n" + test1
        assert cog.util.msg_splitter(test2) == [test1 + "stop here", test1[:-1]]
    finally:
        cog.util.MSG_LIMIT = old_limit


def test_transpose_table():
    input = [
        [0, 1, 2, 3],
        [4, 5, 6, 7],
        [8, 9, 10, 11],
    ]
    expect = [
        [0, 4, 8],
        [1, 5, 9],
        [2, 6, 10],
        [3, 7, 11],
    ]

    assert cog.util.transpose_table(input) == expect


class NumObj():
    def __init__(self):
        self.num = 0

    async def inc(self):
        self.num += 1

    async def dec(self):
        self.num -= 1


@pytest.mark.asyncio
async def test_wait_cb_send_notice():
    obj = NumObj()

    wcb = cog.util.WaitCB(notice_cb=obj.inc, resume_cb=obj.dec)
    for _ in range(5):
        await wcb.send_notice()

    assert wcb.notice_sent
    assert obj.num == 1


@pytest.mark.asyncio
async def test_wait_cb_send_resume():
    obj = NumObj()

    wcb = cog.util.WaitCB(notice_cb=obj.inc, resume_cb=obj.dec)
    for _ in range(5):
        await wcb.send_notice()
    await wcb.send_resume()

    assert wcb.notice_sent
    assert obj.num == 0


@pytest.mark.asyncio
async def test_wait_cb_send_resume_no_send():
    obj = NumObj()

    wcb = cog.util.WaitCB(notice_cb=obj.inc, resume_cb=obj.dec)
    await wcb.send_resume()

    assert not wcb.notice_sent
    assert obj.num == 0


def test_clean_text():
    assert cog.util.clean_text(r'///---351Test+*;;:,.') == '_351Test_'
    assert cog.util.clean_text(r'///---351Test+*;;:,.', replace='/') == '/351Test/'


def test_shorten_text():
    assert cog.util.shorten_text("Dobrovolskiy Enterprise", 20) == "Dobrovolskiy Enterp."
    assert cog.util.shorten_text("Galileo", 20) == "Galileo"
