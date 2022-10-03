"""
Test util the grab all module.
"""
import datetime
import os
import tempfile

import mock
import pytest

import cog.util
from tests.data import SYSTEMS, USERS


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


def test_number_increment():
    assert cog.util.number_increment('C149') == 'C150'
    assert cog.util.number_increment('Cycle 149') == 'Cycle 150'
    assert cog.util.number_increment('Cycle 149 never ends') == 'Cycle 150 never ends'

    with pytest.raises(ValueError):
        cog.util.number_increment('Cycle')


def test_rel_to_abs():
    expect = os.path.join(cog.util.ROOT_DIR, 'data', 'log.yml')
    assert cog.util.rel_to_abs('data', 'log.yml') == expect


def test_fuzzy_find():
    assert cog.util.fuzzy_find('Alex', USERS) == 'Alexander Astropath'

    with pytest.raises(cog.exc.MoreThanOneMatch):
        cog.util.fuzzy_find('ric', USERS)
    with pytest.raises(cog.exc.NoMatch):
        cog.util.fuzzy_find('zzzz', SYSTEMS)

    assert cog.util.fuzzy_find('WW p', SYSTEMS) == 'WW Piscis Austrini'
    with pytest.raises(cog.exc.MoreThanOneMatch):
        cog.util.fuzzy_find('LHS', SYSTEMS)
    assert cog.util.fuzzy_find('tun', SYSTEMS) == 'Tun'


def test_transpose_table():
    input_table = [
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

    assert cog.util.transpose_table(input_table) == expect


def test_pad_table_to_rectangle():
    input_table = [
        [0, 3],
        [4, 6, 7, 9, 8],
        [8],
        [4, 7],
    ]
    expect = [
        [0, 3, '', '', ''],
        [4, 6, 7, 9, 8],
        [8, '', '', '', ''],
        [4, 7, '', '', ''],
    ]

    assert cog.util.pad_table_to_rectangle(input_table) == expect


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


def test_camel_to_c():
    assert cog.util.camel_to_c("CamelCase") == "camel_case"


def test_generative_split():
    expected = [
        """This is the header
This is the 0th line of text to print.
This is the 1th line of text to print.
This is the 2th line of text to print.
This is the 3th line of text to print.
This is the 4th line of text to print.
This is the 5th line of text to print.
This is the 6th line of text to print.
This is the 7th line of text to print.
This is the 8th line of text to print.
This is the 9th line of text to print.
This is the 10th line of text to print.
This is the 11th line of text to print.
This is the 12th line of text to print.
This is the 13th line of text to print.
This is the 14th line of text to print.
This is the 15th line of text to print.
This is the 16th line of text to print.
This is the 17th line of text to print.
This is the 18th line of text to print.
This is the 19th line of text to print.
This is the 20th line of text to print.
This is the 21th line of text to print.
This is the 22th line of text to print.
This is the 23th line of text to print.
This is the 24th line of text to print.
This is the 25th line of text to print.
This is the 26th line of text to print.
This is the 27th line of text to print.
This is the 28th line of text to print.
This is the 29th line of text to print.
This is the 30th line of text to print.
This is the 31th line of text to print.
This is the 32th line of text to print.
This is the 33th line of text to print.
This is the 34th line of text to print.
This is the 35th line of text to print.
This is the 36th line of text to print.
This is the 37th line of text to print.
This is the 38th line of text to print.
This is the 39th line of text to print.
This is the 40th line of text to print.
This is the 41th line of text to print.
This is the 42th line of text to print.
This is the 43th line of text to print.
This is the 44th line of text to print.
This is the 45th line of text to print.
This is the 46th line of text to print.
This is the 47th line of text to print.""",
        """
This is the 48th line of text to print.
This is the 49th line of text to print.
This is the 50th line of text to print.
This is the 51th line of text to print.
This is the 52th line of text to print.
This is the 53th line of text to print.
This is the 54th line of text to print.
This is the 55th line of text to print.
This is the 56th line of text to print.
This is the 57th line of text to print.
This is the 58th line of text to print.
This is the 59th line of text to print.
This is the 60th line of text to print.
This is the 61th line of text to print.
This is the 62th line of text to print.
This is the 63th line of text to print.
This is the 64th line of text to print.
This is the 65th line of text to print.
This is the 66th line of text to print.
This is the 67th line of text to print.
This is the 68th line of text to print.
This is the 69th line of text to print.
This is the 70th line of text to print.
This is the 71th line of text to print.
This is the 72th line of text to print.
This is the 73th line of text to print.
This is the 74th line of text to print.
This is the 75th line of text to print.
This is the 76th line of text to print.
This is the 77th line of text to print.
This is the 78th line of text to print.
This is the 79th line of text to print.
This is the 80th line of text to print.
This is the 81th line of text to print.
This is the 82th line of text to print.
This is the 83th line of text to print.
This is the 84th line of text to print.
This is the 85th line of text to print.
This is the 86th line of text to print.
This is the 87th line of text to print.
This is the 88th line of text to print.
This is the 89th line of text to print.
This is the 90th line of text to print.
This is the 91th line of text to print.
This is the 92th line of text to print.
This is the 93th line of text to print.
This is the 94th line of text to print.
This is the 95th line of text to print.""",
        """
This is the 96th line of text to print.
This is the 97th line of text to print.
This is the 98th line of text to print.
This is the 99th line of text to print.""",
    ]
    msgs = cog.util.generative_split(list(range(0, 100)), lambda x: "This is the {}th line of text to print.".format(x), header="This is the header")
    assert msgs == expected
    for msg in msgs:
        assert len(msg) < cog.util.MSG_LIMIT


def test_merge_msgs_to_least():
    parts = ["1" * 50, "*" * 100, "$" * 750, "B" * 2000]
    results = cog.util.merge_msgs_to_least(parts, limit=900)
    assert len(results) == 2
    assert len(results[0]) == 900


def test_next_weekly_tick():
    now = datetime.datetime(2021, 8, 24, 16, 34, 39, 246075)
    tick = cog.util.next_weekly_tick(now)
    assert tick == datetime.datetime(2021, 8, 26, 7, 0)

    tick = cog.util.next_weekly_tick(now, -1)
    assert tick == datetime.datetime(2021, 8, 19, 7, 0)

    tick = cog.util.next_weekly_tick(now, 1)
    assert tick == datetime.datetime(2021, 9, 2, 7, 0)

    # Calculate tick just past hour
    tick = cog.util.next_weekly_tick(tick + datetime.timedelta(hours=1), 0)
    assert tick == datetime.datetime(2021, 9, 9, 7, 0)


def test_chunk_file():
    lines = [(str(x) + '\n').encode() for x in range(1, 5002)]

    with tempfile.NamedTemporaryFile() as tfile:
        tfile.writelines(lines)
        tfile.flush()
        assert os.path.exists(tfile.name)

        file_1 = tfile.name + '_000'
        file_2 = tfile.name + '_001'
        try:
            cog.util.chunk_file(tfile.name)
            with open(file_2, 'r') as fin:
                assert fin.read() == '[\n5001\n]'
        finally:
            for fname in [file_1, file_2]:
                try:
                    os.remove(fname)
                except OSError:
                    pass


@mock.patch('cog.util.datetime')
def test_current_cycle(mock_date):
    mock_date.datetime.utcnow.return_value = datetime.datetime(2021, 10, 10, 2, 43, 22, 828086)
    assert cog.util.current_cycle() == 332


def test_cycle_to_start():
    assert cog.util.cycle_to_start(334) == datetime.datetime(2021, 10, 21, 7, 0)


class UpdatedAtObj(cog.util.TimestampMixin):
    def __init__(self):
        self.updated_at = datetime.datetime(2021, 10, 21, 7, 0, tzinfo=datetime.timezone.utc).timestamp()


def test_timestampmixin_notz():
    actual = UpdatedAtObj()
    assert actual.utc_date.tzname() is None
    assert "2021-10-21 07:00:00" == str(actual.utc_date)


def test_timestampmixin_tz():
    actual = UpdatedAtObj()
    assert "UTC" == actual.utc_date_tz.tzname()
    assert "2021-10-21 07:00:00+00:00" == str(actual.utc_date_tz)
