"""
Tests for cogdb.eddn_log
"""
import pathlib
import shutil
import tempfile

import pytest

import cogdb.eddn_log as elog


def test_log_fname():
    assert elog.log_fname(FAKE_MSG) == "journal_1_TIMESTAMP_EDDI"
    assert elog.log_fname(FAKE_MSG_NOTSTAMP) == "journal_1_2023_09_18T17_18_32.061027Z_EDDI"


def test_eddnlog_initialize():
    tempd = tempfile.mkdtemp()
    try:
        shutil.rmtree(tempd)
        elog.EDDNLogger(folder=tempd)
        assert pathlib.Path(str(tempd)).exists()
    finally:
        shutil.rmtree(tempd)


def test_eddnlog_initialize_reset():
    tempd = tempfile.mkdtemp()
    try:
        test_file = pathlib.Path(str(tempd)) / "test.txt"
        with open(test_file, 'w', encoding='utf-8') as fout:
            fout.write("test")
        assert test_file.exists()
        elog.EDDNLogger(folder=tempd, reset=True)
        assert test_file.parent.exists()
        assert not test_file.exists()
    finally:
        shutil.rmtree(tempd)


def test_eddnlog_initialize_errors():
    with pytest.raises(OSError):
        elog.EDDNLogger(folder='/', reset=False)

    with pytest.raises(OSError):
        elog.EDDNLogger(folder='/', reset=True)


def test_eddnlog_write_msg():
    tempd = tempfile.mkdtemp()
    try:
        log = elog.EDDNLogger(folder=tempd)
        fname = pathlib.Path(log.write_msg(FAKE_MSG))
        with fname.open('r', encoding='utf-8') as fin:
            assert '2ea4d754d4b7da2deeb058b2640a3cfba25e3926' in fin.read()
        assert log.count == 1

        fname = pathlib.Path(log.write_msg(FAKE_MSG))
        with fname.open('r', encoding='utf-8') as fin:
            assert '2ea4d754d4b7da2deeb058b2640a3cfba25e3926' in fin.read()
        assert log.count == 2
    finally:
        shutil.rmtree(tempd)


def test_eddnlog_keep_n():
    tempd = pathlib.Path(tempfile.mkdtemp())
    try:
        log = elog.EDDNLogger(folder=tempd, keep_n=2)
        fname = pathlib.Path(log.write_msg(FAKE_MSG))
        with fname.open('r', encoding='utf-8') as fin:
            assert '2ea4d754d4b7da2deeb058b2640a3cfba25e3926' in fin.read()
        assert log.count == 1

        fname = pathlib.Path(log.write_msg(FAKE_MSG_NOTSTAMP))
        with fname.open('r', encoding='utf-8') as fin:
            assert '2ea4d754d4b7da2deeb058b2640a3cfba25e3926' in fin.read()
        assert log.count == 0

        fname = pathlib.Path(log.write_msg(FAKE_MSG_NOTSTAMP))
        with fname.open('r', encoding='utf-8') as fin:
            assert '2ea4d754d4b7da2deeb058b2640a3cfba25e3926' in fin.read()
        assert log.count == 1

        expected = sorted([
            pathlib.Path(f'{tempd}/000_journal_1_2023_09_18T17_18_32.061027Z_EDDI'),
            pathlib.Path(f'{tempd}/001_journal_1_2023_09_18T17_18_32.061027Z_EDDI'),
        ])
        assert sorted(list(tempd.glob('*'))) == expected
    finally:
        shutil.rmtree(tempd)


FAKE_MSG = {
    '$schemaRef': 'https://eddn.edcd.io/schemas/journal/1',
    'header': {
        'gamebuild': 'r295971/r0 ',
        'gameversion': '4.0.0.1601',
        'gatewayTimestamp': '2023-09-18T17:18:32.061027Z',
        'softwareName': 'EDDI',
        'softwareVersion': '4.0.3',
        'uploaderID': '2ea4d754d4b7da2deeb058b2640a3cfba25e3926'
    },
    'message': {
        'timestamp': "TIMESTAMP"
    },
}

FAKE_MSG_NOTSTAMP = {
    '$schemaRef': 'https://eddn.edcd.io/schemas/journal/1',
    'header': {
        'gamebuild': 'r295971/r0 ',
        'gameversion': '4.0.0.1601',
        'gatewayTimestamp': '2023-09-18T17:18:32.061027Z',
        'softwareName': 'EDDI',
        'softwareVersion': '4.0.3',
        'uploaderID': '2ea4d754d4b7da2deeb058b2640a3cfba25e3926'
    },
    'message': {
        'timestamp2': "TIMESTAMP"
    },
}
