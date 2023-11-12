"""
Tests for cogdb.common
"""
import json
import pathlib
import shutil
import tempfile

import cogdb.common
import cogdb.spansh
from cogdb.eddb import SCommodityGroup
from cogdb.eddb import SettlementSecurity


def test_dump_objs_to_file():
    saved = cogdb.common.PRELOAD_DIR
    tempdir = tempfile.mkdtemp()
    fname = pathlib.Path(cogdb.common.PRELOAD_DIR) / 'SettlementSecurity.json'
    result = pathlib.Path(tempdir) / 'SettlementSecurity.json'
    try:
        with open(fname, 'r', encoding='utf-8') as fin:
            cogdb.common.PRELOAD_DIR = tempdir
            objs = [SettlementSecurity(**x) for x in json.load(fin)]
            cogdb.common.dump_dbobjs_to_file(cls=SettlementSecurity, db_objs=objs)
            assert result.exists()
    finally:
        shutil.rmtree(tempdir)
        cogdb.common.PRELOAD_DIR = saved


def test_dump_table_to_file(eddb_session):
    saved = cogdb.common.PRELOAD_DIR
    tempdir = tempfile.mkdtemp()
    result = pathlib.Path(tempdir) / 'SettlementSecurity.json'
    try:
        cogdb.common.PRELOAD_DIR = tempdir
        cogdb.common.dump_table_to_file(eddb_session, cls=SettlementSecurity)
        assert result.exists()
    finally:
        shutil.rmtree(tempdir)
        cogdb.common.PRELOAD_DIR = saved


def test_preload_table_from_file(eddb_session):
    saved = cogdb.common.PRELOAD_DIR
    tempdir = tempfile.mkdtemp()
    inname = pathlib.Path(cogdb.common.PRELOAD_DIR) / 'SettlementSecurity.json'
    outname = pathlib.Path(tempdir) / 'SettlementSecurity.json'
    try:
        cogdb.common.PRELOAD_DIR = tempdir
        with open(inname, 'r', encoding='utf-8') as fin, open(outname, 'w', encoding='utf-8') as fout:
            fout.write(fin.read())
        eddb_session.query(SettlementSecurity).delete()
        assert not eddb_session.query(SettlementSecurity).all()
        eddb_session.commit()
        cogdb.common.preload_table_from_file(eddb_session, cls=SettlementSecurity)
        assert eddb_session.query(SettlementSecurity).all()
    finally:
        shutil.rmtree(tempdir)
        cogdb.common.PRELOAD_DIR = saved


def test_bulk_insert_from_file(eddb_session):
    fake_id = 9999
    try:
        with tempfile.NamedTemporaryFile(mode='w') as tfile:
            tfile.write('[\n')
            tfile.write(str({'id': fake_id, 'name': 'NotPresent'}) + ',\n')
            tfile.write(']\n')
            tfile.flush()
            cogdb.common.bulk_insert_from_file(eddb_session, fname=tfile.name, cls=SCommodityGroup)
            assert eddb_session.query(SCommodityGroup).\
                filter(SCommodityGroup.id == fake_id).\
                one()
    finally:
        eddb_session.rollback()
        eddb_session.query(SCommodityGroup).\
            filter(SCommodityGroup.id == fake_id).\
            delete()


def test_single_insert_from_file(eddb_session):
    fake_id = 9999
    try:
        with tempfile.NamedTemporaryFile(mode='w') as tfile:
            tfile.write('[\n')
            tfile.write(str({'id': fake_id, 'name': 'NotPresent'}) + ',\n')
            tfile.write(']\n')
            tfile.flush()
            cogdb.common.single_insert_from_file(eddb_session, fname=tfile.name, cls=SCommodityGroup)
            assert eddb_session.query(SCommodityGroup).\
                filter(SCommodityGroup.id == fake_id).\
                one()
    finally:
        eddb_session.rollback()
        eddb_session.query(SCommodityGroup).\
            filter(SCommodityGroup.id == fake_id).\
            delete()
