"""
Tests for cog.config
"""
import copy
import tempfile

import aiofiles
import pytest

import cog.config
import cog.util

CONF_PATH = cog.util.rel_to_abs('data', 'config.yml')


@pytest.fixture
def f_config(session):
    """
    Setup a fake Config object ready for testing.
    The filename has been replaced with a tempfile that has some values different than default.
    """
    with tempfile.NamedTemporaryFile() as tfile:
        test_data = copy.deepcopy(cog.config.CONFIG_DEFAULTS)
        test_data['defer_missing'] = 4 * cog.config.CONFIG_DEFAULTS['defer_missing']
        test_data['ttl'] = 2 * cog.config.CONFIG_DEFAULTS['ttl']
        with open(tfile.name, 'w') as fout:
            fout.write(str(test_data))

        yield cog.config.Config(tfile.name)


def test_config__repr__():
    conf = cog.config.Config('/tmp/conf.yml')
    expected = "Config(fname='/tmp/conf.yml')"
    assert repr(conf) == expected


def test_config__getattr(f_config):
    f_config.ports.zmq == cog.config.CONFIG_DEFAULTS['ports']['zmq']


def test_config__getitem__(f_config):
    f_config['ports']['zmq'] == cog.config.CONFIG_DEFAULTS['ports']['zmq']


def test_config_unwrap(f_config):
    assert f_config.ports.unwrap == cog.config.CONFIG_DEFAULTS['ports']


def test_config_read(f_config):
    assert f_config.ttl == cog.config.CONFIG_DEFAULTS['ttl']
    f_config.read()
    assert f_config.ttl == 120


def test_config_write(f_config):
    f_config.conf['ttl'] = 300
    f_config.write()

    with open(f_config.fname) as fin:
        assert "ttl: 300" in fin.read()


def test_config_update(f_config):
    assert f_config.ports.zmq == cog.config.CONFIG_DEFAULTS['ports']['zmq']
    f_config.update('ports', 'zmq', value=9)

    assert f_config.ports.zmq == 9
    with open(f_config.fname) as fin:
        assert "zmq: 9" in fin.read()


@pytest.mark.asyncio
async def test_config_aread(f_config):
    assert f_config.ttl == cog.config.CONFIG_DEFAULTS['ttl']
    await f_config.aread()
    assert f_config.ttl == 120


@pytest.mark.asyncio
async def test_config_awrite(f_config):
    f_config.conf['ttl'] = 300
    await f_config.awrite()

    async with aiofiles.open(f_config.fname) as fin:
        text = await fin.read()
        assert "ttl: 300" in text


@pytest.mark.asyncio
async def test_config_aupdate(f_config):
    assert f_config.ports.zmq == cog.config.CONFIG_DEFAULTS['ports']['zmq']
    await f_config.aupdate('ports', 'zmq', value=9)

    assert f_config.ports.zmq == 9
    async with aiofiles.open(f_config.fname) as fin:
        text = await fin.read()
        assert "zmq: 9" in text
