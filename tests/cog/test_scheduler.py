"""
Tests for cog.scheduler
"""
import asyncio
import pytest

import cog.scheduler
import cogdb.scanners
from cog.scheduler import (Scheduler, WrapScanner)


def test_scheduler_init():
    scd = Scheduler()

    assert not scd.sub
    assert scd.count == -1


def test_scheduler__repr__(f_asheet_fortscanner):
    fscan = cogdb.scanners.FortScanner(f_asheet_fortscanner)
    scd = Scheduler()
    scd.register('fort', fscan, ['Fort'])

    assert "Scheduler(count=-1, delay=10," in repr(scd)
    #  print(repr(scd))


def test_scheduler__str__(f_asheet_fortscanner):
    fscan = cogdb.scanners.FortScanner(f_asheet_fortscanner)
    scd = Scheduler()
    scd.register('fort', fscan, ['Fort'])

    assert "Delay: 10" in str(scd)
    #  print(scd)


@pytest.mark.asyncio
async def test_scheduler_wait_for(f_asheet_fortscanner):
    fscan = cogdb.scanners.FortScanner(f_asheet_fortscanner)
    scd = Scheduler()
    scd.register('fort', fscan, ['Fort'])

    await scd.wait_for('Fort')

    assert not fscan.lock.read_mut.locked()
    assert fscan.lock.resource_mut.locked()


@pytest.mark.asyncio
async def test_scheduler_wait_for_no_register(f_asheet_fortscanner):
    fscan = cogdb.scanners.FortScanner(f_asheet_fortscanner)
    scd = Scheduler()
    scd.register('fort', fscan, ['Fort'])

    await scd.wait_for('UM')

    assert not fscan.lock.read_mut.locked()
    assert not fscan.lock.resource_mut.locked()


@pytest.mark.asyncio
async def test_scheduler_unwait_for(f_asheet_fortscanner):
    fscan = cogdb.scanners.FortScanner(f_asheet_fortscanner)
    scd = Scheduler()
    scd.register('fort', fscan, ['Fort'])

    await scd.wait_for('Fort')
    await scd.unwait_for('Fort')

    assert not fscan.lock.read_mut.locked()
    assert not fscan.lock.resource_mut.locked()


@pytest.mark.asyncio
async def test_scheduler_unwait_for_no_register(f_asheet_fortscanner):
    fscan = cogdb.scanners.FortScanner(f_asheet_fortscanner)
    scd = Scheduler()
    scd.register('fort', fscan, ['Fort'])

    await scd.wait_for('UM')
    await scd.unwait_for('UM')

    assert not fscan.lock.read_mut.locked()
    assert not fscan.lock.resource_mut.locked()


def test_scheduler_register(f_asheet_fortscanner):
    fscan = cogdb.scanners.FortScanner(f_asheet_fortscanner)
    scd = Scheduler()
    scd.register('fort', fscan, ['Fort'])

    assert isinstance(scd.wrap_map['fort'], WrapScanner)
    assert isinstance(scd.cmd_map['Fort'][0], WrapScanner)


def test_scheduler_disabled(f_asheet_fortscanner):
    fscan = cogdb.scanners.FortScanner(f_asheet_fortscanner)
    scd = Scheduler()
    scd.register('fort', fscan, ['Fort'])

    assert not scd.disabled('Fort')
    assert not scd.disabled('UM')

    scd.wrap_map['fort'].future = 'fort'

    assert scd.disabled('Fort')


# Tests schedule too implicitly.
@pytest.mark.asyncio
async def test_scheduler_schedule_all(f_asheet_fortscanner, f_asheet_umscanner):
    async def test():
        await asyncio.sleep(20)

    fscan = cogdb.scanners.FortScanner(f_asheet_fortscanner)
    uscan = cogdb.scanners.UMScanner(f_asheet_umscanner)
    scd = Scheduler()
    scd.register('fort', fscan, ['Fort'])
    scd.register('um', uscan, ['UM'])

    fut = asyncio.ensure_future(test())
    for wrap in scd.wrap_map.values():
        wrap.future = fut
    scd.schedule_all()

    for scd in scd.wrap_map.values():
        assert scd.future != fut
        assert scd.future
        scd.future.cancel()


@pytest.mark.asyncio
async def test_scheduler_delayed_update(f_bot, f_asheet_fortscanner, db_cleanup):
    fscan = cogdb.scanners.FortScanner(f_asheet_fortscanner)
    wrap = WrapScanner('fort', fscan, ['Fort'])

    old_bot = cog.util.BOT
    try:
        cog.util.BOT = f_bot
        await cog.scheduler.delayed_update(1, wrap)
    finally:
        cog.util.BOT = old_bot

    assert not wrap.scanner.lock.read_mut.locked()
