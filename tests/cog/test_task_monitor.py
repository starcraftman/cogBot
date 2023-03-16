"""
Tests for cog.task_monitor
"""
import asyncio
import functools

import pytest

import cog.task_monitor


async def func_info(name):
    """ Stop fake task to run and stop. """
    await asyncio.sleep(0.2)
    return f"Info {name}"


async def make_info():
    func = functools.partial(func_info, 'FuncCoro')
    info = {
        'func': func,
        'task': asyncio.create_task(func()),
        'name': 'SampleInfo',
        'description': 'Sample description of info.',
    }

    return info


@pytest.mark.asyncio
async def test_summarize_info_running():
    info = await make_info()
    try:
        expect = ['SampleInfo', 'Running', 'Sample description of info.']
        assert expect == cog.task_monitor.summarize_info(info)
    finally:
        await info['task']


@pytest.mark.asyncio
async def test_summarize_info_stopped():
    info = await make_info()
    try:
        expect = ['SampleInfo', 'Stopped', 'Sample description of info.']
        await info['task']
        assert expect == cog.task_monitor.summarize_info(info)
    finally:
        await info['task']


@pytest.mark.asyncio
async def test_task_monitor__str__():
    mon = cog.task_monitor.TaskMonitor()
    try:
        mon.add_task(functools.partial(func_info, 'FuncCoro'), name='InfoName', description='InfoDescription')
        expect = """Name     | Status  | Description
InfoName | Running | InfoDescription"""
        assert expect == str(mon)
    finally:
        await mon.tasks['InfoName']['task']


@pytest.mark.asyncio
async def test_task_monitor_add():
    mon = cog.task_monitor.TaskMonitor()
    try:
        mon.add_task(functools.partial(func_info, 'FuncCoro'), name='InfoName', description='InfoDescription')

        assert mon.tasks
        assert mon.tasks['InfoName']['name'] == 'InfoName'
        assert not mon.tasks['InfoName']['task'].done()
    finally:
        await mon.tasks['InfoName']['task']


@pytest.mark.asyncio
async def test_task_monitor_restart():
    mon = cog.task_monitor.TaskMonitor()
    try:
        mon.add_task(functools.partial(func_info, 'FuncCoro'), name='InfoName', description='InfoDescription')
        assert mon.tasks
        old_task = await mon.restart_task(name='InfoName')
        await asyncio.sleep(0.1)

        assert old_task.done()
        assert not mon.tasks['InfoName']['task'].done()
    finally:
        await mon.tasks['InfoName']['task']


@pytest.mark.asyncio
async def test_task_monitor_table_cells():
    mon = cog.task_monitor.TaskMonitor()
    try:
        mon.add_task(functools.partial(func_info, 'FuncCoro'), name='InfoName', description='InfoDescription')
        expect = [
            ['Name', 'Status', 'Description'],
            ['InfoName', 'Running', 'InfoDescription']
        ]
        assert expect == mon.table_cells()
    finally:
        await mon.tasks['InfoName']['task']


@pytest.mark.asyncio
async def test_task_monitor_format_table():
    mon = cog.task_monitor.TaskMonitor()
    try:
        mon.add_task(functools.partial(func_info, 'FuncCoro'), name='InfoName', description='InfoDescription')
        expect = """```  Name   | Status  |   Description
-------- | ------- | ---------------
InfoName | Running | InfoDescription```"""
        assert expect == mon.format_table(header=True, wrap_msgs=True)
    finally:
        await mon.tasks['InfoName']['task']
