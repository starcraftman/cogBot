"""
A simple task monitoring object.
Will take a list of objects and descriptions and keep track of their operation.
"""
import asyncio
import logging

import cog.tbl


class TaskMonitor():
    """
    A monitor to oversee tasks that should be running constantly.
    """
    def __init__(self):
        self.tasks = {}

    def __str__(self):
        return self.format_table()

    def add_task(self, func, *, description, name=None, key=None):
        """
        Add a task to the monitor.

        Args:
            func: A function that can be called to create a coroutine to run as a Task.
            description: The description of the task.
            name: The name of the task.
            key: The desired key within the tasks to use. By default, numerically increments from 0.

        Returns: TaskMonitor for daisy chaining.
        """
        if not key:
            key = len(self.tasks)

        self.tasks[key] = {
            'func': func,
            'task': asyncio.create_task(func()),
            'name': name if name else key,
            'description': description,
        }

        return self

    async def restart_task(self, *, key):
        """
        Cancel if needed and then restart the associated Task for a given coroutine.

        Args:
            key: Restart the task with this associated key.

        Raises:
            KeyError: Invalid key to the dictionary of tasks.

        Returns: The old asyncio.Task that was done or cancelled
        """
        info = self.tasks[key]
        task = info['task']
        if not task.done():
            task.cancel('New task will be created.')

        info['task'] = asyncio.create_task(info['func']())

        return task

    def format_table(self, *, header=False, wrap_msgs=False):
        """
        Create a table to embed in a discord message.

        Args:
            wrap_msgs: When True, wrap tables in markdown for discord. Default is False.

        Returns: A string representation of a table summarizing the monitor.
        """
        return cog.tbl.format_table(self.table_cells(), header=header, wrap_msgs=wrap_msgs)[0]

    def table_cells(self):
        """
        Create a list of cells that describe the state of all tasks and
        can be used to create a table.

        Returns: A list of lists of strings.
        """
        return [['Name', 'Status', 'Description']] + [summarize_info(info) for info in self.tasks.values()]


def summarize_info(info):
    """
    Summarize a single info entry within the larger TaskMonitor.tasks list.

    Returns: A list of cells, suitable for a table or reformatting.
    """
    log = logging.getLogger(__name__)
    status = 'Running'
    task = info['task']
    if task.done():
        status = 'Stopped'
        reason = 'Unknown cause'
        if task.cancelled():
            try:
                reason = f'Exception found: {task.exception()}'
            except (asyncio.CancelledError, asyncio.InvalidStateError) as exc:
                reason = f'Unexpected issue: {exc}'
        log.error("Unexpected stop of task %s, exception found: %s", info['name'], reason)

    return [
        f"{info['name']}",
        status,
        f"{info['description']}",
    ]


TASK_MON = TaskMonitor()
