"""
This module exists because Google Sheets API is not very reliable
and more importantly synchronously blocks the main process.
Therefore, all sync operations will be done in a separate process and timed out
if they fail/block/take too long. If they suceed execute any attached callbacks.

Important Notes
---------------
Any code executed inside the Job must support pickling (i.e. pickle.dumps/loads).
That means no local objects/funcs, no weakref dicts.
You can transmit Schema objects HOWEVER the session is invalid in other process.
"""
from __future__ import absolute_import, print_function
import asyncio
import concurrent.futures
# import functools
import logging
import time

import pebble
import pebble.concurrent


POOL = pebble.ProcessPool(max_workers=10, max_tasks=1)
QUE = asyncio.Queue()
RUN = True


class FailedJob(Exception):
    """ Raised internally, cannot restart this job. """
    pass


class Job(object):
    """
    Represents a function executing in a separatee process.

    This worker process can be cancelled (signal kill) or be timed out.
    Most importantly, it is totally isolated from main async process.
    There is no communication once started. It is assumed it has all it needs to complete.
    """
    def __init__(self, func, msg, *, attempts=3, timeout=8, step=2):
        self.step = step
        self.func = func
        self.msg = msg
        self.future = None
        self.attempts = attempts
        self.timeout = timeout
        self.start_time = time.time()
        self.cbs = []

    @property
    def name(self):
        """ Return a name based on message. """
        return "{} {} {}".format(self.msg.timestamp, self.msg.author, self.msg.content)

    def start(self):
        """
        Schedule the function for execution on the process.
        On every start double the timeout and decrement the attempts by 1.

        Raises:
            FailedJob - Attempted to start a job with no more attempts.
        """
        if self.attempts < 1:
            raise FailedJob('The job with func: {} failed.'.format(self.name))

        self.timeout += self.step
        self.attempts -= 1
        self.start_time = time.time()
        self.future = POOL.schedule(self.func, timeout=self.timeout)
        logging.getLogger('cog.jobs').info('Scheduling %s, %d attempts left. Timeout: %ds',
                                           self.name, self.attempts, self.timeout)

    def check_timeout(self):
        """
        If timeout is reached cancel the process and then reschedule.

        Raises:
            FailedJob - Attempted to start a job with no more attempts.
        """
        if (time.time() - self.start_time) > self.timeout:
            logging.getLogger('cog.jobs').warning('Timeout of %d for %s', self.timeout, self.name)
            self.future.cancel()
            self.start()

    def done(self):
        """
        Trigger finished callbacks.
        """
        for a_cb in self.cbs:
            a_cb()

    def register(self, callback):
        """
        Callbacks will only be triggered if the function completes sucessfully.
        Callbacks should take no arguments, use partial.
        """
        self.cbs.append(callback)


async def warn_user(callback, msg):
    """
    Simply send a message directly into original channel mentioning user.
    """
    response = """WARNING {}, I could NOT sync part of the following request to sheet:
        {}
    I STRONGLY advise you to update the sheet manually to correct this.

This may be a temporary failure or it may indicate a more serious issue.
If this appears repeatedly on all commands alert @GearsandCogs
""".format(msg.author.mention, msg.content)
    await callback(msg.channel, response)


async def pool_monitor(live_jobs, fail_cb, delay):
    """
    Simply regularly check in progress jobs for timeout.
    Reschedule on timeout until no attempts left.
    ON FAIL: Emit a callback to dev channel.
    ON SUCESS: Execute any callbacks registered. Callbacks execute in main process.

    Args:
        live_jobs: Shared list of all jobs.
        delay: Poll running jobs on this delay.
    """
    log = logging.getLogger('cog.jobs')

    while RUN:
        for job in live_jobs:
            try:
                job.future.result(0.01)  # Force raising exception
                if job.future.done():
                    log.info('Job Finished %s, time taken: %s',
                             job.name, time.time() - job.start_time)
                    live_jobs.remove(job)
                    job.done()
            except Exception as exc:  # If amy exception, it failed, try again.
                if not isinstance(exc, concurrent.futures.TimeoutError):
                    msg = "Exception raised during process execution!"
                    log.exception(msg + "\n" + str(exc))
                try:
                    job.check_timeout()
                except FailedJob:
                    live_jobs.remove(job)
                    asyncio.ensure_future(warn_user(fail_cb, job.msg))

        if live_jobs:
            log.info('POOL - %s', str(live_jobs))
        await asyncio.sleep(delay)


async def pool_starter(fail_cb, delay=2):
    """
    Start any and all jobs sent into the queue.
    This coroutine will also start the monitor task to check for timeouts.

    Args:
        delay: Poll the queue to start jobs on this delay.
        fail_cb: On failure, send a message to this. Expect to receive a str as arg.
    """
    live_jobs = []
    asyncio.ensure_future(pool_monitor(live_jobs, fail_cb, delay))

    while RUN:
        job = await QUE.get()
        live_jobs.append(job)
        job.start()
        QUE.task_done()
