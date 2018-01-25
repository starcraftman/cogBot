"""
This module allows the background execution of jobs outside the async main.
This module exists because Google Sheets API is not very reliable
and more importantly synchronously blocks the main.
Therefore, all sync operations will be done in a separate process and timed out
if they fail/block/take too long.
Jobs can register callbacks for failure and success, they are mutually exclusive.

Library Documentation:
    https://pythonhosted.org/Pebble/

Important Notes
---------------
Any code executed inside the Job must support pickling (i.e. pickle.dumps/loads).
This caveat does NOT affect callbacks, they are executed in the main and should be short.
Examples of things that won't pickle:
  - local scoped funcs
  - weakref dicts
  - sqlalchemy objects: the raw data is accesible but the session is invalid and relationships
                        will no longer work.
"""
# TODO: Cleanup module, not quite happy with it.
# TODO: Add lots of unit tests.
from __future__ import absolute_import, print_function
import asyncio
import concurrent.futures
import logging
import time

import pebble
import pebble.concurrent

import cog.exc


LIVE_JOBS = []
MAX_WORKERS = 15
POOL = pebble.ProcessPool(max_workers=MAX_WORKERS)
RUN = True
TIME_FMT = "%d/%m %H:%M:%S"


class Job(object):
    """
    Represents a function executing in a separatee process.

    This worker process can be cancelled (signal kill) or be timed out.
    Most importantly, it is totally isolated from main async process.
    There is no communication once started. It is assumed it has all it needs to complete.
    """
    def __init__(self, func, *, ident=None, attempts=3, timeout=15):
        self.func = func
        self.ident = ident if ident else "Job " + time.strftime(TIME_FMT, time.gmtime())
        self.future = None
        self.attempts = attempts
        self.timeout = timeout
        self.start_time = time.time()
        self.done_cbs = []
        self.fail_cbs = []

    def __repr__(self):
        keys = ['func', 'ident', 'future', 'attempts', 'timeout']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]
        kwargs += ['{}={!r}'.format('start_time',
                                    time.strftime(TIME_FMT,
                                                  time.gmtime(getattr(self, 'start_time'))))]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        return repr(self)

    @property
    def is_running(self):
        """ Only useful for general indication. """
        try:
            return self.future.running()
        except AttributeError:
            return False

    def set_ident_from_msg(self, msg, part=None):
        """
        Create an identifiable string to represent this job.
        If this job is part of more than one step, use 'part' to add further
        information about what the job does.
        """
        self.ident = "{} {} {}{}".format(msg.timestamp, msg.author, msg.content,
                                         ' ' + part if part else '')

    def start(self):
        """
        Schedule the function for execution on the process.
        On every start decrement the attempts by 1.

        Raises:
            FailedJob - Attempted to start a job with no more attempts.
        """
        # TODO: Do I need some timeout backoff here? Good question.
        if self.attempts < 1:
            raise cog.exc.FailedJob('Failed job: ' + self.ident)

        self.attempts -= 1
        self.start_time = time.time()
        self.future = POOL.schedule(self.func)
        logging.getLogger('cog.jobs').info('Scheduling %s, %d attempts left. Timeout: %ds',
                                           self.ident, self.attempts, self.timeout)

    def check_timeout(self):
        """
        If timeout is reached cancel the process and then reschedule.

        Raises:
            FailedJob - Attempted to start a job with no more attempts.
        """
        if (time.time() - self.start_time) > self.timeout:
            logging.getLogger('cog.jobs').warning('Timeout after %d for %s, %d attempts left',
                                                  self.timeout, self.ident, self.attempts)
            self.future.cancel()
            self.start()

    def finish(self):
        """
        Trigger finished callbacks.
        """
        try:
            for callback in self.done_cbs:
                callback(self.future.result(0.01))
        except Exception:
            for tup in self.fail_cbs:
                warn_user(tup[0], tup[1], self)

    def add_done_callback(self, callback):
        """
        Will only be called if the Job suceeds.
        Hint: Use functools.partial

        Callback form: callback(result)
        """
        self.done_cbs.append(callback)

    def add_fail_callback(self, tup):
        """
        Will only be called if the job raises an exception. Since it didn't finish, no result.
        Hint: Use functools.partial

        Callback form: callback()
        """
        self.fail_cbs.append(tup)


async def pool_monitor_task(delay=2):
    """
    Simply regularly check in progress jobs for timeout.
    Reschedule on timeout until no attempts left.
    On sucess or failure (no more attempts) trigger job callbacks.

    Args:
        delay: Poll running jobs on this delay (seconds).
    """
    print("Pool monitor task running with delay:", delay)
    log = logging.getLogger('cog.jobs')
    global POOL

    while RUN:
        if LIVE_JOBS:
            log.info('POOL - %s', str(LIVE_JOBS))

            num_jobs = len(LIVE_JOBS)
            if num_jobs >= MAX_WORKERS:
                log.warning("Max workers reached. Currently %d/%d", num_jobs, MAX_WORKERS)

        try:
            POOL.schedule(do_nothing)
        except RuntimeError:  # Pool is in error, restart it
            POOL.close()
            POOL.stop()
            POOL.join()
            POOL = pebble.ProcessPool(max_workers=MAX_WORKERS)

        for job in LIVE_JOBS:
            try:
                job.future.result(0.01)  # Force raising exception
                if job.future.done():
                    log.info('Job Finished %s, time taken: %s',
                             job.ident, time.time() - job.start_time)
                    LIVE_JOBS.remove(job)
                    job.finish()
            except concurrent.futures.CancelledError:  # Job got cancelled
                LIVE_JOBS.remove(job)
            except Exception as exc:  # If amy exception, it failed, try again.
                if not isinstance(exc, concurrent.futures.TimeoutError):
                    msg = "Exception raised during process execution!"
                    log.exception(msg + "\n" + str(exc))
                try:
                    job.check_timeout()
                except cog.exc.FailedJob:
                    LIVE_JOBS.remove(job)
                    job.finish()

        await asyncio.sleep(delay)


async def background_start(job):
    """
    Start a job and add it to the list of live jobs.
    If the pool isn't working, back off until monitor task restores it.

    N.B. Due to async either this or monitor working. Never both.

    Raises:
        RuntimeError - The pool failed critically and could not be restarted.
    """
    log = logging.getLogger('cog.jobs')
    retries = 3
    while retries:
        try:
            log .info('Attempts left %d, starting job %s', retries, str(job))
            job.start()
            break
        except RuntimeError:
            await asyncio.sleep(5)
            retries -= 1
            if not retries:
                raise cog.exc.FailedJob('Failed job: ' + job.ident)

    LIVE_JOBS.append(job)


def warn_user(bot, msg, job):
    """
    Standard user warning if a background job fails after returning to user.
    """
    response = """WARNING {}, execution/synchronization failed for part/all of your command:
        {}
    Job Ident: {}
Please manually check any relevant sheet is up to date. I'm sorry :frowning:

If this appears repeatedly on all commands alert @GearsandCogs
""".format(msg.author.mention, msg.content, job.ident)
    return asyncio.ensure_future(bot.send_message(msg.channel, response))


def do_nothing():
    """
    Schedule this to test pool liveness.
    """
    pass
