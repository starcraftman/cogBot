"""
Implements a very simple scheduler for updating the sheets when they change.

  - Uses rpc logic that wakes up scheduler on loop. Subscribes to POSTs.
  - Updater logic to schedule and cancel updates. Uses cog.jobs for execution.
  - Scheduler registers scanners and commands to block during update.
"""
from __future__ import absolute_import, print_function
import asyncio
import atexit
import datetime
import functools
import logging
import time

import aiozmq
import aiozmq.rpc

import cog.jobs
import cog.util

POST_ADDR = "tcp://127.0.0.1:9000"


class Scheduler(aiozmq.rpc.AttrHandler):
    """
    Schedule updates for the db and manage permitted commands.

    The scheduler uses a sliding window to interrupt previously scheduled scans
    if new activity detected in the hooked sheet.
    Both the job itself and future that delays start can be cancelled at any stage.
    """
    def __init__(self, delay=20):
        self.sub = None
        self.count = -1
        self.delay = delay  # Seconds of timeout before running actual update
        self.wraps = {}

    def __repr__(self):
        keys = ['count', 'delay', 'sub', 'wraps']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        msg = "Delay: {}".format(self.delay)
        for wrap in self.wraps.values():
            msg += "\n{!r}".format(wrap)

    def disabled(self, cmd):
        """ Check if a command is disabled due to scheduled update. """
        for scanner in self.wraps.values():
            if scanner.is_scheduled and cmd in scanner.cmds:
                return True

        return False

    def register(self, name, scanner, cmds):
        """
        Register scanner to be updated.
        """
        self.wraps[name] = WrapScanner(name, scanner, cmds)

    def schedule(self, name):
        """
        Schedule a scanner to fetch latest sheet. If another scheduled, cancel it.

        Name is the name of the scanner in the dictionary, i.e. hudson_cattle
        """
        wrap = self.wraps[name]

        if wrap.is_scheduled:
            wrap.cancel()

        wrap.schedule(self.delay)

    def schedule_all(self):
        """ Schedule all wrappers for update. """
        for name in self.wraps:
            self.schedule(name)

    @aiozmq.rpc.method
    def remote_func(self, scanner, timestamp):
        """ Remote function to be executed. """
        self.count = (self.count + 1) % 1000
        logging.getLogger('cog.scheduler').info(
            'POST %d received: %s %s', self.count, scanner, timestamp)
        self.schedule(scanner)
        print('SCHEDULED: ', scanner, timestamp)

    def close(self):
        """ Properly close pubsub connection on termination. """
        if self.sub and self.count != -1:
            self.sub.close()
            time.sleep(0.5)

    async def connect_sub(self):
        """ Connect the zmq subscriber. """
        channel = 'POSTs'
        self.sub = await aiozmq.rpc.serve_pubsub(self, subscribe=channel,
                                                 bind=POST_ADDR, log_exceptions=True)
        atexit.register(self.close)
        print("Scheduler Subscribed to: {} with tag '{}'".format(POST_ADDR, channel))
        print(aiozmq.rpc.logger)


class WrapScanner(object):
    """
    Wrap a scanner with info about scheduling. Mainly a data class.
    """
    def __init__(self, name, scanner, cmds):
        self.name = name
        self.cmds = cmds
        self.scanner = scanner
        self.future = None
        self.job = None

    def __repr__(self):
        keys = ['name', 'scanner', 'cmds', 'future', 'job']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        return repr(self)

    @property
    def is_scheduled(self):
        """ A job is scheduled if either future or job set. """
        return self.future or self.job

    def cancel(self):
        """
        Cancel any scheduled start or running job.
        """
        log = logging.getLogger("cog.scheduler")
        try:
            self.future.cancel()
            log.warning("Cancelled delayed call for: %s", self.name)
        except AttributeError:
            pass
        try:
            job_str = str(self.job)
            self.job.future.cancel()
            log.warning("Cancelled old job: %s", job_str)
        except AttributeError:
            pass

    def schedule(self, delay):
        """
        Handle both scheduling this new job and cancelling old one.
        """
        self.job = cog.jobs.Job(self.scanner.scan, attempts=6, timeout=30)
        self.job.ident = "Scheduled update for " + self.name
        self.job.add_done_callback(functools.partial(scan_done_cb, self))
        # job.add_fail_callback(cog.jobs.warn_user_callback(bot, msg, job))

        self.future = asyncio.ensure_future(
            delay_call(delay, cog.jobs.background_start, self.job))
        expected = datetime.datetime.utcnow() + datetime.timedelta(seconds=delay)
        logging.getLogger('cog.scheduler').info(
            'Update for %s scheduled for %s', self.name, expected)


async def delay_call(delay, coro, *args, **kwargs):
    """ Simply delay then invoke a coroutine. """
    await asyncio.sleep(delay)
    await coro(*args, **kwargs)


def scan_done_cb(wrap, _):
    """ When finished reset wrap. """
    wrap.job = None
    wrap.future = None


def scan_fail_cb():
    """ If a scheduled update fails, notify hideout. """
    msg = "A scheduled sheet update failed. {} may have to look at me."
    asyncio.ensure_future(cog.util.BOT.send_message(
        cog.util.BOT.get_channel_by_name('private_dev'),
        msg.format(cog.util.BOT.get_member_by_substr("gearsandcogs"))))
