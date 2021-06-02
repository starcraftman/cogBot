"""
Implements a very simple scheduler for updating the sheets when they change.

  - Uses rpc logic that wakes up scheduler on loop. Subscribes to POSTs.
  - Updater logic to schedule jobs with the executor pool.
  - Scheduler registers scanners and commands to block during update.
"""
import asyncio
import atexit
import concurrent.futures as cfut
import datetime
import functools
import logging
import os
import time

import aiozmq
import aiozmq.rpc

import cog.util

ADDR = 'tcp://127.0.0.1:{}'.format(cog.util.get_config('ports', 'zmq'))
POOL = cfut.ProcessPoolExecutor(max_workers=6)


class Scheduler(aiozmq.rpc.AttrHandler):
    """
    Schedule updates for the db and manage permitted commands.

    The scheduler uses a sliding window to interrupt previously scheduled scans
    if new activity detected in the hooked sheet.
    Both the job itself and future that delays start can be cancelled at any stage.
    """
    def __init__(self, *, delay=10):
        self.sub = None
        self.count = -1
        self.delay = delay  # Seconds of timeout before running actual update
        self.wrap_map = {}
        self.cmd_map = {}

    def __repr__(self):
        keys = ['count', 'delay', 'sub', 'wrap_map', 'cmd_map']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __str__(self):
        msg = "### Schedule ###\n\n\tDelay: {}".format(self.delay)
        msg += '__Wraps__\n'
        for wrap in self.wrap_map.values():
            msg += "\n\t{!r}\n".format(wrap)

        return msg

    async def wait_for(self, cmd, wait_cb=None):
        """
        Wait until the scanners for cmd finished.
        If command is not managed by scheduler silently return.
        """
        try:
            for wrap in self.cmd_map[cmd]:
                await wrap.scanner.lock.r_aquire(wait_cb)

            if wait_cb:
                await wait_cb.send_resume()
        except KeyError:
            pass

    async def unwait_for(self, cmd):
        """
        Release locks that were aquired waiting.
        If command is not managed by scheduler silently return.
        """
        try:
            for wrap in self.cmd_map[cmd]:
                await wrap.scanner.lock.r_release()
        except KeyError:
            pass

    def disabled(self, cmd):
        """ Check if a command is disabled due to scheduled update. """
        resp = False
        try:
            for wrap in self.cmd_map[cmd]:
                if wrap.future or wrap.job:
                    resp = True
        except KeyError:
            pass

        return resp

    def register(self, name, scanner, cmds):
        """
        Register scanner to be updated.
        """
        wrap = WrapScanner(name, scanner, cmds)
        self.wrap_map[name] = wrap
        for cmd in cmds:
            try:
                self.cmd_map[cmd] += [wrap]
            except KeyError:
                self.cmd_map[cmd] = [wrap]

    def schedule(self, name, delay=None):
        """
        Schedule a scanner to fetch latest sheet. If another scheduled, cancel it.
        Name is the name of the scanner in the dictionary, i.e. hudson_cattle

        Args:
            delay: Override the default delay for this scheduling.
        """
        try:
            wrap = self.wrap_map[name]
            if wrap.future:
                wrap.cancel()

            if not delay:
                delay = self.delay
            wrap.schedule(delay)
        except KeyError:
            pass

    def schedule_all(self, delay=None):
        """
        Schedule all scanners for update.

        Args:
            delay: Override the default delay for this scheduling.
        """
        if not delay:
            delay = self.delay

        for name in self.wrap_map:
            self.schedule(name, delay)

    @aiozmq.rpc.method
    def remote_func(self, scanner, timestamp):  # pragma: no cover
        """ Remote function to be executed. """
        self.count = (self.count + 1) % 1000
        logging.getLogger(__name__).info(
            'POST %d received: %s %s', self.count, scanner, timestamp)
        self.schedule(scanner)
        print('SCHEDULED: ', scanner, timestamp)

    def close(self):  # pragma: no cover
        """ Properly close pubsub connection on termination. """
        if self.sub and self.count != -1:
            self.sub.close()
            time.sleep(0.5)

    async def connect_sub(self):  # pragma: no cover
        """ Connect the zmq subscriber. """
        channel = 'POSTs'
        self.sub = await aiozmq.rpc.serve_pubsub(self, subscribe=channel,
                                                 connect=ADDR, log_exceptions=True)
        atexit.register(self.close)
        print("Scheduler Subscribed to: {} with tag '{}'".format(ADDR, channel))
        print(aiozmq.rpc.logger)


class WrapScanner():
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

    def cancel(self):
        """
        Cancel any scheduled start or running job.
        """
        try:
            self.future.cancel()
            logging.getLogger(__name__).warning("Cancelled delayed call for: %s", self.name)
        except AttributeError:
            pass

    def schedule(self, delay):
        """
        Handle both scheduling this new job and cancelling old one.
        """
        self.future = asyncio.ensure_future(delayed_update(delay, self))


async def delayed_update(delay, wrap):
    """
    Delayed update of the scanner.
    After delay seconds initialize a full reimport of the sheet.
    """
    log = logging.getLogger(__name__)
    log.info(
        "%s | Delaying start by %d seconds\n    Will run at: %s",
        wrap.name, delay, str(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=delay))
    )
    await asyncio.sleep(delay)

    log.info("%s | Starting delayed call", wrap.name)
    try:
        await wrap.scanner.lock.w_aquire()
        log.debug("%s | w lock get", wrap.name)
        await wrap.scanner.update_cells()
        log.debug("%s | update", wrap.name)

        wrap.job = POOL.submit(wrap.scanner.scheduler_run)
        wrap.job.add_done_callback(functools.partial(done_cb, wrap))
        wrap.future = None
        log.debug("%s | job made", wrap.name)

        while not wrap.job.done():
            await asyncio.sleep(0.5)

        log.debug('Scanner %s has finished', wrap.name)
        wrap.job = None
    finally:
        await wrap.scanner.lock.w_release()
    log.debug("%s | lock %s", wrap.name, wrap.scanner.lock)
    log.info("%s | Finished delayed call", wrap.name)


def done_cb(wrap, fut):  # pragma: no cover
    """
    Callback for the future that runs the scan.
    Partial the wrap in.
    Generally only exceptional case needs handling.
    """
    if not wrap.job.exception():
        return

    log = logging.getLogger(__name__)
    env = os.environ.get('COG_TOKEN', 'dev')
    chan_name = 'private_dev'
    if env in ['prod', 'live']:
        chan_name = 'private_{}'.format(env)
    chan = cog.util.BOT.get_channel_by_name(chan_name)
    try:
        to_mention = cog.util.BOT.get_member_by_substr('gears').mention
    except AttributeError:
        to_mention = "Gears"

    msg = "Sheet update for `{}` failed at {}. {} have a look!\nMost likely seeing this due to duplicate username in row 'B' of this sheet.\n\n{}".format(
        wrap.name, datetime.datetime.now(datetime.timezone.utc),
        to_mention, str(wrap.job.exception()),
    )
    log.error("Critical Worker Error: %s", msg)
    asyncio.create_task(cog.util.BOT.send_message(chan, msg))
