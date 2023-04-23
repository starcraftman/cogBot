"""
Simple module for global configuration of this project.
- Storage in memory of current config.
- Notification of changes on disk trigger reloads.
- Allow easy dot notation and dictionary access to get parts of the config.
"""
import asyncio
import copy
import logging
import pathlib

import aiofiles
import aiofiles.os
from asyncinotify import Inotify, Mask
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


# This is the default values expected
CONFIG_DEFAULTS = {
    'constants': {
        'defer_missing': 650,
        'show_priority_x_hours_before_tick': 48,
        'max_drop': 1000,
        'scheduler_delay': 10,  # Seconds
        'ttl': 60,  # Seconds a time to live message remains posted
    },
    'channels': {
        'ops': 13,
        'snipe': 14,
    },
    'emojis': {
        '_no': "\u274C",
        '_yes': "\u2705",
        '_friendly': "\U0001F7E2",
        '_hostile': "\U0001F534",
    },
    'paths': {
        'donate': 'data/donate.txt',
        'log_conf': 'data/log.yml',
        'service_json': 'data/service_sheets.json',
    },
    'ports': {
        'sanic': 8000,
        'zmq': 9000,
    },
    # All these are secret, expected in configuration file. For information see development kit.
    'dbs': None,
    'discord': None,
    'inara': None,
    'pastebin': None,
    'scanners': None,
    'tests': None,
}


class Config():
    """
    Manage the global configuration for easy reading and writing.
    Features:
        - Dot notation OR normal dictionary access.
        - Defaults stored within config and overwritten by file loaded if present in file.
        - Updates to current configuration triggers writes to the file.
        - Changes to the configuration triggers a reload of the configuration.
        - Supports both sync and async read/write.
    """
    def __init__(self, fname, conf=None):
        self.fname = fname
        path = pathlib.Path(self.fname)
        self.lock = asyncio.Lock()
        if conf:
            self.conf = conf
        elif path.exists():
            self.read()
            self.last_write = path.stat().st_mtime
        else:
            self.conf = copy.deepcopy(CONFIG_DEFAULTS)

    def __repr__(self):
        keys = ['fname', 'last_write']
        kwargs = [f'{key}={getattr(self, key)!r}' for key in keys]

        return f'{self.__class__.__name__}({", ".join(kwargs)})'

    def __getattr__(self, key):
        """
        Allow dot object notation to look through the config.

        If the object found is an end point in the config, return it.
        If it is a dictionary, just return a config object wrapping it.
        If not found, return None.
        """
        found = None
        if self.conf and key in self.conf:
            if not isinstance(self.conf[key], dict):
                found = self.conf[key]
            else:
                found = Config(self.fname, self.conf[key])

        return found

    def __getitem__(self, key):
        """
        Allow access to the internal dictionary.
        """
        return self.conf[key]

    @property
    def unwrap(self):
        """
        To be used when the config isn't automatically unwrapped.
        It just returns the actual current config rather than the object.
        """
        return self.conf

    def update(self, *keys, value=None):
        """
        Update the configuration for a given set of keys to a new value.
        After modifying the configuration, write to file.

        Args:
            keys: The list of strings that are a key path down configuration.

        Kwargs:
            value: The new value to set for this key.
        """
        temp = self.conf
        for key in keys[:-1]:
            temp = temp[key]

        temp[keys[-1]] = value
        self.write()

    def read(self):
        """
        Load the config from the file.
        This will replace the existing configuration with ...

            The contents of CONFIG_DEFAULTS dictionary updated with the contents of the file load.
        """
        conf = copy.deepcopy(CONFIG_DEFAULTS)
        with open(self.fname, encoding='utf-8') as fin:
            loaded = yaml.load(fin, Loader=Loader)

            if loaded:
                conf.update(loaded)
                self.conf = conf

    def write(self):
        """
        Write the current config to the file.
        """
        with open(self.fname, 'w', encoding='utf-8') as fout:
            yaml.dump(self.conf, fout, Dumper=Dumper,
                      default_flow_style=False,
                      explicit_start=True,
                      explicit_end=True)

    async def aupdate(self, *keys, value=None):
        """
        Async version of update.
        """
        temp = self.conf
        for key in keys[:-1]:
            temp = temp[key]

        temp[keys[-1]] = value
        text = yaml.dump(self.conf, Dumper=Dumper,
                         default_flow_style=False,
                         explicit_start=True,
                         explicit_end=True)
        async with self.lock:
            async with aiofiles.open(self.fname, 'w') as fout:
                await fout.write(text)
            self.last_write = (await aiofiles.os.stat(self.fname)).st_mtime

    async def aread(self):
        """
        Async version of read.
        """
        conf = copy.deepcopy(CONFIG_DEFAULTS)
        async with self.lock:
            async with aiofiles.open(self.fname, 'r', encoding='utf-8') as fin:
                text = await fin.read()
                loaded = yaml.load(text, Loader=Loader)

                if loaded:
                    conf.update(loaded)
                    self.conf = conf

    async def awrite(self):
        """
        Async version of write.
        """
        text = yaml.dump(self.conf, Dumper=Dumper,
                         default_flow_style=False,
                         explicit_start=True,
                         explicit_end=True)
        async with self.lock:
            async with aiofiles.open(self.fname, 'w') as fout:
                await fout.write(text)
            self.last_write = (await aiofiles.os.stat(self.fname)).st_mtime

    async def monitor(self):  # pragma: no cover
        """
        This function returns a coroutine to be set on the main loop.
        This function will async block waiting for changes to the config.
        On change event, reload the configuration.
        """
        with Inotify() as inotify:
            inotify.add_watch(self.fname, Mask.MODIFY)
            async for _ in inotify:
                async with self.lock:
                    conf_stat = await aiofiles.os.stat(self.fname)
                if conf_stat.st_mtime > self.last_write:
                    self.last_write = conf_stat.st_mtime
                    logging.getLogger(__name__).info("CONF: External change detected, updating config.")
                    await self.aread()
