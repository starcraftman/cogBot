"""
Simple module for global configuration of this project.
- Storage in memory of current config.
- Notification of changes on disk trigger reloads.
- Allow easy dot notation and dictionary access to get parts of the config.
"""
import copy

import aiofiles
from asyncinotify import Inotify, Mask
import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


# This is the default values expected
CONFIG_DEFAULTS = {
    'carrier_channel': 13,
    'snipe_channel': 14,
    'defer_missing': 650,
    'hours_to_tick_priority': 36,
    'max_drop': 1000,
    'scheduler_delay': 10,
    'ttl': 60,
    'emojis': {
        '_no': "\u274C",
        '_yes': "\u2705",
        '_friendly': "\U0001F7E2",
        '_hostile': "\U0001F534",
    },
    'paths': {
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
        if conf:
            self.conf = conf
        else:
            self.conf = copy.deepcopy(CONFIG_DEFAULTS)

    def __repr__(self):
        keys = ['fname']
        kwargs = ['{}={!r}'.format(key, getattr(self, key)) for key in keys]

        return "{}({})".format(self.__class__.__name__, ', '.join(kwargs))

    def __getattr__(self, key):
        """
        Allow dot object notation to look through the config.

        If the object found is an end point in the config, return it.
        If it is a dictionary, just return a config object wrapping it.
        If not found, return None.
        """
        if self.conf and key in self.conf:
            if not isinstance(self.conf[key], dict):
                return self.conf[key]
            else:
                return Config(self.fname, self.conf[key])

        return None

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
        temp = None
        for key in keys[:-1]:
            temp = self.conf[key]

        temp[keys[-1]] = value
        self.write()

    def read(self):
        """
        Load the config.
        """
        self.conf = copy.deepcopy(CONFIG_DEFAULTS)
        with open(self.fname) as fin:
            loaded = yaml.load(fin, Loader=Loader)
        self.conf.update(loaded)

    def write(self):
        """
        Load the config.
        """
        with open(self.fname, 'w') as fout:
            yaml.dump(self.conf, fout, Dumper=Dumper,
                      default_flow_style=False,
                      explicit_start=True,
                      explicit_end=True)

    async def aupdate(self, *keys, value=None):
        """
        Async version of update.
        """
        temp = None
        for key in keys[:-1]:
            temp = self.conf[key]

        temp[keys[-1]] = value
        await self.awrite()

    async def aread(self):
        """
        Async version of read.
        """
        self.conf = copy.deepcopy(CONFIG_DEFAULTS)
        async with aiofiles.open(self.fname) as fin:
            loaded = yaml.load(await fin.read(), Loader=Loader)
        self.conf.update(loaded)

    async def awrite(self):
        """
        Async version of write.
        """
        text = yaml.dump(self.conf, Dumper=Dumper,
                         default_flow_style=False,
                         explicit_start=True,
                         explicit_end=True)
        async with aiofiles.open(self.fname, 'w') as fout:
            await fout.write(text)

    async def monitor(self):  # pragma: no cover
        """
        This function returns a coroutine to be set on the main loop.
        This function will async block waiting for changes to the config.
        On change event, reload the configuration.
        """
        with Inotify() as inotify:
            inotify.add_watch(self.fname, Mask.MODIFY)
            async for _ in inotify:
                self.read()
