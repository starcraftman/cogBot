"""
Utility functions, mainly matching now.
"""
from __future__ import absolute_import, print_function
import logging
import logging.handlers
import logging.config
import math
import os
import urllib.parse as urlparse
from urllib.parse import urlencode

try:
    import simplejson as json
except ImportError:
    import json
import aiohttp
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import cog.exc

EDSM = 'https://www.edsm.net/api-v1/systems'
MSG_LIMIT = 1985  # Number chars before message truncation


class ModFormatter(logging.Formatter):
    """
    Add a relmod key to record dict.
    This key tracks a module relative this project' root.
    """
    def format(self, record):
        relmod = record.__dict__['pathname'].replace(ROOT_DIR + os.path.sep, '')
        record.__dict__['relmod'] = relmod[:-3]
        return super().format(record)


def substr_match(seq, line, *, skip_spaces=True, ignore_case=True):
    """
    True iff the substr is present in string. Ignore spaces and optionally case.
    """
    return substr_ind(seq, line, skip_spaces=skip_spaces,
                      ignore_case=ignore_case) != []


def substr_ind(seq, line, *, skip_spaces=True, ignore_case=True):
    """
    Return the start and end + 1 index of a substring match of seq to line.

    Returns:
        [start, end + 1] if needle found in line
        [] if needle not found in line
    """
    if ignore_case:
        seq = seq.lower()
        line = line.lower()

    if skip_spaces:
        seq = seq.replace(' ', '')

    start = None
    count = 0
    for ind, char in enumerate(line):
        if skip_spaces and char == ' ':
            continue

        if char == seq[count]:
            if count == 0:
                start = ind
            count += 1
        else:
            count = 0
            start = None

        if count == len(seq):
            return [start, ind + 1]

    return []


def rel_to_abs(*path_parts):
    """
    Convert an internally relative path to an absolute one.
    """
    return os.path.join(ROOT_DIR, *path_parts)


def get_config(*keys):
    """
    Return keys straight from yaml config.
    """
    try:
        with open(YAML_FILE) as fin:
            conf = yaml.load(fin, Loader=Loader)
    except FileNotFoundError:
        raise cog.exc.MissingConfigFile("Missing config.yml. Expected at: " + YAML_FILE)

    for key in keys:
        conf = conf[key]

    return conf


def init_logging():  # pragma: no cover
    """
    Initialize project wide logging. See config file for details and reference on module.

     - On every start the file logs are rolled over.
     - This must be the first invocation on startup to set up logging.
    """
    log_file = rel_to_abs(get_config('paths', 'log_conf'))
    try:
        with open(log_file) as fin:
            lconf = yaml.load(fin, Loader=Loader)
    except FileNotFoundError:
        raise cog.exc.MissingConfigFile("Missing log.yml. Expected at: " + log_file)

    for handler in lconf['handlers']:
        try:
            os.makedirs(os.path.dirname(lconf['handlers'][handler]['filename']))
        except (OSError, KeyError):
            pass

    with open(log_file) as fin:
        logging.config.dictConfig(yaml.load(fin, Loader=Loader))

    print('See main.log for general traces.')
    print('Enabled rotating file logs:')
    for top_log in ('asyncio', 'cog', 'cogdb'):
        for handler in logging.getLogger(top_log).handlers:
            if isinstance(handler, logging.handlers.RotatingFileHandler):
                print('    ' + handler.baseFilename)
                handler.doRollover()

    # Can't configure discord without cloberring existing, so manually setting
    cog_rot = logging.getLogger('cog').handlers[0]
    rhand_file = os.path.join(os.path.dirname(cog_rot.baseFilename), 'discord.log')
    handler = logging.handlers.RotatingFileHandler(filename=rhand_file, encoding=cog_rot.encoding,
                                                   backupCount=cog_rot.backupCount,
                                                   maxBytes=cog_rot.maxBytes)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(cog_rot.formatter)
    print('    ' + handler.baseFilename)

    dlog = logging.getLogger('discord')
    dlog.setLevel(logging.DEBUG)
    dlog.addHandler(handler)
    dlog.addHandler(logging.getLogger('cog').handlers[-1])


def dict_to_columns(data):
    """
    Transform the dict into columnar form with keys as column headers.
    """
    lines = []
    header = []

    for col, key in enumerate(sorted(data)):
        header.append('{} ({})'.format(key, len(data[key])))

        for row, item in enumerate(data[key]):
            try:
                lines[row]
            except IndexError:
                lines.append([])
            while len(lines[row]) != col:
                lines[row].append('')
            lines[row].append(item)

    return [header] + lines


async def get_coords(system_names):
    """
    Query EDSM for the coordinates to all systems requested.

    Returns:
        List of all systems requested. Empty list if invalid request.
        Entries of form: {'name': 'Sol', 'coords': {'x': 0, 'y', 0, 'z': 0}}
    """
    params = {'showCoordinates': 1, 'systemName[]': system_names}
    url_parts = list(urlparse.urlparse(EDSM))
    url_parts[4] = urlencode(params, doseq=True)
    url = urlparse.urlunparse(url_parts)

    with aiohttp.ClientSession() as http:
        async with http.get(url) as resp:
            return json.loads(await resp.text())


def compute_dists(start, others):
    """
    Compute distance/magnitude from start to others.
    Result is added to each entries dict under other['dist'] = result.

    Entries are of form: {'name': 'Sol', 'coords': {'x': 0, 'y', 0, 'z': 0}}

    Returns:
        others, with data embedded.
    """
    pt1 = start['coords']
    for other in others:
        pt2 = other['coords']
        dist = 0
        for let in ['x', 'y', 'z']:
            temp = pt2[let] - pt1[let]
            dist += temp * temp
        other['dist'] = math.sqrt(dist)

    return others


def complete_blocks(parts):
    """
    Take a list of message parts, complete code blocks as needed to
    preserve intended formatting.

    Returns:
        List of messages that have been modified.
    """
    new_parts = []
    incomplete = False
    block = "```"

    for part in parts:
        num_blocks = part.count(block) % 2
        if incomplete and not num_blocks:
            part = block + part + block

        elif incomplete and num_blocks:
            part = block + part
            incomplete = not incomplete

        elif num_blocks:
            part = part + block
            incomplete = not incomplete

        new_parts += [part]

    return new_parts


def msg_splitter(msg):
    """
    Take a msg of arbitrary length and split it into parts that respect discord 2k char limit.

    Returns:
        List of messages to send in order.
    """
    parts = []
    part_line = ''

    for line in msg.split("\n"):
        line = line + "\n"

        if len(part_line) + len(line) > MSG_LIMIT:
            parts += [part_line.rstrip("\n")]
            part_line = line
        else:
            part_line += line

    if part_line:
        parts += [part_line.rstrip("\n")]

    return parts


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
YAML_FILE = rel_to_abs('data', 'config.yml')
