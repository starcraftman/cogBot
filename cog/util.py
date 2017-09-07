"""
Utility functions, mainly matching now.
"""
from __future__ import absolute_import, print_function
import logging
import logging.handlers
import logging.config
import os

import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


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
    with open(YAML_FILE) as fin:
        conf = yaml.load(fin, Loader=Loader)

    for key in keys:
        conf = conf[key]

    return conf


def init_logging():  # pragma: no cover
    """
    Initialize project wide logging. The setup is described best in config file.

     - On every start the file logs are rolled over.
     - This should be first invocation on startup to set up logging.
    """
    log_file = rel_to_abs(get_config('paths', 'log_conf'))
    with open(log_file) as fin:
        lconf = yaml.load(fin, Loader=Loader)
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


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
YAML_FILE = rel_to_abs('data', 'config.yml')
