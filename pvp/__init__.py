"""
For main documentation consult pvp/bot.py
"""
import sys

__version__ = '0.1.0'

try:
    assert sys.version_info[0:2] >= (3, 7)
except AssertionError:
    print('This entire program must be run with python >= 3.7')
    print('If unavailable on platform, see https://github.com/pyenv/pyenv')
    sys.exit(1)
