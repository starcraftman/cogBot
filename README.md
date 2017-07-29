## Simple Python Discord Bot

[![Stories in Ready][WaffleShield]][WaffleDash]

### Overview

This bot is designed to facilitate Powerplay for the federal forces.
The bot runs and monitors our discord channel and manages our powerplay objectives.

### Install

This bot requires python >= 3.5. I suggest using pyenv to provide python and isolate from system.

- Install [pyenv](https://github.com/pyenv/pyenv) however you prefer
- sudo apt-get install libsqlite-dev
- pyenv install 3.5.3
- pyenv local 3.5.3 (in project root)

Now to setup dependent python libraries.
- sudo apt-get install python3-dev libffi-dev libczmq-dev libczmq3
- pip install wheel setuptools
- python setup.py deps

NB: libczqm3 may not be available on some systems. Not to be confused with similarly named zmq kernel messaging.

### Dependencies

Read setup.py, RUN_DEPS and TEST_DEPS variables hold this projects dependencies.

<!-- Links -->
[WaffleShield]: https://badge.waffle.io/starcraftman/cogBot.svg?label=ready&title=Ready
[WaffleDash]: http://waffle.io/starcraftman/cogBot
