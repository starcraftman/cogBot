"""
Test fortification logic
"""
from __future__ import absolute_import, print_function

import pytest

import fort


def test_parse_int():
    assert fort.parse_int('34') == 34
