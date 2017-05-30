"""
Common exceptions.
"""
from __future__ import absolute_import, print_function


class InvalidCommandArgs(Exception):
    """
    Unable to process command due to bad arguements.
    """
    pass


class ColOverflow(Exception):
    """
    Raise when a column has reached end, increment next column.
    """
    pass


class IncompleteData(Exception):
    """
    Raise when data no longer contains useful information.
    """
    pass


class MissingConfigFile(Exception):
    """
    Thrown if a config isn't set properly.
    """
    pass
