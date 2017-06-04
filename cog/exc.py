"""
Common exceptions.
"""
from __future__ import absolute_import, print_function


class SheetParsingError(Exception):
    """
    During sheet parsing, could not determine cell anchors properly.
    """
    pass


class ColOverflow(Exception):
    """
    Raise when a column has reached end, increment next column.
    """
    pass


class InvalidCommandArgs(Exception):
    """
    Unable to process command due to bad arguements.
    """
    pass


class IncorrectData(Exception):
    """
    Raise when data no longer contains useful information.
    """
    pass


class MissingConfigFile(Exception):
    """
    Thrown if a config isn't set properly.
    """
    pass
