"""
Common exceptions.
"""
from __future__ import absolute_import, print_function


class ColOverflow(Exception):
    """
    Raise when a column has reached end, increment next column.
    """
    pass


class IncorrectData(Exception):
    """
    Raise when data no longer contains useful information.
    """
    pass


class InvalidCommandArgs(Exception):
    """
    Unable to process command due to bad arguements.
    """
    pass


class MissingConfigFile(Exception):
    """
    Thrown if a config isn't set properly.
    """
    pass


class MoreThanOneMatch(Exception):
    """
    Too many matches were found for sequence.
    """
    def __init__(self, sequence, matches):
        self.sequence = sequence
        self.matches = matches
        super(MoreThanOneMatch, self).__init__()

    def __repr__(self):
        matched = '    -' + '\n    -'.join(self.matches)
        return "'{} matched all of:\n".format(self.sequence) + matched


class NoMatch(Exception):
    """
    No match was found for sequence.
    """
    pass


class SheetParsingError(Exception):
    """
    During sheet parsing, could not determine cell anchors properly.
    """
    pass
