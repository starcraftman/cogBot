"""
Common exceptions.
"""
from __future__ import absolute_import, print_function


class ArgumentParseError(Exception):
    """ Error raised on failure to parse arguments. """
    def __init__(self, message, usage):
        super(ArgumentParseError, self).__init__()
        self.message = message
        self.usage = usage


class ArgumentHelpError(Exception):
    """ Error raised on request to print help for command. """
    def __init__(self, message):
        super(ArgumentHelpError, self).__init__()
        self.message = message


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


class MissingConfigFile(Exception):
    """
    Thrown if a config isn't set properly.
    """
    pass


def emphasize_match(seq, line, fmt='__{}__'):
    """
    Emphasize the matched portion of string.
    """
    start = line.lower().index(seq)
    matched = line[start:start + len(seq)]
    return line.replace(matched, fmt.format(matched))


class MoreThanOneMatch(Exception):
    """
    Too many matches were found for sequence.
    """
    def __init__(self, sequence, matches, obj_attr):
        self.sequence = sequence
        self.matches = matches
        self.obj_attr = obj_attr
        super(MoreThanOneMatch, self).__init__()

    def __str__(self):
        header = "Resubmit query with more specific criteria."
        header += "\nToo many matches for '{}' in {}s:".format(
            self.sequence, self.matches[0].__class__.__name__)
        matched_strings = [emphasize_match(self.sequence, getattr(obj, self.obj_attr))
                           for obj in self.matches]
        matched = "\n    - " + "\n    - ".join(matched_strings)
        return header + matched


class MsgTooLong(Exception):
    """
    Reached Discord's maximum message length.
    """
    pass


class NoMatch(Exception):
    """
    No match was found for sequence.
    """
    def __init__(self, sequence, cls=None):
        self.sequence = sequence
        self.cls = cls if cls else 'String'
        super(NoMatch, self).__init__()

    def __str__(self):
        return "No matches for '{}' in {}s:".format(self.sequence, self.cls)


class SheetParsingError(Exception):
    """
    During sheet parsing, could not determine cell anchors properly.
    """
    pass
