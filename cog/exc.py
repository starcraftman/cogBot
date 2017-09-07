"""
Common exceptions.
"""
from __future__ import absolute_import, print_function

import cog.util


class CogException(Exception):
    """
    All exceptions subclass this. All exceptions can:
        - Write something useful to the log.
        - Reply to the user with some relevant response.
    """
    def __init__(self, msg=None, lvl='info'):
        super().__init__()
        self.log_level = lvl
        self.message = msg

    def write_log(self, log, *, content, author, channel):
        """
        Log all relevant message about this session.
        """
        log_func = getattr(log, self.log_level)
        header = '\n{}\n{}\n'.format(self.__class__.__name__ + ': ' + self.reply(), '=' * 20)
        log_func(header + log_format(content=content, author=author, channel=channel))

    def reply(self):
        """
        Construct a reponse to user.
        """
        return self.message

    def __str__(self):
        return self.reply()


class UserException(CogException):
    """
    Exception occurred usually due to user error.

    Not unexpected but can indicate a problem.
    """
    pass


class ArgumentParseError(UserException):
    """ Error raised on failure to parse arguments. """
    pass


class ArgumentHelpError(UserException):
    """ Error raised on request to print help for command. """
    pass


class InvalidCommandArgs(UserException):
    """ Unable to process command due to bad arguements.  """
    pass


class MoreThanOneMatch(UserException):
    """ Too many matches were found for sequence.  """
    def __init__(self, sequence, matches, obj_attr):
        super().__init__(None)
        self.sequence = sequence
        self.matches = matches
        self.obj_attr = obj_attr

    def reply(self):
        header = "Resubmit query with more specific criteria."
        header += "\nToo many matches for '{}' in {}s:".format(
            self.sequence, self.matches[0].__class__.__name__)
        matched_strings = [emphasize_match(self.sequence, getattr(obj, self.obj_attr))
                           for obj in self.matches]
        matched = "\n    - " + "\n    - ".join(matched_strings)
        return header + matched


class NoMatch(UserException):
    """
    No match was found for sequence.
    """
    def __init__(self, sequence, cls=None):
        super().__init__(None)
        self.sequence = sequence
        self.cls = cls if cls else 'String'

    def reply(self):
        return "No matches for '{}' in {}s:".format(self.sequence, self.cls)


class InternalException(CogException):
    """
    An internal exception that went uncaught.

    Indicates a severe problem.
    """
    def __init__(self, msg, lvl='exception'):
        super().__init__(msg, lvl)


class ColOverflow(InternalException):
    """ Raise when a column has reached end, increment next column.  """
    def __init__(self):
        super().__init__('Serious problem, uncaught overflow.', 'exception')


class MissingConfigFile(InternalException):
    """ Thrown if a config isn't set properly.  """
    pass


class MsgTooLong(InternalException):
    """
    Reached Discord's maximum message length.
    """
    pass


class SheetParsingError(InternalException):
    """
    During sheet parsing, could not determine cell anchors properly.
    """
    def __init__(self):
        super().__init__('Serious problem, this message should not print.')


class NameCollisionError(SheetParsingError):
    """
    During parsing, two cmdr names collided.
    """
    def __init__(self, sheet, name, rows):
        super().__init__()
        self.name = name
        self.sheet = sheet
        self.rows = rows

    def reply(self):
        lines = [
            "**Critical Error**",
            "----------------",
            "CMDR \"{}\" found in rows {} of the {} Sheet".format(self.name, str(self.rows),
                                                                  self.sheet),
            "",
            "To Resolve:",
            "    Delete or rename the cmdr in one of these rows",
            "    Then execute `admin scan` to reload the db",
        ]
        return "\n".join(lines)


def emphasize_match(seq, line, fmt='__{}__'):
    """
    Emphasize the matched portion of string.
    """
    start, end = cog.util.substr_ind(seq, line)
    matched = line[start:end]
    return line.replace(matched, fmt.format(matched))


def log_format(*, content, author, channel):
    """ Log useful information from discord.py """
    msg = "{aut} sent {cmd} from {cha}/{srv}"
    msg += "Discord ID: " + author.id
    msg += "Username: {}#{}".format(author.name, author.discriminator)
    msg += "Joined: " + str(author.joined_at)
    for role in author.roles[1:]:
        msg += "\n    {} on {}".format(role.name, role.server.name)

    return msg.format(aut=author.display_name, cmd=content,
                      cha=channel, srv=channel.server)
