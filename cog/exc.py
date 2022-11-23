"""
Common exceptions.
"""
from cog.matching import substr_ind, DUMMY_ATTRIBUTE
PAD_HEADER = '=' * 20


class CogException(Exception):
    """
    All exceptions subclass this. All exceptions can:
        - Write something useful to the log.
        - Reply to the user with some relevant response.
    """
    def __init__(self, msg=None, lvl='info'):
        super().__init__(msg)
        self.log_level = lvl

    def write_log(self, log, *, content, author, channel):
        """
        Log all relevant message about this session.
        """
        log_func = getattr(log, self.log_level)
        name = self.__class__.__name__ + ': ' + str(self)
        header = f'\n{name}\n{PAD_HEADER}\n'
        log_func(header + log_format(content=content, author=author, channel=channel))


class UserException(CogException):
    """
    Exception occurred usually due to user error.

    Not unexpected but can indicate a problem.
    """


class ArgumentParseError(UserException):
    """ Error raised on failure to parse arguments. """


class ArgumentHelpError(UserException):
    """ Error raised on request to print help for command. """


class InvalidCommandArgs(UserException):
    """ Unable to process command due to bad arguements.  """


class InvalidPerms(UserException):
    """ Unable to process command due to insufficient permissions.  """


class MoreThanOneMatch(UserException):
    """ Too many matches were found for sequence.  """
    def __init__(self, needle, haystack, type_name, obj_attr=DUMMY_ATTRIBUTE):
        super().__init__('Empty')
        self.needle = needle
        self.haystack = haystack
        self.type_name = type_name
        self.obj_attr = obj_attr

    def __str__(self):
        matches = [emphasize_match(self.needle, getattr(obj, self.obj_attr, obj))
                   for obj in self.haystack]
        matches = "    - " + "\n    - ".join(matches)
        return f"""Unable to match exactly one result. Refine the search.

Looked for __{self.needle}__ in {self.type_name}s. Potentially matched the following:

{matches}"""


class NoMatch(UserException):
    """
    No match was found for sequence.
    """
    def __init__(self, needle, type_name):
        super().__init__('Empty')
        self.needle = needle
        self.type_name = type_name

    def __str__(self):
        return f"""No match when one was required. Refine the search.

Looked for for __{self.needle}__ in {self.type_name}s."""


class CmdAborted(UserException):
    """ Raised to cancel a multistep command. """


class InternalException(CogException):
    """
    An internal exception that went uncaught.

    Indicates a severe problem.
    """
    def __init__(self, msg, lvl='exception'):
        super().__init__(msg, lvl)


class ValidationFail(InternalException):
    """ Raise when a validation function on db has failed. """
    def __init__(self, msg=''):
        super().__init__(msg)


class ColOverflow(InternalException):
    """ Raise when a column has reached end, increment next column.  """
    def __init__(self):
        super().__init__('Serious problem, uncaught overflow.', 'exception')


class FailedJob(InternalException):
    """ Raised internally, cannot restart this job. """


class MissingConfigFile(InternalException):
    """ Thrown if a config isn't set properly.  """


class MsgTooLong(InternalException):
    """
    Reached Discord's maximum message length.
    """


class NoMoreTargets(InternalException):
    """
    There are no more fort targets.
    """


class RemoteError(InternalException):
    """
    Can no longer communicate with a remote that is required.
    """


class SheetParsingError(InternalException):
    """
    During sheet parsing, could not determine cell anchors properly.
    """
    def __init__(self, msg="Critical sheet parsing error."):
        super().__init__(msg)


class NameCollisionError(SheetParsingError):
    """
    During parsing, two cmdr names collided.
    """
    def __init__(self, sheet, name, rows):
        super().__init__()
        self.name = name
        self.sheet = sheet
        self.rows = rows

    def __str__(self):
        lines = [
            "**Critical Error**",
            "----------------",
            f"CMDR \"{self.name}\" found in rows {self.rows} of the {self.sheet} Sheet"
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
    indices = substr_ind(seq.lower(), line.lower(), skip_spaces=True)
    if indices:
        matched = line[indices[0]:indices[1]]
        line = line.replace(matched, fmt.format(matched))

    return line


def log_format(*, content, author, channel):
    """ Log useful information from discord.py """
    msg = "{aut} sent {cmd} from {cha}/{srv}"
    msg += "\n    Discord ID: " + str(author.id)
    msg += f"\n    Username: {author.name}#{author.discriminator}"
    for role in author.roles[1:]:
        msg += f"\n    {role.name} on {role.guild.name}"

    return msg.format(aut=author.display_name, cmd=content,
                      cha=channel, srv=channel.guild)
