"""
Common functions.
"""
from __future__ import absolute_import, print_function
import functools
import logging
import logging.handlers
import logging.config
import os
import sys

import argparse
import tempfile
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import cogdb
import cogdb.schema
import cogdb.query
import cog.exc
import cog.sheets
import cog.tbl


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
YAML_FILE = os.path.join(ROOT_DIR, '.secrets', 'config.yaml')


# TODO: I may be trying too hard to use argparse. May be easier to make small custom parser.


class ThrowArggumentParser(argparse.ArgumentParser):
    def print_help(self, file=None):  # pylint: disable=redefined-builtin
        raise cog.exc.ArgumentHelpError(self.format_help())

    def error(self, message):
        raise cog.exc.ArgumentParseError(message, self.format_usage())

    def exit(self, status=0, message=None):
        """
        Suppress default exit behaviour.
        """
        raise cog.exc.ArgumentParseError(message, self.format_usage())


class ModFormatter(logging.Formatter):
    """
    Add a relmod key to record dict.
    This key tracks a module relative this project' root.
    """
    def format(self, record):
        relmod = record.__dict__['pathname'].replace(ROOT_DIR + os.path.sep, '')
        record.__dict__['relmod'] = relmod[:-3]
        return super(ModFormatter, self).format(record)


def get_config(*keys):
    """
    Return keys straight from yaml config.
    """
    with open(YAML_FILE) as conf:
        conf = yaml.load(conf, Loader=Loader)

    for key in keys:
        conf = conf[key]

    return conf


def init_logging():
    """
    Initialize project wide logging. The setup is described best in config file.

     - On every start the file logs are rolled over.
     - This should be first invocation on startup to set up logging.
    """
    log_folder = os.path.join(tempfile.gettempdir(), 'cog')
    try:
        os.makedirs(log_folder)
    except OSError:
        pass
    print('LOGGING FOLDER:', log_folder)

    with open(rel_to_abs('log.yaml')) as fin:
        log_conf = yaml.load(fin, Loader=Loader)
    logging.config.dictConfig(log_conf)

    for handler in logging.getLogger('cog').handlers + logging.getLogger('cogdb').handlers:
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            handler.doRollover()


def init_db(sheet_id):
    """
    Run common database initialization.

    - Fetch the current sheet and parse it.
    - Fill database with parsed data.
    - Settatr this module callbacks for GSheets.
    """
    session = cogdb.Session()

    if not session.query(cogdb.schema.System).all():
        secrets = get_config('secrets', 'sheets')
        sheet = cog.sheets.GSheet(sheet_id, rel_to_abs(secrets['json']),
                                  rel_to_abs(secrets['token']))

        system_col = cogdb.query.first_system_column(sheet.get_with_formatting('!A10:J10'))
        cells = sheet.whole_sheet()
        user_col, user_row = cogdb.query.first_user_row(cells)

        # Prep callbacks for later use
        this_module = sys.modules[__name__]
        setattr(this_module, 'callback_add_user',
                functools.partial(cog.sheets.callback_add_user, sheet, user_col))
        setattr(this_module, 'callback_add_fort',
                functools.partial(cog.sheets.callback_add_fort, sheet))

        scanner = cogdb.query.SheetScanner(cells, system_col, user_col, user_row)
        systems = scanner.systems()
        users = scanner.users()
        session.add_all(systems + users)
        session.commit()

        forts = scanner.forts(systems, users)
        session.add_all(forts)
        session.commit()


def rel_to_abs(path):
    """
    Convert an internally relative path to an absolute one.
    """
    return os.path.join(ROOT_DIR, path)


def make_parser(prefix):
    """
    Returns the bot parser.
    """
    parser = ThrowArggumentParser(prog='', description='simple discord bot')

    subs = parser.add_subparsers(title='subcommands',
                                 description='The subcommands of cog')

    sub = subs.add_parser(prefix + 'drop', description='Drop forts for user at system.')
    sub.add_argument('amount', type=int, help='The amount to drop.')
    sub.add_argument('-s', '--system', nargs='+',
                     help='The system to drop at.')
    sub.add_argument('-u', '--user', nargs='+',
                     help='The user to drop for.')
    sub.set_defaults(func=parse_drop)

    sub = subs.add_parser(prefix + 'dump', description='Dump the current db.')
    sub.set_defaults(func=parse_dumpdb)

    sub = subs.add_parser(prefix + 'fort', description='Show next fort target.')
    sub.add_argument('-s', '--systems', nargs='+',
                     help='Show status of these systems.')
    sub.add_argument('-l', '--long', action='store_true',
                     help='show detailed stats')
    sub.add_argument('-n', '--next', action='store_true',
                     help='show NUM systems after current')
    sub.add_argument('num', nargs='?', type=int, default=5,
                     help='number of systems to display')
    sub.set_defaults(func=parse_fort)

    sub = subs.add_parser(prefix + 'info', description='Get information on things.')
    sub.add_argument('user', nargs='?',
                     help='Display information about user.')
    sub.set_defaults(func=parse_info)

    sub = subs.add_parser(prefix + 'scan', description='Scan the sheet for changes.')
    sub.set_defaults(func=parse_scan)

    sub = subs.add_parser(prefix + 'user', description='Manipulate sheet users.')
    sub.add_argument('-a', '--add', action='store_true', default=False,
                     help='Add a user to table if not present.')
    sub.add_argument('-q', '--query', action='store_true', default=False,
                     help='Return username and row if exists.')
    sub.add_argument('user', nargs='+',
                     help='The user to interact with.')
    sub.set_defaults(func=parse_user)

    sub = subs.add_parser(prefix + 'help', description='Show overall help message.')
    sub.set_defaults(func=parse_help)
    return parser


def parse_help(**_):
    """
    Parse the 'help' command.
    """
    over = 'Here is an overview of my commands.\nFor more information do: ![Command] -h\n'
    lines = [
        ['Command', 'Effect'],
        ['!drop', 'Drop forts into the fort sheet.'],
        ['!dump', 'Dump the database to the server console. For admins.'],
        ['!fort', 'Get information about our fort systems.'],
        ['!info', 'Display information on a user.'],
        ['!scan', 'Rebuild the database by fetching and parsing latest data.'],
        ['!time', 'Show game time and time to ticks.'],
        ['!user', 'Manage users.'],
        ['!help', 'This help message.'],
    ]
    return over + cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))


def parse_dumpdb(**_):
    """
    Parse the 'dump' command.

    DB is ONLY dumped to console.
    """
    cogdb.query.dump_db()
    return 'Db has been dumped to console.'


def parse_scan(**_):
    """
    Parse the 'scan' command.
    """
    cogdb.schema.drop_scanned_tables()
    init_db(get_config('hudson', 'cattle', 'id'))
    return 'The database has been updated with the latest sheet data.'


def parse_info(**kwargs):
    """
    Parse the 'info' command.
    """
    args = kwargs['args']
    msg = kwargs['msg']
    if args.user:
        members = msg.channel.server.members
        user = cogdb.query.fuzzy_find(args.user, members, obj_attr='display_name')
    else:
        user = msg.author

    lines = [
        '**' + user.display_name + '**',
        '-' * (len(user.display_name) + 6),
        'Username: {}#{}'.format(user.name, user.discriminator),
        'ID: ' + user.id,
        'Status: ' + str(user.status),
        'Join Date: ' + str(user.joined_at),
        'Roles: ' + str([str(role) for role in user.roles[1:]]),
        'Highest Role: ' + str(user.top_role).replace('@', '@ '),
    ]
    response = '\n'.join(lines)

    return response


def parse_fort(**kwargs):
    """
    Parse the 'fort' command.
    """
    # TODO: Allow lookup by status (--summary)
    session = cogdb.Session()
    args = kwargs['args']
    cur_index = cogdb.query.find_current_target(session)

    if args.systems:
        args.long = True
        systems = []
        for system in args.systems:
            try:
                systems.append(cogdb.query.get_system_by_name(session, system, search_all=True))
            except cog.exc.NoMatch:
                pass
            except cog.exc.MoreThanOneMatch as exc:
                systems.extend(exc.matches)
    elif args.next:
        systems = cogdb.query.get_next_fort_targets(session, cur_index, count=args.num)
    else:
        systems = cogdb.query.get_fort_targets(session, cur_index)

    if args.long:
        lines = [systems[0].__class__.header] + [system.table_row for system in systems]
        response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))
    else:
        response = '\n'.join([system.name for system in systems])

    return response


def parse_user(**kwargs):
    """
    Parse the 'user' command.
    """
    session = cogdb.Session()
    args = kwargs['args']
    try:
        args.user = ' '.join(args.user)
        user = cogdb.query.get_sheet_user_by_name(session, args.user)
    except cog.exc.NoMatch:
        user = None

    if user:
        if args.query or args.add:
            response = "User '{}' already present in row {}.".format(user.sheet_name,
                                                                     user.sheet_row)
    else:
        if args.add:
            new_user = cogdb.query.add_suser(session, callback_add_user, args.user)  # pylint: disable=undefined-variable
            response = "Added '{}' to row {}.".format(new_user.sheet_name,
                                                      new_user.sheet_row)
        else:
            response = "User '{}' not found.".format(args.user)

    return response


def parse_drop(**kwargs):
    """
    Parse the 'drop' command.
    """
    log = logging.getLogger('cog.share')
    session = cogdb.Session()
    args = kwargs['args']
    msg = kwargs['msg']

    # FIXME: Refactor this area. Or alternative to connect parsed commands to actions.
    if args.user:
        args.user = ' '.join(args.user)
        import mock
        duser = mock.Mock()
        duser.suser = cogdb.query.get_sheet_user_by_name(session, args.user)
        duser.sheet_name = duser.suser.sheet_name
        duser.display_name = duser.sheet_name
    else:
        duser = cogdb.query.get_discord_user_by_id(session, msg.author.id)
        get_or_create_sheet_user(session, duser)
    log.info('DROP - Matched duser %s with id %s.',
             args.user if args.user else msg.author.display_name, duser.display_name)

    if args.system:
        args.system = ' '.join(args.system)
        system = cogdb.query.get_system_by_name(session, args.system)
    else:
        current = cogdb.query.find_current_target(session)
        system = cogdb.query.get_fort_targets(session, current)[0]
    log.info('DROP - Matched system %s based on args: %s.',
             system.name, args.system)

    fort = cogdb.query.add_fort(session, callback_add_fort,  # pylint: disable=undefined-variable
                                system=system, user=duser.suser,
                                amount=args.amount)
    log.info('DROP - Sucessfully dropped %d at %s for %s.',
             args.amount, system.name, duser.display_name)

    lines = [fort.system.__class__.header, fort.system.table_row]
    return cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))


def get_or_create_sheet_user(session, duser):
    """
    Try to find a user's entry in the sheet. If sheet_name is set, use that
    otherwise fall back to display_name (their server nickname).
    """
    look_for = duser.sheet_name if duser.sheet_name else duser.display_name

    try:
        suser = cogdb.query.get_sheet_user_by_name(session, look_for)
        duser.sheet_name = suser.sheet_name
    except cog.exc.NoMatch:
        duser.sheet_name = duser.display_name
        suser = cogdb.query.add_suser(session, cog.sheets.callback_add_user,
                                      sheet_name=duser.sheet_name)

    return suser


def get_or_create_duser(member):
    """
    Ensure a member has an entry in the dusers table.

    Returns: The DUser object.
    """
    try:
        session = cogdb.Session()
        duser = cogdb.query.get_discord_user_by_id(session, member.id)
    except cog.exc.NoMatch:
        duser = cogdb.query.add_duser(session, member)

    return duser
