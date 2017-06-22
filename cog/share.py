"""
Common functions.
"""
from __future__ import absolute_import, print_function
import datetime as date
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
YAML_FILE = os.path.join(ROOT_DIR, 'data', 'config.yml')


class ThrowArggumentParser(argparse.ArgumentParser):
    """
    ArgumentParser subclass that does NOT terminate the program.
    """
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


def rel_to_abs(*path_parts):
    """
    Convert an internally relative path to an absolute one.
    """
    return os.path.join(ROOT_DIR, *path_parts)


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

    with open(rel_to_abs(get_config('paths', 'log_conf'))) as fin:
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
        paths = get_config('paths')
        sheet = cog.sheets.GSheet(sheet_id,
                                  rel_to_abs(paths['json']),
                                  rel_to_abs(paths['token']))

        cells = sheet.whole_sheet()
        system_col = cogdb.query.first_system_column(sheet.get_with_formatting('!A10:J10'))
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
    sub.set_defaults(func=command_drop)

    sub = subs.add_parser(prefix + 'dump', description='Dump the current db.')
    sub.set_defaults(func=command_dump)

    sub = subs.add_parser(prefix + 'fort', description='Show next fort target.')
    sub.add_argument('--status', action='store_true',
                     help='Provide an overview of the fort systems.')
    sub.add_argument('--systems', nargs='+',
                     help='Show status of these systems.')
    sub.add_argument('-l', '--long', action='store_true',
                     help='show detailed stats')
    sub.add_argument('-n', '--next', type=int,
                     help='show NUM systems after current')
    sub.set_defaults(func=command_fort)

    sub = subs.add_parser(prefix + 'info', description='Get information on things.')
    sub.add_argument('user', nargs='?',
                     help='Display information about user.')
    sub.set_defaults(func=command_info)

    sub = subs.add_parser(prefix + 'scan', description='Scan the sheet for changes.')
    sub.set_defaults(func=command_scan)

    sub = subs.add_parser(prefix + 'time', description='Time in game and to ticks.')
    sub.set_defaults(func=command_time)

    sub = subs.add_parser(prefix + 'user', description='Manipulate sheet users.')
    sub.add_argument('--cry', nargs='+',
                     help='Set your tag in the sheets.')
    sub.add_argument('--name', nargs='+',
                     help='Set your name in the sheets.')
    sub.add_argument('--winters', action='store_true',
                     help='Set yourself to use the Winters sheets.')
    sub.add_argument('--hudson', action='store_true',
                     help='Set yourself to use the Hudson sheets.')
    sub.set_defaults(func=command_user)

    sub = subs.add_parser(prefix + 'help', description='Show overall help message.')
    sub.set_defaults(func=command_help)
    return parser


def command_help(**_):
    """
    Provide an overview of help.
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
        ['!user', 'UNUSED ATM. Manage users.'],
        ['!help', 'This help message.'],
    ]

    return over + cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))


def command_dump(**_):
    """
    For debugging, able to dump the database quickly to console.
    """
    cogdb.query.dump_db()
    return 'Db has been dumped to server console.'


def command_scan(**_):
    """
    Allow reindexing the sheets when out of date with new edits.
    """
    cogdb.schema.drop_scanned_tables()
    init_db(get_config('hudson', 'cattle'))
    return 'The database has been updated with the latest sheet data.'


def command_time(**_):
    """
    Provide the time command.

    Shows the time ...
    - In game
    - To daily BGS tick
    - To weekly tick
    """
    now = date.datetime.utcnow().replace(microsecond=0)
    today = now.replace(hour=0, minute=0, second=0)

    daily_tick = today + date.timedelta(hours=16)
    if daily_tick < now:
        daily_tick = daily_tick + date.timedelta(days=1)

    weekly_tick = today + date.timedelta(hours=7)
    while weekly_tick.strftime('%A') != 'Thursday':
        weekly_tick += date.timedelta(days=1)

    lines = [
        'Game Time: **{}**'.format(now.strftime('%H:%M:%S')),
        'Time to BGS Tick: **{}** ({})'.format(daily_tick - now, daily_tick),
        'Time to Cycle Tick: **{}** ({})'.format(weekly_tick - now, weekly_tick),
        'All Times UTC',
    ]
    return '\n'.join(lines)


def command_info(**kwargs):
    """
    Provide information about the discord server.
    """
    args = kwargs.get('args')
    message = kwargs.get('message')

    if args.user:
        members = message.channel.server.members
        user = cogdb.query.fuzzy_find(args.user, members, obj_attr='display_name')
    else:
        user = message.author

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
    return '\n'.join(lines)


def command_fort(**kwargs):
    """
    Provide information on and manage the fort sheet.
    """
    args = kwargs.get('args')
    session = cogdb.Session()
    systems = []

    if args.status:
        states = cogdb.query.get_all_systems_by_state(session)
        total = len(cogdb.query.get_all_systems(session))

        keys = ['cancelled', 'fortified', 'undermined', 'skipped', 'left']
        lines = [
            [key.capitalize() for key in keys],
            ['{}/{}'.format(len(states[key]), total) for key in keys],
        ]
        return cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))

    if args.systems:
        args.long = True
        for system in args.systems:
            try:
                systems.append(cogdb.query.get_system_by_name(session,
                                                              system, search_all=True))
            except (cog.exc.NoMatch, cog.exc.MoreThanOneMatch):
                pass
    elif args.next:
        cur_index = cogdb.query.find_current_target(session)
        systems = cogdb.query.get_next_fort_targets(session,
                                                    cur_index, count=args.next)
    else:
        cur_index = cogdb.query.find_current_target(session)
        systems = cogdb.query.get_fort_targets(session, cur_index)

    if args.long:
        lines = [systems[0].__class__.header] + [system.table_row for system in systems]
        response = cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))
    else:
        lines = [system.short_display() for system in systems]
        response = '\n'.join(lines)

    return response


def command_user(**_):
    """
    Manage user properties.
    """
    return 'Reserved for future use.'


def command_drop(**kwargs):
    """
    Drop forts at the fortification target.
    """
    log = logging.getLogger('cog.share')
    session = cogdb.Session()
    args = kwargs.get('args')
    msg = kwargs.get('message')

    if args.user:
        args.user = ' '.join(args.user)
        import mock
        duser = mock.Mock()
        duser.suser = cogdb.query.get_sheet_user_by_name(session, args.user)
        duser.sheet_name = duser.suser.sheet_name
        duser.display_name = duser.sheet_name
    else:
        duser = cogdb.query.get_discord_user_by_id(session, msg.author.id)
        cogdb.query.get_or_create_sheet_user(session, duser)
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

    return fort.system.short_display()


def dict_to_columns(data):
    """
    Transform the dict into columnar form with keys as column headers.
    """
    lines = []
    header = []

    for col, key in enumerate(sorted(data)):
        header.append('{} ({})'.format(key, len(data[key])))

        for row, item in enumerate(data[key]):
            try:
                lines[row]
            except IndexError:
                lines.append([])
            while len(lines[row]) != col:
                lines[row].append('')
            lines[row].append(item)

    return [header] + lines
