"""
Common functions.
"""
from __future__ import absolute_import, print_function
import logging
import logging.handlers
import logging.config
import os

import argparse
import tempfile
import yaml
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

import cogdb
import cogdb.query
import cog.sheets
import cog.tbl


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
YAML_FILE = os.path.join(ROOT_DIR, '.secrets', 'config.yaml')


# TODO: I may be trying too hard to use argparse. May be easier to make small custom parser.


class ArgumentParseError(Exception):
    """ Error raised instead of exiting on argparse error. """
    pass


class ThrowArggumentParser(argparse.ArgumentParser):
    def error(self, message=None):
        """
        Suppress default exit after error.
        """
        raise ArgumentParseError()

    def exit(self, status=0, message=None):
        """
        Suppress default exit behaviour.
        """
        raise ArgumentParseError()


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

    IMPORTANT: On every start the file logs are rolled over.
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
    Scan sheet and fill database if empty.
    """
    session = cogdb.Session()

    if not session.query(cogdb.schema.System).all():
        secrets = get_config('secrets', 'sheets')
        sheet = cog.sheets.GSheet(sheet_id, rel_to_abs(secrets['json']),
                                  rel_to_abs(secrets['token']))

        system_col = cogdb.query.first_system_column(sheet.get_with_formatting('!A10:J10'))
        cells = sheet.whole_sheet()
        user_col, user_row = cogdb.query.first_user_row(cells)
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


def make_parser():
    """
    Returns the bot parser.
    """
    parser = ThrowArggumentParser(prog='cog', description='simple discord bot')

    subs = parser.add_subparsers(title='subcommands',
                                 description='The subcommands of cog')

    sub = subs.add_parser('drop', description='Drop forts for user at system.')
    sub.add_argument('amount', type=int, help='The amount to drop.')
    sub.add_argument('-s', '--system', required=True, nargs='+',
                     help='The system to drop at.')
    sub.add_argument('-u', '--user', nargs='+',
                     help='The user to drop for.')
    sub.set_defaults(func=parse_drop)

    sub = subs.add_parser('dump', description='Dump the current db.')
    sub.set_defaults(func=parse_dumpdb)

    sub = subs.add_parser('fort', description='Show next fort target.')
    sub.add_argument('-l', '--long', action='store_true', default=False,
                     help='show detailed stats')
    sub.add_argument('-n', '--next', action='store_true', default=False,
                     help='show NUM systems after current')
    sub.add_argument('num', nargs='?', type=int, default=5,
                     help='number of systems to display')
    sub.set_defaults(func=parse_fort)

    sub = subs.add_parser('info', description='Get information on things.')
    sub.add_argument('user', nargs='?',
                     help='Display information about user.')
    sub.set_defaults(func=parse_info)

    sub = subs.add_parser('user', description='Manipulate sheet users.')
    sub.add_argument('-a', '--add', action='store_true', default=False,
                     help='Add a user to table if not present.')
    sub.add_argument('-q', '--query', action='store_true', default=False,
                     help='Return username and row if exists.')
    sub.add_argument('user', nargs='+',
                     help='The user to interact with.')
    sub.set_defaults(func=parse_user)

    sub = subs.add_parser('help', description='Show overall help message.')
    sub.set_defaults(func=parse_help)
    return parser


def parse_help(**_):
    """
    Simply prints overall help documentation.
    """
    lines = [
        ['Command', 'Effect'],
        ['!fort', 'Show current fort target.'],
        ['!fort -l', 'Show current fort target\'s status.'],
        ['!fort -n NUM', 'Show the next NUM targets. Default NUM = 5.'],
        ['!fort -nl NUM', 'Show status of the next NUM targets.'],
        ['!user -a USER', 'Add a USER to table.'],
        ['!user -q USER', 'Check if user is in table.'],
        ['!drop AMOUNT -s SYSTEM -u USER', 'Increase by AMOUNT forts for USER at SYSTEM'],
        ['!info USER', 'Display information on user.'],
        ['!help', 'This help message.'],
    ]
    return cog.tbl.wrap_markdown(cog.tbl.format_table(lines, header=True))


def parse_dumpdb(**_):
    cogdb.query.dump_db()


def parse_info(**kwargs):
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
    session = cogdb.Session()
    args = kwargs['args']
    cur_index = cogdb.query.find_current_target(session)
    if args.next:
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
    session = cogdb.Session()
    args = kwargs['args']
    args.user = ' '.join(args.user)
    try:
        user = cogdb.query.get_sheet_user_by_name(session, args.user)
    except cog.exc.NoMatch:
        user = None

    if user:
        if args.query or args.add:
            response = "User '{}' already present in row {}.".format(user.sheet_name,
                                                                     user.sheet_row)
    else:
        if args.add:
            new_user = cogdb.query.add_suser(session, cog.sheets.callback_add_user, args.user)
            response = "Added '{}' to row {}.".format(new_user.sheet_name,
                                                      new_user.sheet_row)
        else:
            response = "User '{}' not found.".format(args.user)

    return response


def parse_drop(**kwargs):
    session = cogdb.Session()
    args = kwargs['args']
    args.system = ' '.join(args.system)
    if args.user:
        args.user = ' '.join(args.user)

    system = cogdb.query.get_system_by_name(session, args.system)
    user = cogdb.query.get_sheet_user_by_name(session, args.user)
    fort = cogdb.query.add_fort(session, cog.sheets.callback_add_fort,
                                system=system, user=user, amount=args.amount)

    lines = [fort.system.__class__.header, fort.system.table_row]
    return cog.tbl.wrap_markdown(cog.tbl.format_table(lines, sep='|', header=True))


def check_member(member):
    """
    Ensure a member has entries in requisite tables.
    """
    session = cogdb.Session()
    try:
        cogdb.query.get_discord_user_by_id(session, member.id)
    except cog.exc.NoMatch:
        duser = cogdb.query.add_duser(session, member)

    # Try to loosely match if first fails
    tries = 3
    look_for = duser.display_name
    while tries:
        try:
            suser = cogdb.query.get_sheet_user_by_name(session, look_for)
            duser.sheet_name = suser.sheet_name
            tries = 0
        except cog.exc.NoMatch:
            look_for = duser.display_name[1:-1]
            tries -= 1
            if not tries:
                cogdb.query.add_suser(session, cog.sheets.callback_add_user,
                                      sheet_name=duser.sheet_name)

    return duser
