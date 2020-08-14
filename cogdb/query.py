"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
from __future__ import absolute_import, print_function
import logging
import os
import tempfile

import sqlalchemy.exc as sqla_exc
import sqlalchemy.orm.exc as sqla_oexc

import cog.exc
import cog.sheets
from cog.util import substr_match
import cogdb
import cogdb.schema
from cogdb.schema import (DUser, System, PrepSystem, SystemUM, SheetRow,
                          Drop, Hold, EFaction, ESheetType,
                          Admin, ChannelPerm, RolePerm, FortOrder, KOS)
from cogdb.side import HUDSON_CONTROLS, WINTERS_CONTROLS


DEFER_MISSING = 750


def fuzzy_find(needle, stack, obj_attr='zzzz', ignore_case=True):
    """
    Searches for needle in whole stack and gathers matches. Returns match if only 1.

    Raise separate exceptions for NoMatch and MoreThanOneMatch.
    """
    matches = []
    for obj in stack:
        try:
            if substr_match(needle, getattr(obj, obj_attr, obj), ignore_case=ignore_case):
                matches.append(obj)
        except cog.exc.NoMatch:
            pass

    num_matches = len(matches)
    if num_matches == 1:
        return matches[0]

    if num_matches == 0:
        cls = stack[0].__class__.__name__ if getattr(stack[0], '__class__') else 'string'
        raise cog.exc.NoMatch(needle, cls)

    raise cog.exc.MoreThanOneMatch(needle, matches, obj_attr)


def dump_db():  # pragma: no cover
    """
    Purely debug function, shunts db contents into file for examination.
    """
    session = cogdb.Session()
    fname = os.path.join(tempfile.gettempdir(), 'dbdump_' + os.environ.get('COG_TOKEN', 'dev'))
    print("Dumping db contents to:", fname)
    with open(fname, 'w') as fout:
        for cls in [DUser, SheetRow, System, SystemUM, Drop, Hold,
                    FortOrder, Admin, RolePerm, ChannelPerm]:
            fout.write('---- ' + str(cls) + ' ----\n')
            fout.writelines([str(obj) + "\n" for obj in session.query(cls)])


def get_duser(session, discord_id):
    """
    Return the DUser that has the same discord_id.

    Raises:
        NoMatch - No possible match found.
    """
    try:
        return session.query(DUser).filter_by(id=discord_id).one()
    except sqla_oexc.NoResultFound:
        raise cog.exc.NoMatch(discord_id, 'DUser')


def ensure_duser(session, member):
    """
    Ensure a member has an entry in the dusers table. A DUser is required by all users.

    Returns: The DUser
    """
    try:
        duser = get_duser(session, member.id)
        duser.display_name = member.display_name
    except cog.exc.NoMatch:
        duser = add_duser(session, member)

    return duser


def add_duser(session, member, *, faction=EFaction.hudson):
    """
    Add a discord user to the database.
    """
    name = member.display_name
    new_duser = DUser(id=member.id, display_name=name,
                      pref_name=name, faction=faction)
    session.add(new_duser)
    session.commit()

    return new_duser


def check_pref_name(session, duser, new_name):
    """
    Check that new name is not taken by another DUser or present as a stray in SheetRows.

    Raises:
        InvalidCommandArgs - DUser.pref_name taken by another DUser.
    """
    others = session.query(DUser).filter(DUser.id != duser.id, DUser.pref_name == new_name).all()
    if others:
        raise cog.exc.InvalidCommandArgs(
            "Sheet name {}, taken by {}.\n\nPlease choose another.".format(
                new_name, others[0].display_name))

    # Note: Unlikely needed, should be caught above. However, no fixed relationship guaranteeing.
    # for sheet in session.query(SheetRow).filter(SheetRow.name == new_name).all():
        # if sheet.duser(session).id != duser.id:
            # raise cog.exc.InvalidCommandArgs("Sheet name {}, taken by {}.\n\nPlease choose another.".format(new_name, sheet.duser(session).display_name))


def next_sheet_row(session, *, cls, faction, start_row):
    """
    Find the next available row to add a SheetRow for.

    Must scan all users, gaps may exist in sheet.
    """
    try:
        users = session.query(cls).filter_by(faction=faction).order_by(cls.row).all()
        last_user = users[0]
        next_row = users[-1].row + 1
        for user in users[1:]:
            if last_user.row + 1 != user.row:
                next_row = last_user.row + 1
                break
            last_user = user
    except IndexError:
        next_row = start_row

    return next_row


def add_sheet(session, name, **kwargs):
    """
    Simply add user past last user in sheet.

    Kwargs:
        cry: The cry to use.
        faction: By default Hudson. Any of EFaction.
        type: By default cattle sheet. Any of SheetRow subclasses.
        start_row: Starting row if none inserted.
    """
    faction = kwargs.get('faction', EFaction.hudson)
    cls = getattr(cogdb.schema, kwargs.get('type', ESheetType.cattle))
    cry = kwargs.get('cry', '')

    next_row = next_sheet_row(session, cls=cls, faction=faction,
                              start_row=kwargs['start_row'])
    sheet = cls(name=name, cry=cry, row=next_row, faction=faction)
    session.add(sheet)
    session.commit()

    return sheet


def fort_get_medium_systems(session):
    """
    Return unfortified systems designated for small/medium ships.
    """
    mediums = session.query(System).all()
    unforted = [med for med in mediums if "S/M" in med.notes and not med.is_fortified and not
                med.skip and not med.missing < DEFER_MISSING]
    return unforted


def fort_get_systems(session, mediums=True):
    """
    Return a list of all Systems. PrepSystems are not included.

    args:
        mediums: If false, exclude all systems designated for j
                 Determined by "S/M" being in notes.
    """
    query = session.query(System).filter(System.type != 'prep')
    if not mediums:
        med_names = [med.name for med in fort_get_medium_systems(session)]
        query = query.filter(System.name.notin_(med_names))

    return query.all()


def fort_get_preps(session):
    """
    Return a list of all PrepSystems.
    """
    return session.query(PrepSystem).all()


def fort_find_current_index(session):
    """
    Scan Systems from the beginning to find next unfortified target that is not Othime.

    Raises:
        NoMoreTargets - No more targets left OR a serious problem with data.
    """
    for ind, system in enumerate(fort_get_systems(session)):
        if system.is_fortified or system.skip or system.missing < DEFER_MISSING:
            continue

        return ind

    raise cog.exc.NoMoreTargets('No more fort targets at this time.')


def fort_find_system(session, system_name, search_all=True):
    """
    Return the System with System.name that matches.
    If search_all True, search all systems.
    If search_all False, search from current target forward.

    Raises:
        NoMatch - No possible match found.
        MoreThanOneMatch - Too many matches possible, ask user to resubmit.
    """
    try:
        return session.query(System).filter_by(name=system_name).one()
    except (sqla_oexc.NoResultFound, sqla_oexc.MultipleResultsFound):
        index = 0 if search_all else fort_find_current_index(session)
        systems = fort_get_systems(session)[index:] + fort_get_preps(session)
        return fuzzy_find(system_name, systems, 'name')


def fort_get_systems_by_state(session):
    """
    Return a dictionary that lists the systems states below:

        left: Has neither been fortified nor undermined.
        fortified: Has been fortified and not undermined.
        undermined: Has been undermined and not fortified.
        cancelled: Has been both fortified and undermined.
    """
    log = logging.getLogger('cogdb.query')
    states = {
        'cancelled': [],
        'fortified': [],
        'left': [],
        'undermined': [],
        'skipped': [],
    }

    for system in fort_get_systems(session):
        log.info('STATE - %s', system)
        if system.is_fortified and system.is_undermined:
            states['cancelled'].append(system)
        if system.is_undermined:
            states['undermined'].append(system)
        if system.is_fortified:
            states['fortified'].append(system)
        if not system.is_fortified and not system.skip:
            states['left'].append(system)
        if system.skip:
            states['skipped'].append(system)

    return states


def fort_get_targets(session):
    """
    Returns a list of Systems that should be fortified.

    - First System is not Othime and is unfortified.
    - Second System if prsent is Othime, only when not fortified.
    - All Systems after are prep targets.
    """
    targets = fort_order_get(session)
    if targets:
        return targets[:1]

    current = fort_find_current_index(session)
    systems = fort_get_systems(session)
    targets = [systems[current]]

    mediums = fort_get_medium_systems(session)
    if mediums and mediums[0].name != targets[0].name:
        targets.append(mediums[0])

    targets += fort_get_preps(session)

    return targets


def fort_get_next_targets(session, count=1):
    """
    Return next 'count' fort targets.
    """
    systems = fort_order_get(session)
    start = 1
    if not systems:
        systems = fort_get_systems(session)
        start = fort_find_current_index(session) + 1

    targets = []
    for system in systems[start:]:
        if system.is_fortified or system.skip or system.missing < DEFER_MISSING:
            continue

        targets.append(system)
        count = count - 1

        if count == 0:
            break

    return targets


def fort_get_deferred_targets(session):
    """
    Return all deferred targets under deferal amount.
    """
    return [system for system in fort_get_systems(session)
            if system.missing < DEFER_MISSING and not system.is_fortified]


def fort_add_drop(session, *, user, system, amount):
    """
    Add a Drop for 'amount' to the database where Drop intersects at:
        System.name and SUser.name
    If fort exists, increment its value. Else add it to database.

    Kwargs: system, user, amount

    Returns: The Drop object.

    Raises:
        InvalidCommandArgs: User requested an amount outside bounds [-800, 800]
    """
    if amount not in range(-800, 801):
        raise cog.exc.InvalidCommandArgs('Drop amount must be in range [-800, 800]')

    try:
        drop = session.query(Drop).filter_by(user_id=user.id, system_id=system.id).one()
    except sqla_oexc.NoResultFound:
        drop = Drop(user_id=user.id, system_id=system.id, amount=0)
        session.add(drop)

    log = logging.getLogger('cogdb.query')
    log.info('ADD_DROP - Before: Drop %s, System %s', drop, system)
    drop.amount = max(0, drop.amount + amount)
    system.fort_status = system.fort_status + amount
    session.commit()
    log.info('ADD_DROP - After: Drop %s, System %s', drop, system)

    return drop


def fort_order_get(_):
    """
    Get the order of systems to fort.

    If any systems have been completed, remove them from the list.

    Returns: [] if no systems set, else a list of System objects.
    """
    systems = []
    dsession = cogdb.Session()  # Isolate deletions, feels a bit off though
    for fort_order in dsession.query(FortOrder).order_by(FortOrder.order):
        system = dsession.query(System).filter_by(name=fort_order.system_name).one()
        if system.is_fortified or system.missing < DEFER_MISSING:
            dsession.delete(fort_order)
        else:
            systems += [system]

    dsession.commit()
    return systems


def fort_order_set(session, system_names):
    """
    Simply set the systems in the order desired.

    Ensure systems are actually valid before.
    """
    try:
        for ind, system_name in enumerate(system_names):
            if not isinstance(system_name, System):
                system_name = fort_find_system(session, system_name).name
            session.add(FortOrder(order=ind, system_name=system_name))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        session.rollback()
        raise cog.exc.InvalidCommandArgs("Duplicate system specified, check your command!")
    except cog.exc.NoMatch:
        session.rollback()
        raise cog.exc.InvalidCommandArgs("System '{}' not found in fort systems.".format(system_name))


def fort_order_drop(session, systems):
    """
    Drop the given system_names from the override table.
    """
    for system_name in systems:
        try:
            if isinstance(system_name, System):
                system_name = system_name.name
            session.delete(session.query(FortOrder).filter_by(system_name=system_name).one())
        except sqla_oexc.NoResultFound:
            pass

    session.commit()


def um_find_system(session, system_name):
    """
    Find the SystemUM with system_name
    """
    try:
        return session.query(SystemUM).filter_by(name=system_name).one()
    except (sqla_oexc.NoResultFound, sqla_oexc.MultipleResultsFound):
        systems = session.query(SystemUM).\
            filter(SystemUM.name.ilike('%{}%'.format(system_name))).\
            all()

        if len(systems) > 1:
            raise cog.exc.MoreThanOneMatch(system_name, systems, SystemUM)

        if len(systems) == 0:
            raise cog.exc.NoMatch(system_name, SystemUM)

        return systems[0]


def um_get_systems(session, exclude_finished=True):
    """
    Return a list of all current undermining targets.

    kwargs:
        finished: Return just the finished targets.
    """
    systems = session.query(SystemUM).all()
    if exclude_finished:
        systems = [system for system in systems if not system.is_undermined]

    return systems


def um_reset_held(session, user):
    """
    Reset all held merits to 0.
    """
    holds = session.query(Hold).filter_by(user_id=user.id).all()
    for hold in holds:
        hold.held = 0

    session.commit()
    return holds


def um_redeem_merits(session, user):
    """
    Redeem all held merits for user.
    """
    total = 0
    holds = session.query(Hold).filter_by(user_id=user.id).all()
    for hold in holds:
        total += hold.held
        hold.redeemed = hold.redeemed + hold.held
        hold.held = 0

    session.commit()
    return (holds, total)


def um_add_hold(session, **kwargs):
    """
    Add or update the user's Hold, that is their UM merits held or redeemed.
        System.name and SUser.name
    If Hold exists, increment the held value. Otherwise add it to database.

    Returns: The Hold object.

    Raises:
        InvalidCommandArgs: Hold cannot be negative.
    """
    system = kwargs['system']
    user = kwargs['user']
    held = kwargs['held']

    if held < 0:
        raise cog.exc.InvalidCommandArgs('Hold amount must be in range [0, \u221E]')

    try:
        hold = session.query(Hold).filter_by(user_id=user.id,
                                             system_id=system.id).one()
    except sqla_oexc.NoResultFound:
        hold = Hold(user_id=user.id, system_id=system.id, held=0, redeemed=0)
        session.add(hold)

    hold.held = held
    session.commit()

    return hold


def um_all_held_merits(session):
    """
    Return a list of lists that show all users with merits still held.

    List of the form:
    [
        [CMDR, system_name_1, system_name_2, ...],
        [cmdrname, merits_system_1, merits_system_2, ...],
        [cmdrname, merits_system_1, merits_system_2, ...],
    ]
    """
    c_dict = {}
    for merit in session.query(Hold).filter(Hold.held > 0).order_by(Hold.system_id).all():
        try:
            c_dict[merit.user.name][merit.system.name] = merit
        except KeyError:
            c_dict[merit.user.name] = {merit.system.name: merit}

    systems = session.query(SystemUM).order_by(SystemUM.id).all()
    system_names = [sys.name for sys in systems]
    rows = []
    for cmdr in c_dict:
        row = [cmdr]
        for system_name in system_names:
            try:
                row += [c_dict[cmdr][system_name].held]
            except KeyError:
                row += [0]

        rows += [row]

    return [['CMDR'] + system_names] + rows


def get_admin(session, member):
    """
    If the member is an admin, return the Admin.
    Otherwise, raise NoMatch.
    """
    try:
        return session.query(Admin).filter_by(id=member.id).one()
    except sqla_oexc.NoResultFound:
        raise cog.exc.NoMatch(member.display_name, 'Admin')


def add_admin(session, member):
    """
    Add a new admin.
    """
    try:
        session.add(Admin(id=member.id))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        raise cog.exc.InvalidCommandArgs("Member {} is already an admin.".format(member.display_name))


def add_channel_perm(session, cmd, server_name, channel_name):
    try:
        session.add(ChannelPerm(cmd=cmd, server=server_name, channel=channel_name))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        raise cog.exc.InvalidCommandArgs("Channel permission already exists.")


def add_role_perm(session, cmd, server_name, role_name):
    try:
        session.add(RolePerm(cmd=cmd, server=server_name, role=role_name))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
        raise cog.exc.InvalidCommandArgs("Role permission already exists.")


def remove_channel_perm(session, cmd, server_name, channel_name):
    try:
        session.delete(session.query(ChannelPerm).
                       filter_by(cmd=cmd, server=server_name, channel=channel_name).one())
        session.commit()
    except sqla_oexc.NoResultFound:
        raise cog.exc.InvalidCommandArgs("Channel permission does not exist.")


def remove_role_perm(session, cmd, server_name, role_name):
    try:
        session.delete(session.query(RolePerm).
                       filter_by(cmd=cmd, server=server_name, role=role_name).one())
        session.commit()
    except sqla_oexc.NoResultFound:
        raise cog.exc.InvalidCommandArgs("Role permission does not exist.")


def check_perms(msg, args):
    """
    Check if a user is authorized to issue this command.
    Checks will be made against channel and user roles.

    Raises InvalidPerms if any permission issue.
    """
    session = cogdb.Session()
    check_channel_perms(session, args.cmd, msg.channel.guild.name, msg.channel.name)
    check_role_perms(session, args.cmd, msg.channel.guild.name, msg.author.roles)


def check_channel_perms(session, cmd, server_name, channel_name):
    """
    A user is allowed to issue a command if:
        a) no restrictions for the cmd
        b) the channel is whitelisted in the restricted channels

    Raises InvalidPerms if fails permission check.
    """
    channels = [perm.channel for perm in session.query(ChannelPerm).
                filter_by(cmd=cmd, server=server_name)]
    if channels and channel_name not in channels:
        raise cog.exc.InvalidPerms("The '{}' command is not permitted on this channel.".format(
            cmd.lower()))


def check_role_perms(session, cmd, server_name, member_roles):
    """
    A user is allowed to issue a command if:
        a) no roles set for the cmd
        b) he matches ANY of the set roles

    Raises InvalidPerms if fails permission check.
    """
    perm_roles = {perm.role for perm in session.query(RolePerm).
                  filter_by(cmd=cmd, server=server_name)}
    member_roles = {role.name for role in member_roles}
    if perm_roles and len(member_roles - perm_roles) == len(member_roles):
        raise cog.exc.InvalidPerms("You do not have the roles for the command.")


def complete_control_name(partial, include_winters=False):
    """
    Provide name completion of Federal controls without db query.
    """
    systems = HUDSON_CONTROLS[:]
    if include_winters:
        systems += WINTERS_CONTROLS

    return fuzzy_find(partial, systems)


def kos_search_cmdr(session, term):
    """
    Search for a kos entry for cmdr.
    """
    term = '%' + str(term) + '%'
    return session.query(KOS).filter(KOS.cmdr.ilike(term)).all()
