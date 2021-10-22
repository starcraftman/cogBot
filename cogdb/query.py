"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
import copy
import datetime
import logging
import os
import tempfile

import sqlalchemy as sqla
import sqlalchemy.exc as sqla_exc
import sqlalchemy.orm.exc as sqla_oexc

import cog.exc
import cog.sheets
import cog.util
import cogdb.eddb
from cog.util import substr_match
from cogdb.schema import (DiscordUser, FortSystem, FortPrep, FortDrop, FortUser, FortOrder,
                          EFortType, UMSystem, UMUser, UMHold, EUMSheet, EUMType, KOS,
                          AdminPerm, ChannelPerm, RolePerm,
                          TrackSystem, TrackSystemCached, TrackByID, OCRTracker, OCRTrigger,
                          OCRPrep, Global, Vote, EVoteType, Consolidation)
from cogdb.scanners import FortScanner


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


def dump_db(session):  # pragma: no cover
    """
    Purely debug function, shunts db contents into file for examination.
    """
    fname = os.path.join(tempfile.gettempdir(), 'dbdump_' + os.environ.get('COG_TOKEN', 'dev'))
    print("Dumping db contents to:", fname)
    with open(fname, 'w') as fout:
        for cls in [DiscordUser, FortUser, FortSystem, FortDrop, FortOrder,
                    UMUser, UMSystem, UMHold, KOS, AdminPerm, RolePerm, ChannelPerm]:
            fout.write('---- ' + str(cls) + ' ----\n')
            fout.writelines([str(obj) + "\n" for obj in session.query(cls)])


def get_duser(session, discord_id):
    """
    Return the DUser that has the same discord_id.

    Raises:
        NoMatch - No possible match found.
    """
    try:
        return session.query(DiscordUser).filter(DiscordUser.id == discord_id).one()
    except sqla_oexc.NoResultFound as exc:
        raise cog.exc.NoMatch(discord_id, 'DiscordUser') from exc


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


def add_duser(session, member):
    """
    Add a discord user to the database.
    """
    new_duser = DiscordUser(id=member.id, display_name=member.display_name,
                            pref_name=member.display_name)
    session.add(new_duser)
    session.commit()

    return new_duser


def users_with_all_merits(session):
    """
    Query and return the list of DiscordUsers by their total merit contributions.

    Args:
        session: A session onto the database.

    Returns:
        A list of objects of form: [[DiscordUser, total_merits], ... ]
    """
    return session.query(DiscordUser, (FortUser.dropped + UMUser.held + UMUser.redeemed).label('total')).\
        outerjoin(FortUser, DiscordUser.pref_name == FortUser.name).\
        outerjoin(UMUser, DiscordUser.pref_name == UMUser.name).\
        order_by(sqla.desc("total")).\
        all()


def users_with_fort_merits(session):
    """
    Query and return the list of DiscordUsers by their total fort contributions.

    Args:
        session: A session onto the database.

    Returns:
        A list of objects of form: [[DiscordUser, fort_merits], ... ]
    """
    return session.query(DiscordUser, FortUser.dropped).\
        join(FortUser, FortUser.name == DiscordUser.pref_name).\
        order_by(FortUser.dropped.desc()).\
        all()


def users_with_um_merits(session):
    """
    Query and return the list of DiscordUsers by their total undermining contributions.

    Args:
        session: A session onto the database.

    Returns:
        A list of objects of form: [[DiscordUser, um_merits], ... ]
    """
    return session.query(DiscordUser, (UMUser.held + UMUser.redeemed).label('um_merits')).\
        join(UMUser, UMUser.name == DiscordUser.pref_name).\
        filter(UMUser.sheet_src == EUMSheet.main).\
        order_by(sqla.desc('um_merits')).\
        all()


def check_pref_name(session, new_name):
    """
    Check that new name is not taken by another DUser or present as a stray in SheetRows.

    Raises:
        InvalidCommandArgs - DUser.pref_name taken by another DUser.
    """
    try:
        existing = session.query(DiscordUser).filter(DiscordUser.pref_name == new_name).one()
        raise cog.exc.InvalidCommandArgs(
            "Sheet name {}, taken by {}.\n\nPlease choose another.".format(
                new_name, existing.display_name))
    except sqla_oexc.NoResultFound:
        pass


def next_sheet_row(session, *, cls, start_row, sheet_src=None):
    """
    Find the next available row to add in the sheet based on entries.
    """
    rows_query = session.query(cls.row)
    if sheet_src:
        rows_query = rows_query.filter(cls.sheet_src == sheet_src)
    rows = [x[0] for x in rows_query.order_by(cls.row).all()]

    next_row = start_row
    if rows:
        complete_list = list(range(rows[0], rows[-1] + 2))
        next_row = sorted(list(set(complete_list) - set(rows)))[0]

    return next_row


def add_sheet_user(session, *, cls, discord_user, start_row, sheet_src=None):
    """
    Add a fort sheet user to system based on a Member.

    Kwargs:
        cls: The class of the sheet user like FortUser.
        duser: The DiscordUser object of the requesting user.
        start_row: Starting row if none inserted.
    """
    next_row = next_sheet_row(session, cls=cls, start_row=start_row, sheet_src=sheet_src)
    user = cls(name=discord_user.pref_name, cry=discord_user.pref_cry, row=next_row)
    if sheet_src:
        user.sheet_src = sheet_src
    session.add(user)
    session.commit()

    return user


def fort_get_medium_systems(session):
    """
    Return unfortified systems designated for small/medium ships.
    """
    return session.query(FortSystem).\
        filter(FortSystem.is_medium,
               sqla.not_(FortSystem.is_skipped),
               sqla.not_(FortSystem.is_fortified),
               sqla.not_(FortSystem.is_deferred)).\
        all()


def fort_get_systems(session, *, mediums=True, ignore_skips=True):
    """
    Return a list of all FortSystems. PrepSystems are not included.

    kwargs:
        mediums: If false, exclude all systems designated for j
                 Determined by "S/M" being in notes.
        ignore_skips: If True, ignore systems with notes containing: "eave for".
    """
    query = session.query(FortSystem).filter(FortSystem.type != 'prep')

    if ignore_skips:
        query = query.filter(sqla.not_(FortSystem.is_skipped))
    if not mediums:
        query = query.filter(sqla.not_(FortSystem.is_medium))

    return query.all()


def fort_get_preps(session):
    """
    Return a list of all PrepSystems.
    """
    return session.query(FortPrep).\
        filter(sqla.not_(FortPrep.is_fortified)).\
        all()


def fort_find_current_index(session):
    """
    Scan Systems from the beginning to find next unfortified target that is not Othime.

    Raises:
        NoMoreTargets - No more targets left OR a serious problem with data.
    """
    try:
        system_id = session.query(FortSystem.id).\
            filter(sqla.not_(FortSystem.is_fortified),
                   sqla.not_(FortSystem.is_skipped),
                   sqla.not_(FortSystem.is_deferred)).\
            first()
        return system_id[0] - 1
    except sqla_oexc.NoResultFound as exc:
        raise cog.exc.NoMoreTargets('No more fort targets at this time.') from exc


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
        return session.query(FortSystem).filter(FortSystem.name == system_name).one()
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
    log = logging.getLogger(__name__)
    states = {
        'cancelled': [],
        'fortified': [],
        'left': [],
        'undermined': [],
        'skipped': [],
        'almost_done': [],
    }

    for system in fort_get_systems(session, ignore_skips=False):
        log.info('STATE - %s', system)
        if system.is_fortified and system.is_undermined:
            states['cancelled'].append(system)
        elif system.is_undermined:
            states['undermined'].append(system)
        elif system.is_fortified:
            states['fortified'].append(system)
        elif system.is_skipped:
            states['skipped'].append(system)
        elif system.is_deferred:
            states['almost_done'].append(system)
        else:
            states['left'].append(system)

    return states


def fort_get_next_targets(session, *, offset=0, count=4):
    """
    Return the fort targets that need to be completed.
    If manual targets set, return those.
    Otherwise, return up to count systems that are not:
        - preps
        - priority or deferred systems

    Args:
        session: A session onto db.

    Kwargs:
        offset: If set, start offset forward from current active fort target. Default 0
        count: Return this many targets. Default 4
    """
    targets = fort_order_get(session)
    if not targets:
        targets = session.query(FortSystem).\
            filter(sqla.not_(FortSystem.is_skipped),
                   sqla.not_(FortSystem.is_priority),
                   sqla.not_(FortSystem.is_fortified),
                   sqla.not_(FortSystem.is_deferred)).\
            limit(count + offset).\
            all()

    return targets[offset:]


def fort_get_systems_x_left(session, left=None, *, include_preps=False):
    """
    Return all systems that have merits missing and
    less than or equal to left.

    Args:
        session: A session to the db.
        left: The amount that should be missing or less. If not passed, defer_missing constant.

    Kwargs:
        include_preps: By default preps not included, allows to override.
    """
    if not left:
        left = cog.util.CONF.defer_missing

    query = session.query(FortSystem).\
        filter(sqla.not_(FortSystem.is_skipped),
               sqla.not_(FortSystem.is_fortified),
               FortSystem.missing <= left)
    if not include_preps:
        query = query.filter(FortSystem.type != EFortType.prep)

    return query.all()


def fort_get_priority_targets(session):
    """
    Return all deferred targets under deferal amount.
    This will also return any systems that are prioritized.
    """
    priority = session.query(FortSystem).\
        filter(sqla.not_(FortSystem.is_skipped),
               sqla.not_(FortSystem.is_fortified),
               FortSystem.type != EFortType.prep,
               FortSystem.is_priority).\
        all()
    deferred = session.query(FortSystem).\
        filter(sqla.not_(FortSystem.is_skipped),
               sqla.not_(FortSystem.is_fortified),
               FortSystem.type != EFortType.prep,
               FortSystem.is_deferred).\
        all()

    return priority, deferred


def fort_add_drop(session, *, user, system, amount):
    """
    Add a Drop for 'amount' to the database where Drop intersects at:
        System.name and SUser.name
    If fort exists, increment its value. Else add it to database.

    Kwargs: system, user, amount

    Returns: The Drop object.

    Raises:
        InvalidCommandArgs: User requested an amount outside bounds [-MAX_DROP, MAX_DROP]
    """
    max_drop = cog.util.CONF.constants.max_drop
    if amount not in range(-1 * max_drop, max_drop + 1):
        raise cog.exc.InvalidCommandArgs('Drop amount must be in range [-{num}, {num}]'.format(num=max_drop))

    try:
        drop = session.query(FortDrop).\
            filter(FortDrop.user_id == user.id,
                   FortDrop.system_id == system.id).\
            one()
    except sqla_oexc.NoResultFound:
        drop = FortDrop(user_id=user.id, system_id=system.id, amount=0)
        session.add(drop)

    log = logging.getLogger(__name__)
    log.info('ADD_DROP - Before: Drop %s, System %s', drop, system)
    drop.amount = max(0, drop.amount + amount)
    system.fort_status = system.fort_status + amount
    session.commit()
    log.info('ADD_DROP - After: Drop %s, System %s', drop, system)

    return drop


def fort_order_remove_finished(session):
    """
    Clean up any FortOrders that have been completed.
    Deletions will be comitted.
    """
    for fort_order in session.query(FortOrder).order_by(FortOrder.order):
        if fort_order.system.is_fortified or fort_order.system.is_deferred:
            session.delete(fort_order)

    session.commit()


def fort_order_get(session):
    """
    Get the order of systems to fort.

    Returns: [] if no systems set, else a list of System objects.
    """
    return session.query(FortSystem).\
        join(FortOrder, FortSystem.name == FortOrder.system_name).\
        order_by(FortOrder.order).\
        all()


def fort_order_set(session, systems):
    """
    Simply set the systems in the order desired.

    Ensure systems are actually valid before.
    """
    try:
        for ind, system in enumerate(systems, start=1):
            if not isinstance(system, FortSystem):
                system = fort_find_system(session, system).name
            session.add(FortOrder(order=ind, system_name=system))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError) as exc:
        session.rollback()
        raise cog.exc.InvalidCommandArgs("Duplicate system specified, check your command!") from exc
    except cog.exc.NoMatch as exc:
        session.rollback()
        raise cog.exc.InvalidCommandArgs("FortSystem '{}' not found in fort systems.".format(system)) from exc


def fort_order_drop(session):
    """
    Drop the existing FortOrder table.
    """
    session.query(FortOrder).delete()
    session.commit()


def um_find_system(session, system_name, *, sheet_src=EUMSheet.main):
    """
    Find the UMSystem with system_name
    """
    try:
        return session.query(UMSystem).\
            filter(UMSystem.name == system_name,
                   UMSystem.sheet_src == sheet_src).\
            one()
    except (sqla_oexc.NoResultFound, sqla_oexc.MultipleResultsFound) as exc:
        systems = session.query(UMSystem).\
            filter(UMSystem.name.ilike('%{}%'.format(system_name)),
                   UMSystem.sheet_src == sheet_src).\
            all()

        if len(systems) > 1:
            raise cog.exc.MoreThanOneMatch(system_name, systems, UMSystem) from exc

        if len(systems) == 0:
            raise cog.exc.NoMatch(system_name, UMSystem) from exc

        return systems[0]


def um_get_systems(session, *, exclude_finished=True, sheet_src=EUMSheet.main, ignore_leave=True):
    """
    Return a list of all current undermining targets.

    kwargs:
        exclude_finished: Return only active UM targets.
        sheet_src: Select UM targets from sheet_src, default main.
        ignore_leave: Ignore the systems with like "leave for now" in notes.
    """
    systems = session.query(UMSystem).\
        filter(UMSystem.sheet_src == sheet_src)

    if ignore_leave:
        systems = systems.filter(sqla.not_(UMSystem.is_skipped))
    systems = systems.all()

    if exclude_finished:
        # Force in memory check, due to differing implementation of is_undermined
        systems = [x for x in systems if not x.is_undermined]

    return systems


def um_reset_held(session, user, *, sheet_src=EUMSheet.main):
    """
    Reset all held merits to 0.
    """
    holds = session.query(UMHold).\
        filter(UMHold.user_id == user.id,
               UMHold.sheet_src == sheet_src).all()
    for hold in holds:
        hold.held = 0

    session.commit()
    return holds


def um_redeem_merits(session, user, *, sheet_src=EUMSheet.main):
    """
    Redeem all held merits for user.
    """
    total = 0
    holds = session.query(UMHold).\
        filter(UMHold.user_id == user.id).\
        all()
    for hold in holds:
        total += hold.held
        hold.redeemed += hold.held
        hold.held = 0

    session.commit()
    return (holds, total)


def um_redeem_systems(session, user, systems, *, sheet_src=EUMSheet.main):
    """
    Redeem merits only for the specified user and the systems that matched exactly.
    """
    total = 0
    subq = session.query(UMSystem.id).\
        filter(UMSystem.name.in_(systems),
               UMSystem.sheet_src == sheet_src).\
        scalar_subquery()
    holds = session.query(UMHold).filter(UMHold.user_id == user.id,
                                         UMHold.system_id.in_(subq)).all()
    for hold in holds:
        total += hold.held
        hold.redeemed = hold.redeemed + hold.held
        hold.held = 0

    session.commit()
    return (holds, total)


def um_add_system_targets(session, um_systems):
    """
    Given a set of um targets, add them to the database.

    Args:
        session: A session onto the db.
        um_systems: A list of dictionary objects of form:
            {
                "sys_name": system.name,
                "power": power[0],
                "security": security,
                "trigger": reinforced_trigger,
                "priority": priority,
            }
    """
    last = session.query(UMSystem).\
        filter(UMSystem.sheet_src == EUMSheet.main).\
        order_by(UMSystem.id.desc()).\
        limit(1).\
        all()

    last_id = 1
    last_col = cog.sheets.Column("D")
    if last:
        last_id = last[0].id
        last_col = cog.sheets.Column(last[0].sheet_col)

    systems = []
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        for sys in um_systems:
            nearest_controls = cogdb.eddb.get_nearest_controls(
                eddb_session, centre_name=sys['sys_name'], limit=1
            )
            last_id += 1
            # Each new system uses 2 columns past the last
            systems.append(UMSystem(
                id=last_id,
                sheet_src=EUMSheet.main,
                type=EUMType.control,
                name=sys['sys_name'],
                sheet_col=last_col.offset(2),
                close_control=nearest_controls[0].name,
                security=sys.pop('security'),
                goal=sys['trigger'],
                notes=sys['power'],
                priority=sys['priority'],
            ))
    session.add_all(systems)


def um_add_hold(session, *, sheet_src=EUMSheet.main, **kwargs):
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
        hold = session.query(UMHold).\
            filter(UMHold.user_id == user.id,
                   UMHold.system_id == system.id,
                   UMHold.sheet_src == sheet_src).\
            one()
    except sqla_oexc.NoResultFound:
        hold = UMHold(user_id=user.id, system_id=system.id, held=0, redeemed=0, sheet_src=sheet_src)
        session.add(hold)

    hold.held = held
    session.commit()

    return hold


def um_all_held_merits(session, *, sheet_src=EUMSheet.main):
    """
    Return a list of lists that show all users with merits still held.

    Systems excluded if:
        - System set to "Leave for now"
        - No held merits & system is undermined.

    List of the form:
    [
        [CMDR, system_name_1, system_name_2, ...],
        [cmdrname, merits_system_1, merits_system_2, ...],
        [cmdrname, merits_system_1, merits_system_2, ...],
    ]
    """
    systems = session.query(UMSystem).\
        filter(UMSystem.sheet_src == EUMSheet.main,
               sqla.not_(UMSystem.is_skipped),
               sqla.or_(UMSystem.held_merits > 0,
                        sqla.not_(UMSystem.is_undermined))).\
        order_by(UMSystem.id).\
        all()
    system_ids = [x.id for x in systems]
    held_merits = session.query(UMHold).\
        filter(UMHold.held > 0,
               UMHold.sheet_src == sheet_src,
               UMHold.system_id.in_(system_ids)).\
        order_by(UMHold.system_id).\
        all()

    c_dict = {}
    for merit in held_merits:
        try:
            c_dict[merit.user.name][merit.system.name] = merit
        except KeyError:
            c_dict[merit.user.name] = {merit.system.name: merit}

    rows = []
    system_names = [sys.name for sys in systems]
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
        return session.query(AdminPerm).filter(AdminPerm.id == member.id).one()
    except sqla_oexc.NoResultFound as exc:
        raise cog.exc.NoMatch(member.display_name, 'Admin') from exc


def add_admin(session, member):
    """
    Add a new admin.
    """
    try:
        session.add(AdminPerm(id=member.id))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError) as exc:
        raise cog.exc.InvalidCommandArgs("Member {} is already an admin.".format(member.display_name)) from exc


def show_guild_perms(session, guild, prefix='!'):
    """
    Find all existing rules and format a summary to display.

    Args:
        session: A session onto the db.
        guild: The guild that set restrictions.

    Returns: A formatted string summarizing rules for guild.
    """
    msg = f"__Existing Rules For {guild.name}__"

    rules = session.query(ChannelPerm).filter(ChannelPerm.guild_id == guild.id).all()
    if rules:
        msg += "\n\n__Channel Rules__\n"
        for rule in rules:
            msg += "`{prefix}{cmd}` limited to channel: {chan}\n".format(prefix=prefix, cmd=rule.cmd, chan=guild.get_channel(rule.channel_id).mention)

    rules = session.query(RolePerm). filter(RolePerm.guild_id == guild.id).all()
    if rules:
        msg += "\n\n__Role Rules__\n"
        for rule in rules:
            msg += "`{prefix}{cmd}` limited to role: {role}\n".format(prefix=prefix, cmd=rule.cmd, role=guild.get_role(rule.role_id).name)

    return msg.rstrip()


def add_channel_perms(session, cmds, guild, channels):
    """
    Add channel restrictions to an existing commands.

    Args:
        session: A session onto the db.
        cmds: A list of command names, as seen by user (i.e. ['fort', 'um']).
        guild: The guild to set restrictions.
        channels: A list of Channels on the guild where the commands should be restricted.

    Raises:
        InvalidCommandArgs: Tells user of any existing rules, adds all others.
    """
    msg = ""
    for cmd in cmds:
        for channel in channels:
            try:
                session.add(ChannelPerm(cmd=cmd, guild_id=guild.id, channel_id=channel.id))
                session.commit()
            except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
                msg += f"Channel permission exists for: {cmd} on {channel.name}\n"

    if msg:
        raise cog.exc.InvalidCommandArgs("Existing rules below, remaining rules added:\n\n" + msg)


def add_role_perms(session, cmds, guild, roles):
    """
    Add role restrictions to existing commands.

    Args:
        session: A session onto the db.
        cmds: A list of command names, as seen by user (i.e. ['fort', 'um']).
        guild: The guild to set restrictions.
        roles: A list of Roles on the guild where the commands should be restricted.

    Raises:
        InvalidCommandArgs: Tells user of any existing rules, adds all others.
    """
    msg = ""
    for cmd in cmds:
        for role in roles:
            try:
                session.add(RolePerm(cmd=cmd, guild_id=guild.id, role_id=role.id))
                session.commit()
            except (sqla_exc.IntegrityError, sqla_oexc.FlushError):
                msg += f"Role permission exists for: {cmd} on {role.name}\n"

    if msg:
        raise cog.exc.InvalidCommandArgs("Existing rules below, remaining rules added:\n\n" + msg)


def remove_channel_perms(session, cmds, guild, channels):
    """
    Remove channel restrictions to existing commands.
    Attempting to remove non existant rules is ignored silently.

    Args:
        session: A session onto the db.
        cmds: A list of command names, as seen by user (i.e. ['fort', 'um']).
        guild: The guild to set restrictions.
        roles: A list of Roles on the guild where the commands should be restricted.
    """
    for cmd in cmds:
        for channel in channels:
            try:
                session.query(ChannelPerm).\
                    filter(ChannelPerm.cmd == cmd,
                           ChannelPerm.guild_id == guild.id,
                           ChannelPerm.channel_id == channel.id).\
                    delete()
            except sqla_oexc.NoResultFound:
                pass
    session.commit()


def remove_role_perms(session, cmds, guild, roles):
    """
    Remove role restrictions to existing commands.
    Attempting to remove non existant rules is ignored silently.

    Args:
        session: A session onto the db.
        cmds: A list of command names, as seen by user (i.e. ['fort', 'um']).
        guild: The guild to set restrictions.
        roles: A list of Roles on the guild where the commands should be restricted.
    """
    for cmd in cmds:
        for role in roles:
            try:
                session.query(RolePerm).\
                    filter(RolePerm.cmd == cmd,
                           RolePerm.guild_id == guild.id,
                           RolePerm.role_id == role.id).\
                    delete()
            except sqla_oexc.NoResultFound:
                pass
    session.commit()


def check_perms(session, msg, cmd):
    """
    Check if a user is authorized to issue this command.
    Checks will be made against channel and user roles.

    Raises InvalidPerms if any permission issue.
    """
    check_channel_perms(session, cmd, msg.channel.guild, msg.channel)
    check_role_perms(session, cmd, msg.channel.guild, msg.author.roles)


def check_channel_perms(session, cmd, server, channel):
    """
    A user is allowed to issue a command if:
        a) no restrictions for the cmd
        b) the channel is whitelisted in the restricted channels

    Raises InvalidPerms if fails permission check.
    """
    perms = session.query(ChannelPerm).\
        filter(ChannelPerm.cmd == cmd,
               ChannelPerm.guild_id == server.id).\
        all()
    channels = [perm.channel_id for perm in perms]
    if channels and channel.id not in channels:
        raise cog.exc.InvalidPerms("The '{}' command is not permitted on this channel.".format(
            cmd.lower()))


def check_role_perms(session, cmd, server, member_roles):
    """
    A user is allowed to issue a command if:
        a) no roles set for the cmd
        b) he matches ANY of the set roles

    Raises InvalidPerms if fails permission check.
    """
    perms = session.query(RolePerm).\
        filter(RolePerm.cmd == cmd,
               RolePerm.guild_id == server.id).\
        all()
    perm_roles = {perm.role_id for perm in perms}
    member_roles = {role.id for role in member_roles}
    if perm_roles and len(member_roles - perm_roles) == len(member_roles):
        raise cog.exc.InvalidPerms("You do not have the roles for the command.")


def complete_control_name(partial, include_winters=False):
    """
    Provide name completion of Federal controls without db query.
    """
    with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
        systems = cogdb.eddb.get_controls_of_power(eddb_session, power='%hudson')
        if include_winters:
            systems += cogdb.eddb.get_controls_of_power(eddb_session, power='%winters')

    return fuzzy_find(partial, systems)


def kos_search_cmdr(session, term):
    """
    Search for a kos entry for cmdr.
    """
    term = '%' + str(term) + '%'
    return session.query(KOS).filter(KOS.cmdr.ilike(term)).all()


def kos_add_cmdr(session, kos_info):
    """
    Add a kos entry to the local database.

    args:
        cmdr: The cmdr name.
        faction: The faction in question.
        reason: The reason for addition if provided.
        is_friendly: If this user should be treated as friendly.
    """
    return session.add(KOS(cmdr=kos_info['cmdr'], squad=kos_info['squad'],
                           reason=kos_info['reason'], is_friendly=kos_info['is_friendly']))


def track_add_systems(session, systems, distance):
    """
    Add all systems specified to tracking with distance specified.

    Returns: List of added system names.
    """
    track_systems = track_get_all_systems(session)
    track_systems = [x.system for x in track_systems]
    to_add = set(systems) - set(track_systems)
    added = [TrackSystem(system=x, distance=distance) for x in to_add]
    session.add_all(added)

    return added


def track_remove_systems(session, systems):
    """
    Remove all systems specified to tracking with distance specified.

    Returns: [system_name, system_name, ...]
    """
    track_systems = session.query(TrackSystem).\
        filter(TrackSystem.system.in_(systems)).\
        all()

    removed = []
    for sys in track_systems:
        session.delete(sys)
        removed += [sys.system]

    return removed


def track_get_all_systems(session):
    """
    Provide a complete list of all systems under tracking.

    Returns: [TrackSystem, TrackSystem, ...]
    """
    return session.query(TrackSystem).all()


def track_show_systems(session):
    """
    Format into the smallest number of messages possible the list of current IDs.
    """
    track_systems = session.query(TrackSystem).\
        order_by(TrackSystem.system).\
        all()

    msgs = []
    cur_msg = "__Tracking System Rules__\n"
    pad = " " * 4
    for track in track_systems:
        cur_msg += "\n{}{}".format(pad, str(track))
        if len(cur_msg) > cog.util.MSG_LIMIT:
            msgs += [cur_msg]
            cur_msg = ""

    if cur_msg:
        msgs += [cur_msg]

    return msgs


def track_systems_computed_add(session, systems, centre):
    """
    Add to the computed systems database area, merge in new systems.
    For all systems existing, add centre to overlap.

    Args:
        session: The session to the db.
        systems: The system names to add.
        centre: The centre system associated with these systems.

    Returns: (added, modified)
        added: A list of system names added to tracking.
        modified: A list of system names where overlap was incremented.
    """
    existing = session.query(TrackSystemCached).\
        filter(TrackSystemCached.system.in_(systems)).\
        all()

    for system in existing:
        system.add_overlap(centre)

    to_add = set(systems) - {x.system for x in existing}
    added = [TrackSystemCached(system=x, overlaps_with=centre) for x in to_add]
    session.add_all(added)

    return [x.system for x in added], [x.system for x in existing]


def track_systems_computed_remove(session, centre):
    """
    Update the computed systems database area.
    Remove all systems that no longer need tracking once centre removed.
    For remaining systems update overlap tracking.

    Args:
        session: The session to the db.
        centre: The centre system associated with the systems to update.

    Returns: (deleted, modified)
        deleted: A list of system names removed from tracking.
        modified: A list of system names where overlap was reduced.
    """
    existing = session.query(TrackSystemCached).\
        filter(TrackSystemCached.overlaps_with.ilike('%{}%'.format(centre))).\
        all()

    deleted, modified = [], []
    for system in existing:
        if system.remove_overlap(centre):
            session.delete(system)
            deleted += [system.system]
        else:
            modified += [system.system]

    return deleted, modified


def track_systems_computed_check(session, system_name):
    """
    Check if a system is under tracking.

    Returns: True iff the system is to track.
    """
    try:
        return session.query(TrackSystemCached).filter(TrackSystemCached.system == system_name).one()
    except sqla_exc.NoResultFound:
        return None


def track_ids_update(session, ids_dict, date_obj=None):
    """
    Update the tracked IDs into the database.
    Important: IGNORE updates where system in data is same as in db.
    ids_dict is a dict of form:
        {
            {ID: {'id': ID, 'group': GROUP, 'system': SYSTEM, 'override': False, 'updated_at': datetime obj},
            ...
        }

    ID, GROUP, SYSTEM and OVERRIDE are the respective data to store for an ID in schema:
    If the information exists it will be updated, else inserted.

    Args:
        session: The session to db.
        ids_dict: See above dictionary.
        date_obj: Optional, if provided data will be accepted only if timestamp is newer. Expecting datetime object.

    Returns: (list_updated, list_removed) - both lists are lists of IDs
    """
    added, updated = [], []
    track_ids = session.query(TrackByID).\
        filter(TrackByID.id.in_(ids_dict.keys())).\
        all()

    copy_ids_dict = copy.deepcopy(ids_dict)
    for track in track_ids:
        data = copy_ids_dict[track.id]
        new_system = data.get('system', track.system)

        # Reject possible data that is older than current
        if date_obj and track.updated_at > date_obj or track.system == new_system:
            del copy_ids_dict[track.id]
            continue

        track.updated_at = data.get('updated_at', date_obj)
        track.squad = data.get("squad", track.squad)
        track.override = data.get("override", track.override)
        track.spotted(new_system)
        updated += [track.id]

        del copy_ids_dict[track.id]

    for data in copy_ids_dict.values():
        session.add(TrackByID(**data))
        added += [data['id']]

    return (updated, added)


def track_ids_remove(session, ids):
    """
    Remove from tracking the current

    Returns: [id_removed, id_removed, ...]
    """
    track_ids = session.query(TrackByID).\
        filter(TrackByID.id.in_(ids)).\
        all()
    for tid in track_ids:
        session.delete(tid)

    return [x.id for x in track_ids]


def track_ids_check(session, id):
    """
    Return True iff the id is set to be manually tracked via override.
    """
    try:
        return session.query(TrackByID).filter(TrackByID.id == id, TrackByID.override).one()
    except sqla_exc.NoResultFound:
        return None


def track_ids_show(session):
    """
    Format into the smallest number of messages possible the list of current IDs.

    Args: ids - select only these ids. If not provided, show all.

    Returns: Series of messages < 1900 that can be sent.
    """
    track_ids = session.query(TrackByID).all()

    msgs = []
    cur_msg = "__Tracking IDs__\n"
    for track in track_ids:
        cur_msg += "\n" + str(track)
        if len(cur_msg) > cog.util.MSG_LIMIT:
            msgs += [cur_msg]
            cur_msg = ""

    if cur_msg:
        msgs += [cur_msg]

    return msgs


def track_ids_newer_than(session, date):
    """
    Query the database for all TrackByIDs that occured after a date.
    """
    return session.query(TrackByID).\
        filter(TrackByID.updated_at > date).\
        order_by(TrackByID.updated_at).\
        all()


def update_ocr_live(session, trackers_dict, sheet_date=None):
    """
    Update the tracked IDs into the database.
    tracker_dict is a dict of form:
        {
            SYSTEM:
                {
                    'system': SYSTEM,
                    'fort': FORT,
                    'um': UM,
                    'updated_at': UPDATED_AT,
                },
            ...
        }

    FORT, UM and UPDATED_AT are the respective data to store for a SYSTEM in schema:
    If the information exists it will be updated, else inserted.
    If updated_at not present but sheet_date is, sheet_date will be set as updated_at time.

    Args:
        session: The session to db.
        tracker_dict: See above dictionary.
        sheet_date: Optionally will be used if no 'updated_at' key in dict.

    Returns: (updated, removed) - Both lists are SYSTEM names (str).
    """
    added, updated = [], []
    ocr_systems = session.query(OCRTracker).\
        filter(OCRTracker.system.in_(trackers_dict.keys())).\
        all()

    copy_tracker_dict = copy.deepcopy(trackers_dict)
    for system in ocr_systems:
        # Reject possible data that is older than current
        if sheet_date and system.updated_at >= sheet_date:
            del copy_tracker_dict[system.id]
            continue

        data = copy_tracker_dict[system.system]

        data['updated_at'] = data.get('updated_at', sheet_date)
        try:
            system.update(**data)
            updated += [system.system]
        except cog.exc.ValidationFail:
            pass

        del copy_tracker_dict[system.system]

    for data in copy_tracker_dict.values():
        try:
            session.add(OCRTracker(**data))
            added += [data['system']]
        except cog.exc.ValidationFail:
            pass

    return (updated, added)


def update_ocr_trigger(session, trigger_dict, sheet_date=None):
    """
    Update the tracked IDs into the database.
    trigger_dict is a dict of form:
        {
            SYSTEM: {
                'system': SYSTEM,
                'fort_trigger': FORT_TRIGGER,
                'um_trigger': UM_TRIGGER,
                'base_income': BASE_INCOME,
                'last_upkeep': LAST_CYCLE_UPKEEP,
                'updated_at': UPDATED_AT,
                },
            ...
        }

    FORT_TRIGGER, UM_TRIGGER, BASE_INCOME, LAST_CYCLE_UPKEEP and UPDATED_AT are the
    respective data to store for a SYSTEM in the dict.
    If the information exists it will be updated, else inserted.

    Args:
        session: The session to db.
        trigger_dict: See above dictionary.
        sheet_date: Optionally will be used if no 'updated_at' key in dict.

    Returns: (updated, removed) - Both lists are SYSTEM names (str).
    """
    added, updated = [], []
    ocr_systems = session.query(OCRTrigger).\
        filter(OCRTrigger.system.in_(trigger_dict.keys())).\
        all()

    copy_trigger_dict = copy.deepcopy(trigger_dict)
    for system in ocr_systems:
        data = copy_trigger_dict[system.system]

        data['updated_at'] = data.get('updated_at', sheet_date)
        try:
            system.update(**data)
            updated += [system.system]
        except cog.exc.ValidationFail:
            pass

        del copy_trigger_dict[system.system]

    for data in copy_trigger_dict.values():
        try:
            session.add(OCRTrigger(**data))
            added += [data['system']]
        except cog.exc.ValidationFail:
            pass

    return (updated, added)


def update_ocr_prep(session, prep_dict, sheet_date=None):
    """
    Update the tracked IDs into the database.
    prep_dict is a dict of form:
        {
            SYSTEM: {
                'system': SYSTEM,
                'merits': MERITS,
                'updated_at': datetime obj
            },
            ...
        }

    SYSTEM and MERITS are the respective data to store for an SYSTEM in schema:
    If the information exists it will be updated, else inserted.

    Args:
        session: The session to db.
        prep_dict: See above dictionary.
        sheet_date: Optionally will be used if no 'updated_at' key in dict.

    Returns: (updated, removed) - Both lists are SYSTEM names (str).
    """
    added, updated = [], []
    ocr_systems = session.query(OCRPrep).\
        filter(OCRPrep.system.in_(prep_dict.keys())).\
        all()

    copy_prep_dict = copy.deepcopy(prep_dict)
    for system in ocr_systems:
        # Reject possible data that is older than current
        if sheet_date and system.updated_at >= sheet_date:
            del copy_prep_dict[system.system]
            continue

        data = copy_prep_dict[system.system]

        data['updated_at'] = data.get('updated_at', sheet_date)
        try:
            system.update(**data)
            updated += [system.system]
        except cog.exc.ValidationFail:
            pass

        del copy_prep_dict[system.system]

    for data in copy_prep_dict.values():
        try:
            session.add(OCRPrep(**data))
            added += [data['system']]
        except cog.exc.ValidationFail:
            pass

    return (updated, added)


def get_oldest_ocr_trigger(session):
    """
    Return the oldest OCRTrigger entry. If no entries, returns None.

    Args:
        session: The session to db.
    """
    try:
        return session.query(OCRTrigger).\
            order_by(OCRTrigger.updated_at.asc()).\
            limit(1).\
            one()
    except sqla_oexc.NoResultFound:
        return None


def get_current_global(session):
    """
    Return the current global for this cycle.
    If none present, generate one.

    Returns: The current Global
    """
    try:
        globe = session.query(Global).one()
    except sqla_oexc.NoResultFound:
        globe = Global()
        session.add(globe)
        session.flush()

    return globe


def ocr_update_fort_status(session):
    """
    Iterate every fort in the system and update fort_status, um_status and triggers if needed.
    For any system that is updated generate an update_system_dict to be sent in batch.

    Args:
        session: A session for the database.

    Returns: (cell_updates, warnings)
        cell_updates: A list of FortScanner.update_system_dicts that will update the sheet with OCR changes.
        warnings: A list of warnings about NEWLY undermined systems.
    """
    cell_updates = []

    for sys in session.query(FortSystem):
        if not sys.ocr_tracker:
            continue
        changed = False

        if sys.ocr_tracker.fort > sys.fort_status:
            sys.fort_status = sys.ocr_tracker.fort
            changed = True
        if sys.ocr_tracker.um > sys.um_status:
            sys.um_status = sys.ocr_tracker.um
            changed = True

        if changed:
            cell_updates += FortScanner.update_system_dict(sys.sheet_col, sys.fort_status, sys.um_status)

    return cell_updates


def ocr_prep_report(session):
    """
    Generate a small report on the preps currently tracked.

    Args:
        session: A session for the database.

    Returns: Report on current consolidation and prep merits. (String)
    """
    globe = get_current_global(session)
    msg = """__Hudson Preps Report__

Current Consolidation: {}%
""".format(globe.consolidation)
    for prep in session.query(OCRPrep).order_by(OCRPrep.merits.desc()).all():
        msg += "\n" + str(prep)

    return msg


def post_cycle_db_cleanup(session):
    """
    Cleanup the database post cycle change:
        Zero out existing OCRTracker objects fort and um fields.
        Delete all votes of last cycle.

    Args:
        session: Session on to the db.
    """
    for tracker in session.query(OCRTracker):
        tracker.fort = 0
        tracker.um = 0
    session.query(Vote).delete()
    session.commit()


def add_vote(session, discord_id, vote_type, amount):
    """
    Cast a vote.
    """
    the_vote = get_vote(session, discord_id, vote_type)
    the_vote.update_amount(amount)
    session.commit()

    return the_vote


def get_vote(session, discord_id, vote_type):
    """
    Get if user in Vote DB already.
    """
    try:
        the_vote = session.query(Vote).\
            filter(Vote.id == discord_id, Vote.vote == vote_type).\
            one()
    except sqla_oexc.NoResultFound:
        the_vote = Vote(id=discord_id, vote=vote_type, amount=0)
        session.add(the_vote)

    return the_vote


def get_all_votes(session):
    """
    Get all current votes, ordered by name.
    """
    return session.query(Vote, DiscordUser).\
        join(DiscordUser, Vote.id == DiscordUser.id).\
        order_by(Vote.updated_at.desc()).\
        all()


def get_cons_prep_totals(session):
    """
    Compute the current total of cons and prep votes by query.

    Returns: cons_total, prep_total
    """
    try:
        cons_total, prep_total = session.query(sqla.func.ifnull(sqla.func.sum(Vote.amount), 0)).\
            group_by(Vote.vote).\
            order_by(Vote.vote).\
            all()
        cons_total, prep_total = int(cons_total[0]), int(prep_total[0])
    except ValueError:
        cons_total, prep_total = 0, 0

    return cons_total, prep_total


def get_all_snipe_holds(session):
    """
    Args:
        session: A session onto the db.

    Returns: A list of UMHolds for the snipe sheet. Empty by default.
    """
    return session.query(UMHold).\
        filter(UMHold.sheet_src == EUMSheet.snipe,
               UMHold.held > 0).\
        all()


def get_snipe_members_holding(session, guild):
    """
    Find the members who are holding merits on the snipe sheet.
    For each found member, attempt to resolve them with the guild to mention them.
    If we cannot resolve their name with guild, just write the name.
    Format one large message reminding the users and return it.

    Args:
        session: A session onto the db.
        guild: The guild of the server in question.

    Returns:
        A string formatted mentioning or naming all users with snipe merits.
    """
    template_msg = "{} is holding {} merits in {}\n"
    reply = ""
    for hold in get_all_snipe_holds(session):
        duser = hold.user.discord_user
        found = guild.get_member_named(duser.pref_name)
        mention = found.mention if found else duser.pref_name
        reply += template_msg.format(mention, hold.held, hold.system.name)

    return reply


def get_consolidation_this_week(session):
    """
    Return all consolidation tracking data since the last tick.
    """
    last_tick = cog.util.next_weekly_tick(datetime.datetime.utcnow(), -1)

    return session.query(Consolidation).\
        filter(Consolidation.updated_at > last_tick).\
        order_by(Consolidation.updated_at.asc()).\
        all()
