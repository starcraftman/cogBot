"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
import contextlib
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
import cogdb.spy_squirrel as spy
from cog.util import fuzzy_find
from cogdb.schema import (DiscordUser, FortSystem, FortPrep, FortDrop, FortUser, FortOrder,
                          EFortType, UMSystem, UMUser, UMHold, EUMSheet, EUMType, KOS,
                          AdminPerm, ChannelPerm, RolePerm,
                          TrackSystem, TrackSystemCached, TrackByID,
                          Global, Vote, EVoteType, Consolidation,
                          SheetRecord)


def dump_db(session):  # pragma: no cover
    """
    Purely debug function, shunts db contents into file for examination.
    """
    fname = os.path.join(tempfile.gettempdir(), 'dbdump_' + os.environ.get('TOKEN', 'dev'))
    print("Dumping db contents to:", fname)
    with open(fname, 'w', encoding='utf-8') as fout:
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
            f"Sheet name {new_name}, taken by {existing.display_name}.\n\nPlease choose another.")
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
        return fuzzy_find(system_name, systems, obj_attr='name', obj_type='System')


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
    Return the next fort targets that need to be completed.
    First consider any manually set fort systems if uncompleted.
    Otherwise, return up to count systems that are not:
        - preps
        - priority or deferred systems

    Args:
        session: A session onto db.
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
    to_remove = session.query(FortOrder).\
        join(FortSystem, FortOrder.system_name == FortSystem.name).\
        filter(FortSystem.is_fortified).\
        all()
    for fort_order in to_remove:
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
        raise cog.exc.InvalidCommandArgs(f"FortSystem '{system}' not found in fort systems.") from exc


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
            filter(UMSystem.name.ilike(f'%{system_name}%'),
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
        filter(UMHold.user_id == user.id,
               UMHold.sheet_src == sheet_src).\
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
    for cmdr, cmdr_systems in c_dict.items():
        row = [cmdr]
        for system_name in system_names:
            try:
                row += [cmdr_systems[system_name].held]
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
        raise cog.exc.InvalidCommandArgs(f"Member {member.display_name} is already an admin.") from exc


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
            msg += f"`{prefix}{rule.cmd}` limited to channel: {guild.get_channel(rule.channel_id).mention}\n"

    rules = session.query(RolePerm). filter(RolePerm.guild_id == guild.id).all()
    if rules:
        msg += "\n\n__Role Rules__\n"
        for rule in rules:
            msg += f"`{prefix}{rule.cmd}` limited to role: {guild.get_role(rule.role_id).name}\n"

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


def check_perms(msg, cmd):
    """
    Check if a user is authorized to issue this command.
    Checks will be made against channel and user roles.

    Raises InvalidPerms if any permission issue.
    """
    with cogdb.session_scope(cogdb.Session) as session:
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
        raise cog.exc.InvalidPerms(f"The '{cmd.lower()}' command is not permitted on this channel.")


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

    obj_type = "Constrol Systems of Hudson" + " and Winters" if include_winters else ""
    return fuzzy_find(partial, systems, obj_type=obj_type)


def kos_kill_list(session):
    """
    Determine if a given commander is on the kill list.

    Args:
        session: A session onto the database.

    Returns: The full list of all CMDRs on the kill list.
    """
    return [x[0] for x in session.query(KOS.cmdr).filter(sqla.not_(KOS.is_friendly))]


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
        cur_msg += f"\n{pad}{track}"
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
        filter(TrackSystemCached.overlaps_with.ilike(f'%{centre}%')).\
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

    for carrier_id, data in ids_dict.items():
        try:
            track = session.query(TrackByID).\
                filter(TrackByID.id == carrier_id).\
                one()

            # Reject data that is older than current or
            if date_obj and track.updated_at > date_obj:
                continue

            track.updated_at = data.get('updated_at', date_obj if date_obj else track.updated_at)
            if track.system == data.get("system"):
                continue

            track.squad = data.get("squad", track.squad)
            track.override = data.get("override", track.override)
            track.spotted(data.get("system", track.system))
            updated += [carrier_id]
        except sqla.exc.NoResultFound:
            session.add(TrackByID(**data))
            added += [carrier_id]

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


def track_ids_check(session, cid):
    """
    Return True iff the id is set to be manually tracked via override.
    """
    try:
        return session.query(TrackByID).filter(TrackByID.id == cid, TrackByID.override).one()
    except sqla_exc.NoResultFound:
        return None


@contextlib.contextmanager
def track_ids_show(session, *, override_only=False):
    """
    Show the current location of all tracked IDs.
    Tracked carriers are sorted by squadron and then ID.
    This is a context manager and returns a temporary filename to transmit.

    Args:
        session: A session onto the database.
        override_only: When true, return only the specifically flagged carriers.

    Returns: A single filename with all information requested.
    """
    track_ids = session.query(TrackByID)
    if override_only:
        track_ids = track_ids.filter(TrackByID.override)
    track_ids = track_ids.order_by(TrackByID.squad, TrackByID.id).all()

    msg = "__Tracking IDs__\n\n"
    msg += '\n'.join([f'    {track}' for track in track_ids])
    with tempfile.NamedTemporaryFile(mode='w') as tfile:
        tfile.write(msg)
        tfile.flush()
        yield tfile.name


def track_ids_newer_than(session, date):
    """
    Query the database for all TrackByIDs that occured after a date.

    Args:
        session: A session onto the database.
        date: A timestamp, select all those updated after this date.

    Returns: A list of TrackByID objects.
    """
    return session.query(TrackByID).\
        filter(TrackByID.updated_at > date).\
        order_by(TrackByID.updated_at).\
        all()


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


def post_cycle_db_cleanup(session, eddb_session):
    """
    Cleanup the database post cycle change:
        Remove FortOrder and Votes from db.
        Remove SpyPreps, SpyVotes and SpySystems from EDDB db.

    Args:
        session: Session on to the db.
        eddb_session: Session on to the EDDB db.
    """
    for cls in [FortOrder, Vote]:
        session.query(cls).delete()
    for cls in [spy.SpyPrep, spy.SpyVote, spy.SpySystem]:
        eddb_session.query(cls).delete()


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
    cons_total, prep_total = 0, 0
    results = session.query(Vote.vote, sqla.func.sum(Vote.amount)).\
        group_by(Vote.vote).\
        order_by(Vote.vote).\
        all()

    for result in results:
        if result[0] == EVoteType.cons:
            cons_total = int(result[1])
        else:
            prep_total = int(result[1])

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
        order_by(UMHold.user_id, UMHold.system_id).\
        all()


def get_snipe_members_holding(session):
    """
    Find the members who are holding merits on the snipe sheet.
    For each found member, attempt to resolve them with the guild to mention them.
    If we cannot resolve their name with guild, just write the name.
    Format as many messages as needed and return the list.

    Args:
        session: A session onto the db.
        guild: The guild of the server in question. Optional.
               If not provided, use pref_name otherwise directly mention.

    Returns:
        A list of messages to send to the snipe channels.
    """
    msgs, grouped = [], {}
    for hold in get_all_snipe_holds(session):
        duser = hold.user.discord_user
        try:
            grouped[duser.mention] += [f'{hold.held} in {hold.system.name}']
        except KeyError:
            grouped[duser.mention] = [f'{hold.held} in {hold.system.name}']

    msg = ""
    for mention, merits in grouped.items():
        systems = ', '.join(merits)
        msg += f"{mention} is holding {systems}\n"
        if len(msg) > cog.util.MSG_LIMIT:
            msgs += [msg]
            msg = ''

    if msg:
        msgs += [msg]

    return msgs


def get_consolidation_in_range(session, start, end=None):
    """
    Return all consolidation tracking data since the last tick.

    If start and end left bbank, return this cycle's votes.
    Dates are inclusive on both sides.

    Args:
        session: Session to the db.
        start: Fetch only those entries after this datetime, this date required.
        end: If present, fetch only those entries before this datetime.
    """
    query = session.query(Consolidation).\
        filter(Consolidation.updated_at >= start)

    if end:
        query = query.filter(Consolidation.updated_at <= end)

    return query.order_by(Consolidation.updated_at.asc()).all()


def fort_response_normal(session, eddb_session, *, next_systems=3):
    """Create the normal fort message with current targets.

    The message will be created according to following rules:
        - Always show active preps first, with current fort target in Active Targets.
        - Always show the next_systems amount of systems in Next Targets
        - When priority systems are set, display those in Priority Systems.
        - When deferred systems are present AND show_deferred True, display those in Almost Done.

    Args:
        session: A session onto the db.
        eddb_session: A session onto the EDDB db.
        next_count: The amount of systems to show not including preps and active target.

    Returns: A formatted message to send to channe.
    """
    preps = cogdb.query.fort_get_preps(session)
    forts = cogdb.query.fort_get_next_targets(session, count=next_systems + 1)

    lines = ['__Active Targets__']
    lines += [system.display() for system in preps + forts[:1]]
    forts = forts[1:]

    if forts:
        lines += ['\n__Next Targets__']
        lines += [system.display() for system in forts]

    globe = cogdb.query.get_current_global(session)
    priority, deferred = cogdb.query.fort_get_priority_targets(session)
    show_deferred = deferred and (
        globe.show_almost_done
        or cog.util.is_near_tick()
        or cogdb.TEST_DB
    )
    if priority:
        lines += ['\n__Priority Systems__'] + route_systems(eddb_session, priority)
    if show_deferred:
        lines += ['\n__Almost Done__'] + route_systems(eddb_session, deferred)

    return '\n'.join(lines)


def fort_response_manual(session):
    """Create the manual fort order message.

    The message will be created according to following rules:
        - Always show active preps first that have been manually set.
        - Afterwards show all manually set systems.

    Args:
        session: A session onto the db.

    Returns: A formatted message to send to channe.
    """
    manual_forts = cogdb.query.fort_order_get(session)
    preps = [x for x in manual_forts if x.is_prep]
    forts = [x for x in manual_forts if not x.is_prep]

    lines = ['__Active Targets (Manual Order)__']
    lines += [system.display() for system in preps + forts]

    return '\n'.join(lines)


def route_systems(eddb_session, systems):
    """
    Take a series of FortSystem objects from local database and return them
    sorted by best route given following criteria and formatted for display.
        - Start at systems closest HQ.
        - Route remaining systems by closest to last position.
        - Format them for display to user into a list.

    Args:
        eddb_sesion: A session onto the EDDB db.
        systems: A list of FortSystem objects from the live database.

    Returns:
        [System.display(), System.display(), ...]
    """
    if len(systems) < 2:
        return [system.display() for system in systems]

    mapped_originals = {x.name: x for x in systems}
    _, routed_systems = cogdb.eddb.find_route_closest_hq(eddb_session, [x.name for x in systems])
    return [mapped_originals[x.name].display() for x in routed_systems]


def add_sheet_record(session, **kwargs):
    """
    Add a permanent record of a change to the sheets and db. The main purpose is historical to
    be able to record important commands.

    Args:
        session: A session onto the db.
        discord_id: The discord id of the requesting user.
        channel_id: The channel id where message was sent.
        command: The text of the message user sent.
        sheet_src: The sheet being modified, one of: ['fort', 'um', 'snipe']

    Returns:
        The added SheetRecord
    """
    record = SheetRecord(**kwargs)
    session.add(record)

    return record


def get_user_sheet_records(session, *, discord_id, cycle=None):
    """
    Get sheet records for a particular cycle and user, a way to see what
    changes user has requested for bot.

    Args:
        session: A session onto the db.
        discord_id: The discord id of the requesting user.
        cycle: The cycle to pull information for. Default is current cycle.

    Returns:
        All SheetRecords matching, empty list if none found.
    """
    if not cycle:
        cycle = cog.util.current_cycle()

    return session.query(SheetRecord).\
        filter(SheetRecord.discord_id == discord_id,
               SheetRecord.cycle == cycle).\
        order_by(SheetRecord.created_at).\
        all()
