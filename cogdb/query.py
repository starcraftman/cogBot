"""
Module should handle logic related to querying/manipulating tables from a high level.
"""
import copy
import logging
import os
import tempfile

import sqlalchemy as sqla
import sqlalchemy.exc as sqla_exc
import sqlalchemy.orm.exc as sqla_oexc

import cog.exc
import cog.sheets
from cog.util import substr_match, get_config
from cogdb.schema import (DiscordUser, FortSystem, FortPrep, FortDrop, FortUser, FortOrder,
                          UMSystem, UMUser, UMHold, KOS, AdminPerm, ChannelPerm, RolePerm,
                          TrackSystem, TrackSystemCached, TrackByID, OCRTracker, OCRTrigger,
                          OCRPrep, Global)
from cogdb.eddb import HUDSON_CONTROLS, WINTERS_CONTROLS
from cogdb.scanners import FortScanner

DEFER_MISSING = get_config("limits", "defer_missing", default=750)
MAX_DROP = get_config("limits", "max_drop", default=1000)


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
        return session.query(DiscordUser).filter_by(id=discord_id).one()
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
    return session.query(DiscordUser, (sqla.func.ifnull(FortUser.dropped, 0) + sqla.func.ifnull(UMUser.combo, 0)).label('total')).\
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
    return session.query(DiscordUser, UMUser.combo).\
        join(UMUser, UMUser.name == DiscordUser.pref_name).\
        order_by(UMUser.combo.desc()).\
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


def next_sheet_row(session, *, cls, start_row):
    """
    Find the next available row to add in the sheet based on entries.
    """
    next_row = start_row
    rows = [x[0] for x in session.query(cls.row).order_by(cls.row).all()]
    if rows:
        complete_list = list(range(rows[0], rows[-1] + 2))
        next_row = sorted(list(set(complete_list) - set(rows)))[0]

    return next_row


def add_sheet_user(session, *, cls, discord_user, start_row):
    """
    Add a fort sheet user to system based on a Member.

    Kwargs:
        cls: The class of the sheet user like FortUser.
        duser: The DiscordUser object of the requesting user.
        start_row: Starting row if none inserted.
    """
    next_row = next_sheet_row(session, cls=cls, start_row=start_row)
    user = cls(name=discord_user.pref_name, cry=discord_user.pref_cry, row=next_row)
    session.add(user)
    session.commit()

    return user


def fort_get_medium_systems(session):
    """
    Return unfortified systems designated for small/medium ships.
    """
    mediums = session.query(FortSystem).\
        filter(FortSystem.is_medium, FortSystem.skip == 0).\
        all()
    unforted = [med for med in mediums if not med.is_fortified
                and not med.missing < DEFER_MISSING]
    return unforted


def fort_get_systems(session, mediums=True):
    """
    Return a list of all Systems. PrepSystems are not included.

    args:
        mediums: If false, exclude all systems designated for j
                 Determined by "S/M" being in notes.
    """
    query = session.query(FortSystem).filter(FortSystem.type != 'prep')
    if not mediums:
        med_names = [med.name for med in fort_get_medium_systems(session)]
        query = query.filter(FortSystem.name.notin_(med_names))

    return query.all()


def fort_get_preps(session):
    """
    Return a list of all PrepSystems.
    """
    return session.query(FortPrep).all()


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
        return session.query(FortSystem).filter_by(name=system_name).one()
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
    - Second System if present is a medium only system, if one remains unfortified.
    - All Systems after are prep targets.
    """
    targets = fort_order_get(session)
    if targets:
        return targets[:1]

    current = fort_find_current_index(session)
    systems = fort_get_systems(session)
    targets = [systems[current]]

    mediums = fort_get_medium_systems(session)
    if mediums and mediums[0].name != systems[current].name:
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
        InvalidCommandArgs: User requested an amount outside bounds [-MAX_DROP, MAX_DROP]
    """
    if amount not in range(-1 * MAX_DROP, MAX_DROP + 1):
        raise cog.exc.InvalidCommandArgs('Drop amount must be in range [-{num}, {num}]'.format(num=MAX_DROP))

    try:
        drop = session.query(FortDrop).filter_by(user_id=user.id, system_id=system.id).one()
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


def fort_order_get(session):
    """
    Get the order of systems to fort.

    If any systems have been completed, remove them from the list.

    Returns: [] if no systems set, else a list of System objects.
    """
    systems = []

    for fort_order in session.query(FortOrder).order_by(FortOrder.order):
        system = fort_order.system
        if system.is_fortified or system.missing < DEFER_MISSING:
            session.delete(fort_order)
        else:
            systems += [system]

    return systems


def fort_order_set(session, system_names):
    """
    Simply set the systems in the order desired.

    Ensure systems are actually valid before.
    """
    try:
        for ind, system_name in enumerate(system_names, start=1):
            if not isinstance(system_name, FortSystem):
                system_name = fort_find_system(session, system_name).name
            session.add(FortOrder(order=ind, system_name=system_name))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError) as exc:
        session.rollback()
        raise cog.exc.InvalidCommandArgs("Duplicate system specified, check your command!") from exc
    except cog.exc.NoMatch as exc:
        session.rollback()
        raise cog.exc.InvalidCommandArgs("FortSystem '{}' not found in fort systems.".format(system_name)) from exc


def fort_order_drop(session, systems):
    """
    Drop the given system_names from the override table.
    """
    for system_name in systems:
        try:
            if isinstance(system_name, FortSystem):
                system_name = system_name.name
            session.delete(session.query(FortOrder).filter_by(system_name=system_name).one())
        except sqla_oexc.NoResultFound:
            pass

    session.commit()


def um_find_system(session, system_name):
    """
    Find the UMSystem with system_name
    """
    try:
        return session.query(UMSystem).filter_by(name=system_name).one()
    except (sqla_oexc.NoResultFound, sqla_oexc.MultipleResultsFound) as exc:
        systems = session.query(UMSystem).\
            filter(UMSystem.name.ilike('%{}%'.format(system_name))).\
            all()

        if len(systems) > 1:
            raise cog.exc.MoreThanOneMatch(system_name, systems, UMSystem) from exc

        if len(systems) == 0:
            raise cog.exc.NoMatch(system_name, UMSystem) from exc

        return systems[0]


def um_get_systems(session, exclude_finished=True):
    """
    Return a list of all current undermining targets.

    kwargs:
        finished: Return just the finished targets.
    """
    systems = session.query(UMSystem).all()
    if exclude_finished:
        systems = [system for system in systems if not system.is_undermined]

    return systems


def um_reset_held(session, user):
    """
    Reset all held merits to 0.
    """
    holds = session.query(UMHold).filter_by(user_id=user.id).all()
    for hold in holds:
        hold.held = 0

    session.commit()
    return holds


def um_redeem_merits(session, user):
    """
    Redeem all held merits for user.
    """
    total = 0
    holds = session.query(UMHold).filter_by(user_id=user.id).all()
    for hold in holds:
        total += hold.held
        hold.redeemed = hold.redeemed + hold.held
        hold.held = 0

    session.commit()
    return (holds, total)


def um_redeem_systems(session, user, systems):
    """
    Redeem merits only for the specified user and the systems that matched exactly.
    """
    total = 0
    subq = session.query(UMSystem.id).\
        filter(UMSystem.name.in_(systems)).\
        scalar_subquery()
    holds = session.query(UMHold).filter(UMHold.user_id == user.id,
                                         UMHold.system_id.in_(subq)).all()
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
        hold = session.query(UMHold).filter_by(user_id=user.id,
                                               system_id=system.id).one()
    except sqla_oexc.NoResultFound:
        hold = UMHold(user_id=user.id, system_id=system.id, held=0, redeemed=0)
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
    for merit in session.query(UMHold).filter(UMHold.held > 0).order_by(UMHold.system_id).all():
        try:
            c_dict[merit.user.name][merit.system.name] = merit
        except KeyError:
            c_dict[merit.user.name] = {merit.system.name: merit}

    systems = session.query(UMSystem).order_by(UMSystem.id).all()
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
        return session.query(AdminPerm).filter_by(id=member.id).one()
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


def add_channel_perm(session, cmd, server, channel):
    try:
        session.add(ChannelPerm(cmd=cmd, server_id=server.id, channel_id=channel.id))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError) as exc:
        raise cog.exc.InvalidCommandArgs("Channel permission already exists.") from exc


def add_role_perm(session, cmd, server, role):
    try:
        session.add(RolePerm(cmd=cmd, server_id=server.id, role_id=role.id))
        session.commit()
    except (sqla_exc.IntegrityError, sqla_oexc.FlushError) as exc:
        raise cog.exc.InvalidCommandArgs("Role permission already exists.") from exc


def remove_channel_perm(session, cmd, server, channel):
    try:
        session.delete(session.query(ChannelPerm).
                       filter_by(cmd=cmd, server_id=server.id, channel_id=channel.id).one())
        session.commit()
    except sqla_oexc.NoResultFound as exc:
        raise cog.exc.InvalidCommandArgs("Channel permission does not exist.") from exc


def remove_role_perm(session, cmd, server, role):
    try:
        session.delete(session.query(RolePerm).
                       filter_by(cmd=cmd, server_id=server.id, role_id=role.id).one())
        session.commit()
    except sqla_oexc.NoResultFound as exc:
        raise cog.exc.InvalidCommandArgs("Role permission does not exist.") from exc


def check_perms(session, msg, args):
    """
    Check if a user is authorized to issue this command.
    Checks will be made against channel and user roles.

    Raises InvalidPerms if any permission issue.
    """
    check_channel_perms(session, args.cmd, msg.channel.guild, msg.channel)
    check_role_perms(session, args.cmd, msg.channel.guild, msg.author.roles)


def check_channel_perms(session, cmd, server, channel):
    """
    A user is allowed to issue a command if:
        a) no restrictions for the cmd
        b) the channel is whitelisted in the restricted channels

    Raises InvalidPerms if fails permission check.
    """
    channels = [perm.channel_id for perm in session.query(ChannelPerm).
                filter_by(cmd=cmd, server_id=server.id)]
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
    perm_roles = {perm.role_id for perm in session.query(RolePerm).
                  filter_by(cmd=cmd, server_id=server.id)}
    member_roles = {role.id for role in member_roles}
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


def kos_add_cmdr(session, cmdr, faction, reason, is_friendly=False):
    """
    Add a kos entry to the local database.

    args:
        cmdr: The cmdr name.
        faction: The faction in question.
        reason: The reason for addition if provided.
        is_friendly: If this user should be treated as friendly.
    """
    return session.add(KOS(cmdr=cmdr, faction=faction, reason=reason, is_friendly=is_friendly))


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


def track_systems_computed_update(session, systems):
    """
    Update the computed systems database area, merge in new systems.
    Ensure no dupes added.

    Returns: [system_name, system_name, ...]
    """
    track_systems = session.query(TrackSystemCached.system).\
        filter(TrackSystemCached.system.in_(systems)).\
        all()
    existing = [x[0] for x in track_systems]
    to_add = set(systems) - set(existing)
    added = [TrackSystemCached(system=x) for x in to_add]
    session.add_all(added)

    return added


def track_systems_computed_remove(session, to_keep):
    """
    Update the computed systems database area, remove all systems not in
    to_keep array.

    Returns: [system_name, system_name, ...]
    """
    track_systems = session.query(TrackSystemCached).\
        filter(TrackSystemCached.system.notin_(to_keep)).\
        all()

    removed = []
    for sys in track_systems:
        session.delete(sys)
        removed += [sys.system]

    return removed


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
        # Reject possible data that is older than current
        if date_obj and track.updated_at > date_obj:
            del copy_ids_dict[track.id]
            continue

        data = copy_ids_dict[track.id]
        if data.get("squad", ""):
            track.squad = data['squad']
        if data.get("override", None):
            track.override = data['override']
        track.system = data.get('system', None)
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
        session.add(OCRTracker(**data))
        added += [data['system']]

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
        session.add(OCRTrigger(**data))
        added += [data['system']]

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
        session.add(OCRPrep(**data))
        added += [data['system']]

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

    print("HI")
    for sys in session.query(FortSystem):
        print(sys.name)
        if not sys.ocr_tracker:
            continue
        changed = False

        print(sys.name, sys.fort_status, sys.ocr_tracker.fort, sys.um_status, sys.ocr_tracker.um)
        if not sys.is_fortified and sys.ocr_tracker.fort > sys.fort_status:
            sys.fort_status = sys.ocr_tracker.fort
            changed = True
        if not sys.is_undermined and sys.ocr_tracker.um > sys.um_status:
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
