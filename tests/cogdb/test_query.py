# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Test cogdb.query module.
"""
import datetime
import sqlalchemy.orm.exc
import mock
import pytest

import cog.exc
import cogdb
from cogdb.schema import (DiscordUser, FortSystem, FortUser, FortOrder,
                          UMUser, UMSystem, UMHold, EUMSheet, AdminPerm, ChannelPerm, RolePerm,
                          KOS, TrackSystem, TrackSystemCached, TrackByID,
                          Vote, EVoteType, SheetRecord)
import cogdb.query

from tests.conftest import Channel, Member, Message, Role, Guild


def test_get_duser(session, f_dusers):
    f_duser = f_dusers[0]
    duserQ = cogdb.query.get_duser(session, f_duser.id)
    assert isinstance(duserQ, DiscordUser)
    assert duserQ.display_name == f_duser.display_name

    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.get_duser(session, 0)


def test_ensure_duser_no_create(session, f_dusers):
    expect = f_dusers[0]
    member = mock.Mock()
    member.id = 1
    member.display_name = 'default'
    duser = cogdb.query.ensure_duser(session, member)
    assert duser == expect


def test_ensure_duser_create(session, f_dusers):
    member = mock.Mock()
    member.id = 2000
    member.display_name = 'NewUser'
    duser = cogdb.query.ensure_duser(session, member)
    assert duser.id == member.id
    assert duser.display_name == member.display_name

    last_user = session.query(DiscordUser).order_by(DiscordUser.id.desc()).limit(1).one()
    assert last_user == duser


def test_add_duser(session, f_dusers):
    member = mock.Mock()
    member.id = 2000
    member.display_name = 'NewUser'

    cogdb.query.add_duser(session, member)
    assert session.query(DiscordUser).order_by(DiscordUser.id.desc()).limit(1).one().id == member.id

    member.id = 2001
    member.display_name += '2'
    cogdb.query.add_duser(session, member)
    assert session.query(DiscordUser).order_by(DiscordUser.id.desc()).limit(1).one().id == member.id


def test_check_pref_name(session, f_dusers, f_fort_testbed):
    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.check_pref_name(session, f_dusers[0].pref_name)

    # No raise
    cogdb.query.check_pref_name(session, "ANewName")


def test_next_sheet_row_fort(session, f_dusers, f_fort_testbed):
    row = cogdb.query.next_sheet_row(session, cls=FortUser, start_row=11)
    assert row == 18

    last = session.query(FortUser).order_by(FortUser.row.desc()).limit(1).one()
    last.row = 22
    session.commit()
    row = cogdb.query.next_sheet_row(session, cls=FortUser, start_row=11)
    assert row == 17

    # Use opposite class to ensure empty testbed
    row = cogdb.query.next_sheet_row(session, cls=UMUser, start_row=11)
    assert row == 11


def test_next_sheet_row_um(session, f_dusers, f_um_testbed):
    row = cogdb.query.next_sheet_row(session, cls=UMUser, start_row=11)
    assert row == 20

    last = session.query(UMUser).order_by(UMUser.row.desc()).limit(1).one()
    last.row = 22
    session.commit()
    row = cogdb.query.next_sheet_row(session, cls=UMUser, start_row=11)
    assert row == 19

    # Use opposite class to ensure empty testbed
    row = cogdb.query.next_sheet_row(session, cls=FortUser, start_row=14)
    assert row == 14


def test_add_sheet_user_fort(session, f_dusers, db_cleanup):
    duser = mock.Mock(id=999999999999999999999999, pref_name='Fort User1', pref_cry='No cry')

    cogdb.query.add_sheet_user(session, cls=FortUser, discord_user=duser, start_row=5)

    latest = session.query(FortUser).all()[-1]
    assert latest.name == duser.pref_name
    assert latest.row == 5


def test_add_sheet_user_um(session, f_dusers, db_cleanup):
    duser = mock.Mock(id=999999999999999999999999, pref_name='UM User1', pref_cry='No cry')

    cogdb.query.add_sheet_user(session, cls=UMUser, discord_user=duser, start_row=5, sheet_src=EUMSheet.main)

    latest = session.query(UMUser).all()[-1]
    assert latest.name == duser.pref_name
    assert latest.row == 5
    assert latest.sheet_src == EUMSheet.main


def test_add_sheet_snipe_um(session, f_dusers, db_cleanup):
    duser = mock.Mock(id=999999999999999999999999, pref_name='Snipe User1', pref_cry='No cry')

    cogdb.query.add_sheet_user(session, cls=UMUser, discord_user=duser, start_row=5, sheet_src=EUMSheet.snipe)

    latest = session.query(UMUser).all()[-1]
    assert latest.name == duser.pref_name
    assert latest.row == 5
    assert latest.sheet_src == EUMSheet.snipe


def test_fort_get_medium_systems(session, f_dusers, f_fort_testbed):
    mediums = cogdb.query.fort_get_medium_systems(session)
    assert mediums
    assert mediums[0].name == "Othime"


def test_fort_get_systems(session, f_dusers, f_fort_testbed):
    systems = cogdb.query.fort_get_systems(session)
    assert len(systems) == 8
    assert systems[0].name == 'Frey'
    assert systems[-1].name == 'LPM 229'


def test_fort_get_preps(session, f_dusers, f_fort_testbed):
    systems = cogdb.query.fort_get_preps(session)
    assert [system.name for system in systems] == ['Rhea']


def test_fort_get_systems_no_mediums(session, f_dusers, f_fort_testbed):
    systems = cogdb.query.fort_get_systems(session, mediums=False)
    assert len(systems) == 7
    assert systems[0].name == 'Frey'
    assert systems[-1].name == 'LPM 229'


def test_fort_get_systems_by_state(session, f_dusers, f_fort_testbed):
    systems = cogdb.query.fort_get_systems(session)
    systems[1].fort_status = 8425
    systems[1].undermine = 1.0
    systems[4].undermine = 1.9
    session.commit()

    systems = cogdb.query.fort_get_systems_by_state(session)
    assert [sys.name for sys in systems['cancelled']] == ["Nurundere"]
    assert [sys.name for sys in systems['fortified']] == ["Frey"]
    assert [sys.name for sys in systems['undermined']] == [
        "Alpha Fornacis", 'WW Piscis Austrini', 'LPM 229']
    assert [sys.name for sys in systems['skipped']] == ['Sol', 'Phra Mool']
    assert [sys.name for sys in systems['almost_done']] == ['Dongkum']
    assert [sys.name for sys in systems['left'][0:3]] == [
        "LHS 3749", "Othime"]


def test_fort_find_current_index(session, f_dusers, f_fort_testbed):
    assert cogdb.query.fort_find_current_index(session) == 1


def test_fort_find_system(session, f_dusers, f_fort_testbed):
    sys = cogdb.query.fort_find_system(session, 'Sol')
    assert isinstance(sys, cogdb.schema.FortSystem)
    assert sys.name == 'Sol'

    sys = cogdb.query.fort_find_system(session, 'alp')
    assert sys.name == 'Alpha Fornacis'


def test_fort_get_next_targets(session, f_dusers, f_fort_testbed):
    targets = cogdb.query.fort_get_next_targets(session, offset=1, count=1)
    assert [sys.name for sys in targets] == ["LHS 3749"]

    targets = cogdb.query.fort_get_next_targets(session, offset=1, count=2)
    assert [sys.name for sys in targets] == ["LHS 3749", "Alpha Fornacis"]


def test_fort_get_systems_x_left(session, f_dusers, f_fort_testbed):
    systems = cogdb.query.fort_get_systems_x_left(session, 5000)
    assert systems[0].name == 'Nurundere'
    assert len(systems) == 3

    systems = cogdb.query.fort_get_systems_x_left(session, 5000, include_preps=True)
    assert len(systems) == 4


def test_fort_get_priority_targets(session, f_dusers, f_fort_testbed):
    session.add(
        FortSystem(id=11, name='PriorityForted', fort_status=9000, trigger=8563, notes='Priority For S/M Ships', sheet_col='BZ', sheet_order=58)
    )
    session.commit()
    priority, deferred = cogdb.query.fort_get_priority_targets(session)
    assert priority[0].name == 'Othime'
    assert len(priority) == 1
    assert deferred[0].name == 'Dongkum'
    assert len(deferred) == 1


def test_fort_add_drop(session, f_dusers, f_fort_testbed, db_cleanup):
    system = session.query(FortSystem).filter(FortSystem.name == 'Sol').one()
    user = f_fort_testbed[0][-1]

    with pytest.raises(sqlalchemy.orm.exc.NoResultFound):
        session.query(cogdb.schema.FortDrop).\
            filter_by(user_id=user.id, system_id=system.id).\
            one()

    old_fort = system.fort_status
    drop = cogdb.query.fort_add_drop(session, system=system, user=user, amount=400)
    session.commit()

    assert drop.amount == 400
    assert system.fort_status == old_fort + 400
    assert session.query(cogdb.schema.FortDrop).filter_by(user_id=user.id, system_id=system.id).one()


def test_fort_add_drop_max(session, f_dusers, f_fort_testbed, db_cleanup):
    system = session.query(FortSystem).filter(FortSystem.name == 'Sol').one()
    user = f_fort_testbed[0][-1]
    to_drop = cog.util.CONF.constants.max_drop + 1

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.fort_add_drop(session, system=system, user=user, amount=to_drop)


def test_fort_order_get(session, f_dusers, f_fort_testbed, f_fortorders):
    names = []
    for system in cogdb.query.fort_order_get(session):
        assert isinstance(system, FortSystem)
        names += [system.name]
    assert names == [order.system_name for order in f_fortorders]


def test_fort_order_set(session, f_dusers, f_fort_testbed, f_fortorders):
    cogdb.query.fort_order_drop(session)
    assert cogdb.query.fort_order_get(session) == []

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.fort_order_set(session, ['Cubeo'])

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.fort_order_set(session, ['Sol', 'Sol', 'Sol'])

    expect = ['Sol', 'LPM 229']
    cogdb.query.fort_order_set(session, expect)
    assert [sys.name for sys in cogdb.query.fort_order_get(session)] == expect


def test_fort_order_drop(session, f_dusers, f_fort_testbed, f_fortorders):
    cogdb.query.fort_order_drop(session)

    assert session.query(FortOrder).all() == []


def test_fort_order_remove_finished(session, f_dusers, f_fort_testbed, f_fortorders):
    sol = session.query(FortSystem).filter(FortSystem.name == "Sol").one()
    sol.fort_status = 20000

    cogdb.query.fort_order_remove_finished(session)
    assert [x[0] for x in session.query(FortOrder.system_name).order_by(FortOrder.order)] == ["LPM 229", "Othime"]


def test_um_find_system(session, f_dusers, f_um_testbed):
    system = cogdb.query.um_find_system(session, 'Cemplangpa')
    assert system.name == 'Cemplangpa'

    system = cogdb.query.um_find_system(session, 'Cemp')
    assert system.name == 'Cemplangpa'

    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.um_find_system(session, 'NotThere')

    with pytest.raises(cog.exc.MoreThanOneMatch):
        cogdb.query.um_find_system(session, 'r')


def test_um_get_systems(session, f_dusers, f_um_testbed):
    systems = [system.name for system in
               cogdb.query.um_get_systems(session, exclude_finished=False, ignore_leave=False)]
    assert 'LeaveIt' in systems
    assert 'Cemplangpa' in systems
    assert 'Burr' in systems
    assert 'AF Leopris' in systems

    systems = [system.name for system in
               cogdb.query.um_get_systems(session, exclude_finished=True, ignore_leave=False)]
    assert 'LeaveIt' in systems
    assert 'Cemplangpa' not in systems
    assert 'Burr' in systems
    assert 'AF Leopris' in systems

    systems = [system.name for system in
               cogdb.query.um_get_systems(session, exclude_finished=True, ignore_leave=True)]
    assert 'LeaveIt' not in systems
    assert 'Cemplangpa' not in systems
    assert 'Burr' in systems
    assert 'AF Leopris' in systems


def test_um_reset_held(session, f_dusers, f_um_testbed):
    user = f_um_testbed[0][0]
    assert user.merit_summary() == 'Holding 2600, Redeemed 11350'
    cogdb.query.um_reset_held(session, user)
    assert user.merit_summary() == 'Holding 0, Redeemed 11350'


def test_um_redeem_merits(session, f_dusers, f_um_testbed):
    user = f_um_testbed[0][0]
    assert user.merit_summary() == 'Holding 2600, Redeemed 11350'
    cogdb.query.um_redeem_merits(session, user)
    assert user.merit_summary() == 'Holding 0, Redeemed 13950'


def test_um_add_system_targets(session, f_dusers, f_um_testbed):
    um_systems = [{
        "sys_name": "Ross 860",
        "power": "Edmund Mahon",
        "security": "Low",
        "trigger": 15000,
        "priority": "Normal",
    }]

    cogdb.query.um_add_system_targets(session, um_systems)
    session.commit()
    result = session.query(UMSystem).\
        filter(UMSystem.name == um_systems[0]['sys_name']).\
        one()

    assert result
    assert result.close_control == "Vega"


def test_um_add_hold(session, f_dusers, f_um_testbed):
    user = f_um_testbed[0][1]
    system = f_um_testbed[1][0]
    session.query(UMHold).delete()
    assert not session.query(UMHold).all()

    cogdb.query.um_add_hold(session, held=600, system=system, user=user)
    hold = session.query(UMHold).filter_by(system_id=system.id, user_id=user.id).one()
    assert hold.held == 600

    hold = cogdb.query.um_add_hold(session, held=2000, system=system, user=user)
    hold = session.query(UMHold).filter_by(system_id=system.id, user_id=user.id).one()
    assert hold.held == 2000


def test_um_all_held_merits(session, f_dusers, f_um_testbed):
    expect = [
        ['CMDR', 'Cemplangpa', 'Pequen', 'Burr', 'AF Leopris', 'Empty'],
        ['User2', 450, 2400, 0, 0, 0],
        ['User1', 0, 400, 2200, 0, 0]
    ]
    assert cogdb.query.um_all_held_merits(session) == expect


def test_admin_get(session, f_dusers, f_admins):
    member = Member(f_dusers[0].display_name, None, id=f_dusers[0].id)
    assert f_admins[0] == cogdb.query.get_admin(session, member)

    member = Member("NotThere", None, id="2000")
    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.get_admin(session, member)


def test_add_admin(session, f_dusers, f_admins):
    cogdb.query.add_admin(session, f_dusers[-1])

    assert session.query(AdminPerm).all()[-1].id == f_dusers[-1].id


def test_show_guild_perms(session, f_cperms, f_rperms):
    expected = """__Existing Rules For Test__

__Channel Rules__
`!drop` limited to channel: AChannel


__Role Rules__
`!drop` limited to role: ARole"""
    server = Guild('Test', f_cperms[0].guild_id)
    server.channels = [Channel('AChannel', id=2001)]
    server.roles = [Role("ARole", id=3001)]

    msg = cogdb.query.show_guild_perms(session, server, prefix='!')
    assert msg == expected


def test_add_channel_perms(session, f_cperms):
    server = Guild('Test', id=10)
    channel = Channel('AChannel', id=3001)
    cogdb.query.add_channel_perms(session, ['Status'], server, [channel])

    obj = session.query(ChannelPerm).filter_by(cmd='Status', channel_id=channel.id).one()
    assert obj.cmd == 'Status'

    with pytest.raises(cog.exc.InvalidCommandArgs):
        try:
            channel.id = 2001
            cogdb.query.add_channel_perms(session, ['drop'], server, [channel])
        finally:
            session.rollback()


def test_add_role_perms(session, f_rperms):
    server = Guild('Test', id=10)
    role = Role("ARole", id=2201)
    cogdb.query.add_role_perms(session, ['Status'], server, [role])

    obj = session.query(RolePerm).filter_by(cmd='Status', role_id=role.id).one()
    assert obj.cmd == 'Status'

    with pytest.raises(cog.exc.InvalidCommandArgs):
        try:
            role.id = 3001
            cogdb.query.add_role_perms(session, ['drop'], server, [role])
        finally:
            session.rollback()


def test_remove_channel_perms(session, f_cperms):
    perm = f_cperms[0]
    server = Guild('Test', id=perm.guild_id)
    channel = Channel('AChannel', id=perm.channel_id)
    cogdb.query.remove_channel_perms(session, [perm.cmd], server, [channel])

    assert not session.query(ChannelPerm).all()
    cogdb.query.remove_channel_perms(session, [perm.cmd], server, [channel])  # Sanity check, no raise


def test_remove_role_perms(session, f_rperms):
    perm = f_rperms[0]
    server = Guild('Test', id=perm.guild_id)
    role = Role('ARole', id=perm.role_id)
    cogdb.query.remove_role_perms(session, [perm.cmd], server, [role])

    assert not session.query(RolePerm).all()
    cogdb.query.remove_role_perms(session, [perm.cmd], server, [role])


def test_check_perms(f_cperms, f_rperms):
    cog.parse.make_parser('!')  # Need to force CMD_MAP to populate.
    ops_channel = Channel("Operations", id=2001)
    server = Guild('Test', id=10)
    server.channels = [ops_channel]
    ops_channel.server = server
    roles = [Role('FRC Member', id=3001), Role('Winters', id=3002)]
    author = Member('User1', roles)
    msg = Message('!drop', author, server, ops_channel)
    msg.channel = ops_channel
    msg.channel.guild = server

    cogdb.query.check_perms(msg, 'drop')  # Silent pass

    with pytest.raises(cog.exc.InvalidPerms):
        msg.author.roles = [Role('Winters', id=3002)]
        cogdb.query.check_perms(msg, 'drop')

    with pytest.raises(cog.exc.InvalidPerms):
        msg.author.roles = roles
        msg.channel.name = 'Not Operations'
        msg.channel.id = 9999
        cogdb.query.check_perms(msg, 'drop')


def test_check_channel_perms(session, f_cperms):
    # Silently pass if no raise
    cogdb.query.check_channel_perms(session, 'drop', Guild('Test', id=10), Channel('Operations', id=2001))
    cogdb.query.check_channel_perms(session, 'time', Guild('NoPerm', id=2333), Channel('Operations', id=2001))

    with pytest.raises(cog.exc.InvalidPerms):
        cogdb.query.check_channel_perms(session, 'drop', Guild('Test', id=10), Channel('Not Operations', id=2002))


def test_check_role_perms(session, f_rperms):
    # Silently pass if no raise
    cogdb.query.check_role_perms(session, 'drop', Guild('Test', id=10),
                                 [Role('FRC Member', id=3001), Role('FRC Vet', id=4000)])
    cogdb.query.check_role_perms(session, 'Time', Guild('NoPerm', id=2333),
                                 [Role('Winters', None)])

    with pytest.raises(cog.exc.InvalidPerms):
        cogdb.query.check_role_perms(session, 'drop', Guild('Test', id=10),
                                     [Role('FRC Robot', id=2222), Role('Cookies', id=221)])


def test_complete_control_name():
    assert cogdb.query.complete_control_name("lush") == "Lushertha"

    assert cogdb.query.complete_control_name("pupp", True) == "18 Puppis"

    with pytest.raises(cog.exc.NoMatch):
        assert cogdb.query.complete_control_name("not_there")

    with pytest.raises(cog.exc.MoreThanOneMatch):
        assert cogdb.query.complete_control_name("lhs")


def test_kos_kill_list(session, f_kos):
    assert ['bad_guy', 'BadGuy'] == cogdb.query.kos_kill_list(session)


def test_kos_search_cmdr(session, f_kos):
    results = cogdb.query.kos_search_cmdr(session, 'good_guy')
    assert len(results) == 2
    assert sorted([x.cmdr for x in results]) == sorted(['good_guy', 'good_guy_pvp'])


def test_kos_add_cmdr(session, f_kos):
    cogdb.query.kos_add_cmdr(session, {
        'cmdr': 'cmdr',
        'squad': 'squad',
        'reason': 'A reason',
        'is_friendly': False
    })
    session.commit()

    results = session.query(KOS).all()
    assert results[-1].cmdr == 'cmdr'
    assert results[-1].reason == 'A reason'


def test_track_add_systems(session, f_track_testbed):
    system_name = "Kappa"
    cogdb.query.track_add_systems(session, [system_name], distance=20)
    session.commit()

    result = session.query(TrackSystem).filter(TrackSystem.system == system_name).one()
    assert result.system == system_name
    assert result.distance == 20


def test_track_add_systems_exists(session, f_track_testbed):
    system_name = "Tollan"
    added = cogdb.query.track_add_systems(session, [system_name], distance=20)
    assert added == []


def test_track_remove_systems(session, f_track_testbed):
    system_names = ["Tollan", "Rana"]
    captured = cogdb.query.track_remove_systems(session, system_names)
    session.commit()

    found = session.query(TrackSystem).all()
    assert captured == ["Tollan"]
    assert len(found) == 1
    assert found[0].system == "Nanomam"


def test_track_get_all_systems(session, f_track_testbed):
    captured = cogdb.query.track_get_all_systems(session)

    assert list(sorted([x.system for x in captured])) == ["Nanomam", "Tollan"]


def test_track_show_systems(session, f_track_testbed):
    expected = """__Tracking System Rules__

    Tracking systems <= 15ly from Nanomam
    Tracking systems <= 12ly from Tollan"""

    captured = cogdb.query.track_show_systems(session)
    assert captured == [expected]


def test_track_systems_computed_add_new(session, f_track_testbed):
    system_names = ["System1", "System2", "System3"]

    added, _ = cogdb.query.track_systems_computed_add(session, system_names, "Rhea")
    assert sorted(added) == system_names
    system = session.query(TrackSystemCached).filter(TrackSystemCached.system == "System1").one()
    assert system.overlaps_with == "Rhea"


def test_track_systems_computed_remove(session, f_track_testbed):
    deleted, modified = cogdb.query.track_systems_computed_remove(session, "Tollan")
    session.commit()

    assert deleted == ["Tollan"]
    assert modified == [
        'Bodedi',
        'DX 799',
        'LHS 1885',
        'LHS 215',
        'LHS 221',
        'LHS 246',
        'LHS 262',
        'LHS 283',
        'LHS 6128',
        'Nanomam'
    ]
    assert session.query(TrackSystemCached).filter(TrackSystemCached.system == "Nanomam").one().overlaps_with == "Nanomam"


def test_track_systems_computed_check(session, f_track_testbed):
    assert not cogdb.query.track_systems_computed_check(session, "Kappa")
    assert cogdb.query.track_systems_computed_check(session, "Nanomam")


def test_track_ids_update(session, f_track_testbed):
    id_dict = {
        "J3J-WVT": {"id": "J3J-WVT", "squad": "default", "system": "Rhea", "override": True},
        "ZZZ-111": {"id": "ZZZ-111", "squad": "new", "system": "News", "override": True},
    }

    cogdb.query.track_ids_update(session, id_dict)
    session.commit()

    updated = session.query(TrackByID).filter(TrackByID.id == "J3J-WVT").one()
    added = session.query(TrackByID).filter(TrackByID.id == "ZZZ-111").one()

    assert updated.squad == "default"
    assert updated.system == "Rhea"
    assert added.squad == "new"
    assert added.system == "News"


def test_track_ids_update_ignore(session, f_track_testbed):
    id_dict = {
        "J3J-WVT": {"id": "J3J-WVT", "squad": "IGNORE", "system": "Rana", "override": True},
    }

    cogdb.query.track_ids_update(session, id_dict)
    session.commit()

    ignored = session.query(TrackByID).filter(TrackByID.id == "J3J-WVT").one()

    assert ignored.system == "Rana"
    assert ignored.squad == "CLBF"


def test_track_ids_update_timestamp(session, f_track_testbed):
    id_dict = {
        "J3J-WVT": {"id": "J3J-WVT", "squad": "default", "system": "Rhea", "override": True},
        "ZZZ-111": {"id": "ZZZ-111", "squad": "new", "system": "News", "override": True},
    }
    years_ago = datetime.datetime.utcnow().replace(year=1000)

    cogdb.query.track_ids_update(session, id_dict, years_ago)
    session.commit()

    updated = session.query(TrackByID).filter(TrackByID.id == "J3J-WVT").one()
    added = session.query(TrackByID).filter(TrackByID.id == "ZZZ-111").one()

    assert updated.squad != "default"
    assert updated.system != "Rhea"
    assert added.squad == "new"
    assert added.system == "News"


def test_track_ids_remove(session, f_track_testbed):
    ids = ["J3J-WVT", "ZZZ-111"]
    cogdb.query.track_ids_remove(session, ids)
    session.commit()

    with pytest.raises(sqlalchemy.exc.NoResultFound):
        session.query(TrackByID).filter(TrackByID.id == "J3J-WVT").one()
    with pytest.raises(sqlalchemy.exc.NoResultFound):
        session.query(TrackByID).filter(TrackByID.id == "ZZZ-111").one()


def test_track_ids_show(session, f_track_testbed):
    expected = """__Tracking IDs__

    J3J-WVT [CLBF] jumped Nanomam => Rana
    J3N-53B [CLBF] jumped No Info => No Info
    XNL-3XQ [CLBF] jumped No Info => Tollan, near Tollan
    OVE-111 [Manual] jumped No Info => No Info"""

    with cogdb.query.track_ids_show(session) as fname:
        with open(fname) as fin:
            assert fin.read() == expected


def test_track_ids_check(session, f_track_testbed):
    assert cogdb.query.track_ids_check(session, 'OVE-111')
    assert not cogdb.query.track_ids_check(session, 'J3J-WVT')


def test_track_ids_newer_than(session, f_track_testbed):
    date = datetime.datetime(year=2000, month=1, day=10, hour=0, minute=0, second=0, microsecond=0)
    objs = cogdb.query.track_ids_newer_than(session, date)
    assert sorted([x.id for x in objs]) == ["J3N-53B", "OVE-111"]


def test_users_with_all_merits(session, f_dusers, f_fort_testbed, f_um_testbed):
    cap = cogdb.query.users_with_all_merits(session)
    assert [x[0].display_name for x in cap] == ["User1", "User2", "User3"]
    assert [x[1] for x in cap] == [15050, 8050, 1800]


def test_users_with_fort_merits(session, f_dusers, f_fort_testbed, f_um_testbed):
    cap = cogdb.query.users_with_fort_merits(session)
    assert [x[0].display_name for x in cap] == ["User2", "User3", "User1"]
    assert [x[1] for x in cap] == [2000, 1800, 1100]


def test_users_with_um_merits(session, f_dusers, f_fort_testbed, f_um_testbed):
    cap = cogdb.query.users_with_um_merits(session)
    assert [x[0].display_name for x in cap] == ["User1", "User2"]
    assert [x[1] for x in cap] == [13950, 6050]


def test_post_cycle_db_cleanup(session, f_vote_testbed, eddb_session):
    cogdb.query.post_cycle_db_cleanup(session, eddb_session)

    assert session.query(Vote).all() == []


def test_vote_add(session, f_dusers, f_vote_testbed, f_global_testbed):
    last_duser = f_dusers[-1]
    returned_message = cogdb.query.add_vote(session, last_duser.id, 'prep', 1)
    expected_message = f"**{last_duser.display_name}**: voted 1 Prep."
    assert str(returned_message) == expected_message

    the_vote = cogdb.query.add_vote(session, last_duser.id, EVoteType.cons, 5)
    expect = f"**{last_duser.display_name}**: voted 5 Cons."
    assert str(the_vote) == expect


def test_vote_get(session, f_dusers, f_vote_testbed):
    # Existing
    the_vote = cogdb.query.get_vote(session, f_dusers[1].id, EVoteType.cons)
    assert the_vote.id == f_dusers[1].id
    assert the_vote.vote == EVoteType.cons

    # Doesn't exist
    the_vote = cogdb.query.get_vote(session, f_dusers[2].id, EVoteType.prep)
    assert the_vote.id == f_dusers[2].id
    assert the_vote.vote == EVoteType.prep


def test_get_all_votes(session, f_dusers, f_vote_testbed):
    votes = cogdb.query.get_all_votes(session)

    assert votes[0][0] == f_vote_testbed[-1]  # Returned descending order


def test_get_cons_prep_totals_empty(session, f_dusers):
    cons, preps = cogdb.query.get_cons_prep_totals(session)

    assert cons == 0
    assert preps == 0


def test_get_cons_prep_totals_only_cons(session, f_dusers, f_vote_testbed):
    session.query(Vote).filter(Vote.vote == EVoteType.prep).delete()
    session.commit()
    cons, preps = cogdb.query.get_cons_prep_totals(session)

    assert preps == 0
    assert cons == 4


def test_get_cons_prep_totals(session, f_dusers, f_vote_testbed):
    cons, preps = cogdb.query.get_cons_prep_totals(session)

    assert cons == 4
    assert preps == 7


def test_get_all_snipe_holds(session, f_bot, f_dusers, f_um_testbed):
    expected = [UMHold(id=7, sheet_src=EUMSheet.snipe, system_id=10007, user_id=3, held=5000, redeemed=1200)]
    assert cogdb.query.get_all_snipe_holds(session) == expected


def test_get_snipe_members_holding(session, f_bot, f_dusers, f_um_testbed):
    expected = ['<@3> is holding 5000 in ToSnipe\n']
    assert cogdb.query.get_snipe_members_holding(session) == expected


def test_get_consolidation_in_range_default(session, f_cons_data):
    after = f_cons_data[0].updated_at - datetime.timedelta(minutes=5)
    values = cogdb.query.get_consolidation_in_range(session, start=after)
    assert [x.amount for x in values] == [66, 67, 65, 64, 67, 68]


def test_get_consolidation_in_range_both(session, f_cons_data):
    after = f_cons_data[1]
    before = f_cons_data[-3]
    values = cogdb.query.get_consolidation_in_range(session, start=after.updated_at, end=before.updated_at)
    assert [x.amount for x in values] == [67, 65, 64]


def test_fort_response_normal(session, f_dusers, f_fort_testbed, eddb_session):
    expect = """__Active Targets__
Prep: **Rhea** 5100/10000 :Fortifying: Atropos - 65.55Ly
**Nurundere** 5422/8425 :Fortifying: - 99.51Ly

__Next Targets__
**LHS 3749** 1850/5974 :Fortifying: - 55.72Ly
**Alpha Fornacis**    0/6476 :Fortifying: - 67.27Ly
**WW Piscis Austrini**    0/8563 :Fortifying:, :Undermined: - 101.38Ly

__Priority Systems__
**Othime**    0/7367 :Fortifying: Priority for S/M ships (no L pads) - 83.68Ly

__Almost Done__
**Dongkum** 7000/7239 :Fortifying: (239 left) - 81.54Ly"""
    assert expect == cogdb.query.fort_response_normal(session, eddb_session)


def test_fort_response_manual(session, f_dusers, f_fort_testbed, f_fortorders):
    expect = """__Active Targets (Manual Order)__
**Sol** 2500/5211 :Fortifying:, 2250 :Undermining: Leave For Grinders - 28.94Ly
**LPM 229**    0/9479 :Fortifying:, :Undermined: - 112.98Ly
**Othime**    0/7367 :Fortifying: Priority for S/M ships (no L pads) - 83.68Ly"""
    assert expect == cogdb.query.fort_response_manual(session)


def test_route_systems(session, f_dusers, f_fort_testbed, eddb_session):
    systems = f_fort_testbed[1]

    expected = [
        '**Sol** 2500/5211 :Fortifying:, 2250 :Undermining: Leave For Grinders - 28.94Ly',
        '**Alpha Fornacis**    0/6476 :Fortifying: - 67.27Ly',
        '**Dongkum** 7000/7239 :Fortifying: (239 left) - 81.54Ly',
        '**Nurundere** 5422/8425 :Fortifying: - 99.51Ly',
        '**LHS 3749** 1850/5974 :Fortifying: - 55.72Ly',
        '**Frey** 4910/4910 :Fortified: - 116.99Ly'
    ]
    assert cogdb.query.route_systems(eddb_session, systems[:6]) == expected


def test_route_systems_less_two(session, f_dusers, f_fort_testbed, eddb_session):
    systems = f_fort_testbed[1]

    expected = ['**Frey** 4910/4910 :Fortified: - 116.99Ly']
    assert cogdb.query.route_systems(eddb_session, systems[:1]) == expected


def test_add_sheet_record(session, f_sheet_records):
    cogdb.query.add_sheet_record(session, discord_id=1, channel_id=10,
                                 command='!drop 500 rati', sheet_src='fort')
    session.commit()
    last_record = session.query(SheetRecord).all()[-1]
    assert last_record.command == '!drop 500 rati'
    assert last_record.discord_id == 1


def test_get_user_sheet_records(session, f_sheet_records):
    records = cogdb.query.get_user_sheet_records(session, discord_id=1, cycle=cog.util.current_cycle())

    assert "!drop 500 Rana" == records[-1].command
