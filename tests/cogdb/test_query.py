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
                          UMUser, UMHold, AdminPerm, ChannelPerm, RolePerm,
                          KOS, TrackSystem, TrackSystemCached, TrackByID,
                          OCRTracker, OCRTrigger, OCRPrep)
import cogdb.query

from tests.data import SYSTEMS, USERS
from tests.conftest import Channel, Member, Message, Role, Guild


def test_fuzzy_find():
    assert cogdb.query.fuzzy_find('Alex', USERS) == 'Alexander Astropath'

    with pytest.raises(cog.exc.MoreThanOneMatch):
        cogdb.query.fuzzy_find('ric', USERS)
    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.fuzzy_find('zzzz', SYSTEMS)

    assert cogdb.query.fuzzy_find('WW p', SYSTEMS) == 'WW Piscis Austrini'
    with pytest.raises(cog.exc.MoreThanOneMatch):
        cogdb.query.fuzzy_find('LHS', SYSTEMS)
    assert cogdb.query.fuzzy_find('tun', SYSTEMS) == 'Tun'


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

    cogdb.query.add_sheet_user(session, cls=UMUser, discord_user=duser, start_row=5)

    latest = session.query(UMUser).all()[-1]
    assert latest.name == duser.pref_name
    assert latest.row == 5


def test_fort_get_medium_systems(session, f_dusers, f_fort_testbed):
    mediums = cogdb.query.fort_get_medium_systems(session)
    assert mediums
    assert mediums[0].name == "Othime"


def test_fort_get_systems(session, f_dusers, f_fort_testbed):
    systems = cogdb.query.fort_get_systems(session)
    assert len(systems) == 10
    assert systems[0].name == 'Frey'
    assert systems[-1].name == 'LPM 229'


def test_fort_get_preps(session, f_dusers, f_fort_testbed):
    systems = cogdb.query.fort_get_preps(session)
    assert [system.name for system in systems] == ['Rhea']


def test_fort_get_systems_no_mediums(session, f_dusers, f_fort_testbed):
    systems = cogdb.query.fort_get_systems(session, mediums=False)
    assert len(systems) == 9
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
    assert [sys.name for sys in systems['fortified']] == ["Frey", "Nurundere"]
    assert [sys.name for sys in systems['left'][0:3]] == [
        "LHS 3749", "Dongkum", "Alpha Fornacis"]
    assert [sys.name for sys in systems['undermined']] == [
        "Nurundere", "Dongkum", 'WW Piscis Austrini', 'LPM 229']
    assert [sys.name for sys in systems['skipped']] == ['Sol', 'Phra Mool']


def test_fort_find_current_index(session, f_dusers, f_fort_testbed):
    assert cogdb.query.fort_find_current_index(session) == 1


def test_fort_find_system(session, f_dusers, f_fort_testbed):
    sys = cogdb.query.fort_find_system(session, 'Sol')
    assert isinstance(sys, cogdb.schema.FortSystem)
    assert sys.name == 'Sol'

    sys = cogdb.query.fort_find_system(session, 'alp')
    assert sys.name == 'Alpha Fornacis'


def test_fort_get_targets(session, f_dusers, f_fort_testbed):
    targets = cogdb.query.fort_get_targets(session)
    assert [sys.name for sys in targets] == ['Nurundere', 'Othime', 'Rhea']


def test_fort_get_next_targets(session, f_dusers, f_fort_testbed):
    targets = cogdb.query.fort_get_next_targets(session)
    assert [sys.name for sys in targets] == ["LHS 3749"]

    targets = cogdb.query.fort_get_next_targets(session, count=2)
    assert [sys.name for sys in targets] == ["LHS 3749", "Alpha Fornacis"]


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
    to_drop = cogdb.query.MAX_DROP + 1

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.fort_add_drop(session, system=system, user=user, amount=to_drop)


def test_fort_order_get(session, f_dusers, f_fort_testbed, f_fortorders):
    names = []
    for system in cogdb.query.fort_order_get(session):
        assert isinstance(system, FortSystem)
        names += [system.name]
    assert names == [order.system_name for order in f_fortorders]


def test_fort_order_set(session, f_dusers, f_fort_testbed, f_fortorders):
    cogdb.query.fort_order_drop(session, cogdb.query.fort_order_get(session))
    assert cogdb.query.fort_order_get(session) == []

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.fort_order_set(session, ['Cubeo'])

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.fort_order_set(session, ['Sol', 'Sol', 'Sol'])

    expect = ['Sol', 'LPM 229']
    cogdb.query.fort_order_set(session, expect)
    assert [sys.name for sys in cogdb.query.fort_order_get(session)] == expect


def test_fort_order_drop(session, f_dusers, f_fort_testbed, f_fortorders):
    systems = cogdb.query.fort_order_get(session)
    cogdb.query.fort_order_drop(session, systems[:2])

    session.commit()
    assert session.query(FortOrder).all() == [f_fortorders[2]]


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
    systems = [system.name for system in cogdb.query.um_get_systems(session)]
    assert 'Cemplangpa' not in systems

    systems = [system.name for system in
               cogdb.query.um_get_systems(session, exclude_finished=False)]
    assert 'Cemplangpa' in systems


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


def test_add_channel_perm(session, f_cperms):
    server = Guild('Test', id=10)
    channel = Channel('AChannel', id=3001)
    cogdb.query.add_channel_perm(session, 'Status', server, channel)

    obj = session.query(ChannelPerm).filter_by(cmd='Status', channel_id=channel.id).one()
    assert obj.cmd == 'Status'

    with pytest.raises(cog.exc.InvalidCommandArgs):
        try:
            channel.id = 2001
            cogdb.query.add_channel_perm(session, 'Drop', server, channel)
        finally:
            session.rollback()


def test_add_role_perm(session, f_rperms):
    server = Guild('Test', id=10)
    role = Role("ARole", id=2201)
    cogdb.query.add_role_perm(session, 'Status', server, role)

    obj = session.query(RolePerm).filter_by(cmd='Status', role_id=role.id).one()
    assert obj.cmd == 'Status'

    with pytest.raises(cog.exc.InvalidCommandArgs):
        try:
            role.id = 3001
            cogdb.query.add_role_perm(session, 'Drop', server, role)
        finally:
            session.rollback()


def test_remove_channel_perm(session, f_cperms):
    perm = f_cperms[0]
    server = Guild('Test', id=perm.server_id)
    channel = Channel('AChannel', id=perm.channel_id)
    cogdb.query.remove_channel_perm(session, perm.cmd, server, channel)

    assert not session.query(ChannelPerm).all()

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.remove_channel_perm(session, perm.cmd, server, channel)


def test_remove_role_perm(session, f_rperms):
    perm = f_rperms[0]
    server = Guild('Test', id=perm.server_id)
    role = Role('ARole', id=perm.role_id)
    cogdb.query.remove_role_perm(session, perm.cmd, server, role)

    assert not session.query(RolePerm).all()

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.remove_role_perm(session, perm.cmd, server, role)


def test_check_perms(session, f_cperms, f_rperms):
    ops_channel = Channel("Operations", id=2001)
    server = Guild('Test', id=10)
    server.channels = [ops_channel]
    ops_channel.server = server
    roles = [Role('FRC Member', id=3001), Role('Winters', id=3002)]
    author = Member('User1', roles)
    msg = Message('!drop', author, server, ops_channel)
    msg.channel = ops_channel
    msg.channel.guild = server

    cogdb.query.check_perms(session, msg, mock.Mock(cmd='Drop'))  # Silent pass

    with pytest.raises(cog.exc.InvalidPerms):
        msg.author.roles = [Role('Winters', id=3002)]
        cogdb.query.check_perms(session, msg, mock.Mock(cmd='Drop'))

    with pytest.raises(cog.exc.InvalidPerms):
        msg.author.roles = roles
        msg.channel.name = 'Not Operations'
        msg.channel.id = 9999
        cogdb.query.check_perms(session, msg, mock.Mock(cmd='Drop'))


def test_check_channel_perms(session, f_cperms):
    # Silently pass if no raise
    cogdb.query.check_channel_perms(session, 'Drop', Guild('Test', id=10), Channel('Operations', id=2001))
    cogdb.query.check_channel_perms(session, 'Time', Guild('NoPerm', id=2333), Channel('Operations', id=2001))

    with pytest.raises(cog.exc.InvalidPerms):
        cogdb.query.check_channel_perms(session, 'Drop', Guild('Test', id=10), Channel('Not Operations', id=2002))


def test_check_role_perms(session, f_rperms):
    # Silently pass if no raise
    cogdb.query.check_role_perms(session, 'Drop', Guild('Test', id=10),
                                 [Role('FRC Member', id=3001), Role('FRC Vet', id=4000)])
    cogdb.query.check_role_perms(session, 'Time', Guild('NoPerm', id=2333),
                                 [Role('Winters', None)])

    with pytest.raises(cog.exc.InvalidPerms):
        cogdb.query.check_role_perms(session, 'Drop', Guild('Test', id=10),
                                     [Role('FRC Robot', id=2222), Role('Cookies', id=221)])


def test_complete_control_name():
    assert cogdb.query.complete_control_name("lush") == "Lushertha"

    assert cogdb.query.complete_control_name("pupp", True) == "18 Puppis"

    with pytest.raises(cog.exc.NoMatch):
        assert cogdb.query.complete_control_name("not_there")

    with pytest.raises(cog.exc.MoreThanOneMatch):
        assert cogdb.query.complete_control_name("lhs")


def test_kos_search_cmdr(session, f_kos):
    results = cogdb.query.kos_search_cmdr(session, 'good_guy')
    assert len(results) == 2
    assert sorted([x.cmdr for x in results]) == sorted(['good_guy', 'good_guy_pvp'])


def test_kos_add_cmdr(session, f_kos):
    cogdb.query.kos_add_cmdr(session, 'cmdr', 'faction', 'A reason', False)
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
    system_name = "Rhea"
    cap = cogdb.query.track_add_systems(session, [system_name], distance=20)
    assert cap == []


def test_track_remove_systems(session, f_track_testbed):
    system_names = ["Rhea", "Rana"]
    captured = cogdb.query.track_remove_systems(session, system_names)
    session.commit()

    found = session.query(TrackSystem).all()
    assert captured == ["Rhea"]
    assert len(found) == 1
    assert found[0].system == "Nanomam"


def test_track_get_all_systems(session, f_track_testbed):
    captured = cogdb.query.track_get_all_systems(session)

    assert list(sorted([x.system for x in captured])) == ["Nanomam", "Rhea"]


def test_track_show_systems(session, f_track_testbed):
    expected = """__Tracking System Rules__

    Tracking systems <= 15ly from Nanomam
    Tracking systems <= 15ly from Rhea"""

    captured = cogdb.query.track_show_systems(session)
    assert captured == [expected]


def test_track_systems_computed_update(session, f_track_testbed):
    system_names = ["Rana", "Rhea"]

    cap = cogdb.query.track_systems_computed_update(session, system_names)
    session.commit()

    assert len(cap) == 1
    found = session.query(TrackSystemCached).filter(TrackSystemCached.system.in_(system_names)).all()
    assert len(found) == 2


def test_track_systems_computed_remove(session, f_track_testbed):
    system_names = ["Rana", "Rhea"]
    expected = [
        '44 chi Draconis',
        'Acihaut',
        'Amun',
        'BD-13 2439',
        'Bodedi',
        'DX 799',
        'G 239-25',
        'Lalande 18115',
        'LFT 880',
        'LHS 1885',
        'LHS 215',
        'LHS 221',
        'LHS 2459',
        'LHS 246',
        'LHS 262',
        'LHS 283',
        'LHS 6128',
        'LP 5-88',
        'LP 64-194',
        'LP 726-6',
        'LQ Hydrae',
        'Masans',
        'Nang Ta-khian',
        'Nanomam',
        'Orishpucho',
        'Santal',
        'Tollan'
    ]

    cap = cogdb.query.track_systems_computed_remove(session, system_names)
    session.commit()

    assert cap == expected
    found = session.query(TrackSystemCached).filter(TrackSystemCached.system.in_(system_names)).all()
    assert len(found) == 1


def test_track_systems_computed_check(session, f_track_testbed):
    assert not cogdb.query.track_systems_computed_check(session, "Kappa")
    assert cogdb.query.track_systems_computed_check(session, "Rhea")


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
    cap = cogdb.query.track_ids_show(session)
    expected_1 = ["""__Tracking IDs__

J3J-WVT [CLBF] seen in **No Info** at 2000-01-10 00:00:00.
J3N-53B [CLBF] seen in **No Info** at 2000-01-12 00:00:00.
OVE-111 [Manual] seen in **No Info** at 2000-01-12 00:00:00.
XNL-3XQ [CLBF] seen in **No Info** at 2000-01-10 00:00:00."""]
    assert cap == expected_1


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
