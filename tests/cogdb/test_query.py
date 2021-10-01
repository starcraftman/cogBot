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
                          UMUser, UMHold, EUMSheet, AdminPerm, ChannelPerm, RolePerm,
                          KOS, TrackSystem, TrackSystemCached, TrackByID,
                          OCRTracker, OCRTrigger, OCRPrep, Global, Vote, VoteType)
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
    systems = [system.name for system in cogdb.query.um_get_systems(session)]
    assert 'Cemplangpa' not in systems
    assert 'Burr' in systems
    assert 'AF Leopris' in systems

    systems = [system.name for system in
               cogdb.query.um_get_systems(session, exclude_finished=False)]
    assert 'Cemplangpa' in systems
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


def test_check_perms(session, f_cperms, f_rperms):
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

    cogdb.query.check_perms(session, msg, 'drop')  # Silent pass

    with pytest.raises(cog.exc.InvalidPerms):
        msg.author.roles = [Role('Winters', id=3002)]
        cogdb.query.check_perms(session, msg, 'drop')

    with pytest.raises(cog.exc.InvalidPerms):
        msg.author.roles = roles
        msg.channel.name = 'Not Operations'
        msg.channel.id = 9999
        cogdb.query.check_perms(session, msg, 'drop')


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
    cap = cogdb.query.track_ids_show(session)
    expected = ["""__Tracking IDs__

J3J-WVT [CLBF] jumped **Nanomam** => **Rana**.
J3N-53B [CLBF] jumped **No Info** => **No Info**.
OVE-111 [Manual] jumped **No Info** => **No Info**.
XNL-3XQ [CLBF] jumped **No Info** => **Tollan**, near Tollan."""]
    assert cap == expected


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


def test_update_ocr_live(session, f_ocr_testbed):
    session.query(OCRTracker).delete()
    session.add(OCRTracker(system='Sol', fort=3333, um=2222, updated_at=datetime.datetime(2021, 8, 23, 0, 33, 20)))
    session.commit()
    test_data = {
        'Adeo': {
            'system': 'Adeo',
            'fort': 4444,
            'um': 2332,
            'updated_at': datetime.datetime(2021, 8, 23, 0, 33, 20),
        },
        'Sol': {
            'system': 'Sol',
            'fort': 7777,
            'um': 8888,
            'updated_at': datetime.datetime(2021, 8, 23, 0, 33, 20),
        },
    }
    cogdb.query.update_ocr_live(session, test_data)

    tracks = session.query(OCRTracker).order_by(OCRTracker.system.asc()).all()
    assert tracks[0].um == 2332
    assert tracks[1].fort == 7777


def test_update_ocr_trigger(session, f_ocr_testbed):
    session.query(OCRTrigger).delete()
    session.add(OCRTrigger(system='Sol', fort_trigger=7777, um_trigger=9999, updated_at=datetime.datetime(2021, 8, 23, 0, 33, 20)))
    session.commit()
    test_data = {
        'Adeo': {
            'system': 'Adeo',
            'fort_trigger': 4444,
            'um_trigger': 2332,
            'base_income': 94,
            'last_upkeep': 0,
            'updated_at': datetime.datetime(2021, 8, 23, 0, 33, 20),
        },
        'Sol': {
            'system': 'Sol',
            'fort_trigger': 3333,
            'um_trigger': 400,
            'base_income': 4,
            'last_upkeep': 30,
            'updated_at': datetime.datetime(2021, 8, 23, 0, 33, 20),
        },
    }
    cogdb.query.update_ocr_trigger(session, test_data)

    tracks = session.query(OCRTrigger).order_by(OCRTrigger.system.asc()).all()
    assert tracks[0].um_trigger == 2332
    assert tracks[1].fort_trigger == 3333
    assert tracks[1].base_income == 4


def test_update_ocr_prep(session, f_ocr_testbed):
    session.query(OCRPrep).delete()
    session.add(OCRPrep(system='Sol', merits=7777, updated_at=datetime.datetime(2021, 8, 23, 0, 33, 20)))
    session.commit()
    test_data = {
        'Cubeo': {
            'system': 'Cubeo',
            'merits': 3421,
            'updated_at': datetime.datetime(2021, 8, 23, 0, 33, 20),
        },
        'Sol': {
            'system': 'Sol',
            'merits': 333,
            'updated_at': datetime.datetime(2021, 8, 23, 0, 33, 20),
        },
    }
    cogdb.query.update_ocr_prep(session, test_data)

    tracks = session.query(OCRPrep).order_by(OCRPrep.system.asc()).all()
    assert tracks[0].system == 'Cubeo'
    assert tracks[1].merits == 333


def test_get_oldest_trigger_empty(session):
    oldest = cogdb.query.get_oldest_ocr_trigger(session)
    assert oldest is None


def test_get_oldest_trigger(session, f_ocr_testbed):
    test_data = {
        'Adeo': {
            'system': 'Adeo',
            'fort_trigger': 4444,
            'um_trigger': 2332,
            'base_income': 94,
            'last_upkeep': 0,
            'updated_at': datetime.datetime(2021, 8, 23, 0, 33, 20),
        },
        'Sol': {
            'system': 'Sol',
            'fort_trigger': 3333,
            'um_trigger': 400,
            'base_income': 4,
            'last_upkeep': 30,
            'updated_at': datetime.datetime(2021, 10, 25, 0, 33, 20),
        },
    }
    session.add(OCRTrigger(**test_data['Adeo']))
    session.add(OCRTrigger(**test_data['Sol']))
    session.commit()

    oldest = cogdb.query.get_oldest_ocr_trigger(session)
    assert oldest.system == "Adeo"


def test_get_current_global(session, f_global_testbed):
    session.query(Global).delete()
    current = cogdb.query.get_current_global(session)
    assert current.consolidation == 0
    current.consolidation = 77
    session.commit()

    with cogdb.session_scope(cogdb.Session):
        latest = cogdb.query.get_current_global(session)
        latest == current
        latest.consolidation == 77


# Manually setup a situation to test update of db.
def test_ocr_update_forts(session, db_cleanup):
    date = datetime.datetime(2021, 8, 25, 2, 33, 0)
    fort_systems = [
        FortSystem(id=1, name='Frey', fort_status=4910, trigger=4910, fort_override=0.7, um_status=0, undermine=0.0, distance=116.99, notes='', sheet_col='G', sheet_order=1),
        FortSystem(id=2, name='Nurundere', fort_status=5422, trigger=8425, fort_override=0.6, um_status=0, undermine=0.0, distance=99.51, notes='', sheet_col='H', sheet_order=2),
        FortSystem(id=4, name='Sol', fort_status=2500, trigger=5211, fort_override=0.0, um_status=2250, undermine=0.0, distance=28.94, notes='Leave For Grinders', sheet_col='J', sheet_order=4)
    ]
    ocr_tracks = [
        OCRTracker(id=1, system="Frey", fort=5555, um=2500, updated_at=date),
        OCRTracker(id=2, system="Nurundere", fort=4444, um=1000, updated_at=date),
        OCRTracker(id=3, system="Sol", fort=4444, um=1250, updated_at=date),
    ]
    ocr_triggers = [
        OCRTrigger(id=3, system="Sol", fort_trigger=8000, um_trigger=1000, updated_at=date),
    ]
    session.add_all(fort_systems + ocr_tracks + ocr_triggers)
    session.commit()
    cell_updates = cogdb.query.ocr_update_fort_status(session)
    session.commit()

    system = session.query(FortSystem).filter(FortSystem.name == 'Frey').one()
    assert system.fort_status == 5555
    assert system.um_status == 2500
    system = session.query(FortSystem).filter(FortSystem.name == 'Nurundere').one()
    assert system.fort_status == 5422
    assert system.um_status == 1000
    system = session.query(FortSystem).filter(FortSystem.name == 'Sol').one()
    assert system.fort_status == 4444
    assert system.um_status == 2250

    expect = [{'range': 'G6:G7', 'values': [[5555], [2500]]}, {'range': 'H6:H7', 'values': [[5422], [1000]]}, {'range': 'J6:J7', 'values': [[4444], [2250]]}]
    assert cell_updates == expect


def test_ocr_prep_report(session, f_ocr_testbed, f_global_testbed):
    prep = f_ocr_testbed[2][0]
    session.add(OCRPrep(system='Cubeo', merits=5531, updated_at=prep.updated_at))
    session.commit()
    expect = """__Hudson Preps Report__

Current Consolidation: 77%

Cubeo: 5531, updated at 2021-08-25 02:33:00
Rhea: 0, updated at 2021-08-25 02:33:00"""
    assert cogdb.query.ocr_prep_report(session) == expect


def test_post_cycle_db_cleanup(session, f_ocr_testbed, f_vote_testbed):
    cogdb.query.post_cycle_db_cleanup(session)

    for tracker in session.query(OCRTracker):
        assert tracker.fort == 0
        assert tracker.um == 0

    assert session.query(Vote).all() == []


def test_vote_add(session, f_dusers, f_vote_testbed, f_global_testbed):
    returned_message = cogdb.query.add_vote(session, f_dusers[1].id, 'prep', 1)
    expected_message = "**{}**: voted 1 Prep.".format(f_dusers[1].display_name)
    assert str(returned_message) == expected_message

    the_vote = cogdb.query.add_vote(session, f_dusers[1].id, VoteType.cons, 5)
    expect = "**{}**: voted 5 Cons.".format(f_dusers[1].display_name)
    assert str(the_vote) == expect


def test_vote_get(session, f_dusers, f_vote_testbed):
    # Existing
    the_vote = cogdb.query.get_vote(session, f_dusers[1].id, VoteType.cons)
    assert the_vote.id == f_dusers[1].id
    assert the_vote.vote == VoteType.cons

    # Doesn't exist
    the_vote = cogdb.query.get_vote(session, f_dusers[2].id, VoteType.prep)
    assert the_vote.id == f_dusers[2].id
    assert the_vote.vote == VoteType.prep


def test_get_all_snipe_holds(session, f_bot, f_dusers, f_um_testbed):
    expected = [UMHold(id=7, sheet_src=EUMSheet.snipe, system_id=7, user_id=3, held=5000, redeemed=1200)]
    results = cogdb.query.get_all_snipe_holds(session)
    assert results == expected


def test_get_snipe_members_holding(session, f_bot, f_dusers, f_um_testbed):
    expected = 'User3 is holding 5000 merits in ToSnipe\n'
    results = cogdb.query.get_snipe_members_holding(session, f_bot.guilds[0])
    assert results == expected
