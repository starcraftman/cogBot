"""
Test cogdb.query module.
"""
from __future__ import absolute_import, print_function
import sqlalchemy.orm.exc
import mock
import pytest

import cog.exc
import cogdb
from cogdb.schema import (DUser, System, SheetRow, SheetCattle, SheetUM,
                          Hold, UMExpand, EFaction, ESheetType, Admin,
                          ChannelPerm, RolePerm, FortOrder)
import cogdb.query

from tests.data import SYSTEMS, USERS
from tests.conftest import Channel, Member, Message, Role, Server


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
    duserQ = cogdb.query.get_duser(session, '1000')
    assert isinstance(duserQ, DUser)
    assert duserQ.display_name == 'GearsandCogs'

    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.get_duser(session, '0')


def test_ensure_duser_no_create(session, f_dusers):
    expect = f_dusers[0]
    member = mock.Mock()
    member.id = '1000'
    member.display_name = 'default'
    duser = cogdb.query.ensure_duser(session, member)
    assert duser == expect


def test_ensure_duser_create(session, f_dusers):
    member = mock.Mock()
    member.id = '2000'
    member.display_name = 'NewUser'
    duser = cogdb.query.ensure_duser(session, member)
    assert duser.display_name == member.display_name

    last_user = session.query(DUser).all()[-1]
    assert last_user == duser


def test_add_duser(session, f_dusers):
    member = mock.Mock()
    member.id = '2000'
    member.display_name = 'NewUser'

    cogdb.query.add_duser(session, member)
    assert session.query(DUser).all()[-1].id == member.id

    member.id = '2001'
    member.display_name += '2'
    cogdb.query.add_duser(session, member, faction=EFaction.winters)
    assert session.query(DUser).all()[-1].id == member.id


def test_check_pref_name(session, f_dusers, f_sheets):
    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.check_pref_name(session, f_dusers[1], f_dusers[0].pref_name)

    # No raise
    cogdb.query.check_pref_name(session, f_dusers[0], f_dusers[0].pref_name)


def test_next_sheet_row(session, f_dusers, f_sheets):
    expected_row = session.query(SheetCattle).order_by(SheetCattle.row.desc()).first().row + 1
    row = cogdb.query.next_sheet_row(session, cls=SheetCattle, faction=EFaction.hudson,
                                     start_row=11)
    assert row == expected_row

    user = session.query(SheetCattle).order_by(SheetCattle.row).limit(2).all()[1]
    expected_row = user.row
    session.delete(user)
    row = cogdb.query.next_sheet_row(session, cls=SheetCattle, faction=EFaction.hudson,
                                     start_row=11)
    assert row == expected_row

    for user in session.query(SheetRow):
        session.delete(user)
    session.commit()

    row = cogdb.query.next_sheet_row(session, cls=SheetCattle, faction=EFaction.hudson,
                                     start_row=11)
    assert row == 11


def test_add_sheet(session, f_dusers, f_sheets):
    test_name = 'Jack'
    test_cry = 'I do not cry'
    cogdb.query.add_sheet(session, test_name, cry=test_cry, faction=EFaction.winters,
                          type=ESheetType.undermine, start_row=5)
    latest = session.query(SheetRow).all()[-1]
    assert latest.name == test_name
    assert latest.cry == test_cry
    assert latest.row == 5
    assert latest.type == ESheetType.undermine

    test_name = 'John'
    test_cry = 'I cry'
    cogdb.query.add_sheet(session, test_name, cry=test_cry, faction=EFaction.winters,
                          type=ESheetType.undermine, start_row=5)
    latest = session.query(SheetRow).all()[-1]
    assert latest.name == test_name
    assert latest.cry == test_cry
    assert latest.row == 6
    assert latest.type == ESheetType.undermine


def test_fort_get_othime(session, f_systems):
    assert cogdb.query.fort_get_othime(session).name == 'Othime'


def test_fort_get_systems(session, f_systems):
    systems = cogdb.query.fort_get_systems(session)
    assert len(systems) == 10
    assert systems[0].name == 'Frey'
    assert systems[-1].name == 'LPM 229'


def test_fort_get_preps(session, f_prepsystem):
    systems = cogdb.query.fort_get_preps(session)
    assert [system.name for system in systems] == ['Rhea']


def test_fort_get_systems_not_othime(session, f_systems):
    systems = cogdb.query.fort_get_systems(session, not_othime=True)
    assert len(systems) == 9
    assert systems[0].name == 'Frey'
    assert systems[-1].name == 'LPM 229'


def test_fort_get_systems_by_state(session, f_systems):
    systems = cogdb.query.fort_get_systems(session)
    systems[1].fort_status = 8425
    systems[1].undermine = 1.0
    systems[4].undermine = 1.9
    session.commit()

    systems = cogdb.query.fort_get_systems_by_state(session)
    assert [sys.name for sys in systems['cancelled']] == ["Nurundere"]
    assert [sys.name for sys in systems['fortified']] == ["Frey", "Nurundere"]
    assert [sys.name for sys in systems['left'][0:3]] == ["LHS 3749", "Dongkum",
                                                          "Alpha Fornacis"]
    assert [sys.name for sys in systems['undermined']] == ["Nurundere", "Dongkum"]
    assert [sys.name for sys in systems['skipped']] == ['Sol']


def test_fort_find_current_index(session, f_systems):
    assert cogdb.query.fort_find_current_index(session) == 1


def test_fort_find_system(session, f_systems):
    sys = cogdb.query.fort_find_system(session, 'Sol')
    assert isinstance(sys, cogdb.schema.System)
    assert sys.name == 'Sol'

    sys = cogdb.query.fort_find_system(session, 'alp')
    assert sys.name == 'Alpha Fornacis'


def test_fort_get_targets(session, f_systems, f_prepsystem):
    targets = cogdb.query.fort_get_targets(session)
    assert [sys.name for sys in targets] == ['Nurundere', 'Othime', 'Rhea']


def test_fort_get_next_targets(session, f_systems):
    targets = cogdb.query.fort_get_next_targets(session)
    assert [sys.name for sys in targets] == ["LHS 3749"]

    targets = cogdb.query.fort_get_next_targets(session, count=2)
    assert [sys.name for sys in targets] == ["LHS 3749", "Alpha Fornacis"]


def test_fort_add_drop(session, f_dusers, f_sheets, f_systems, db_cleanup):
    system = session.query(System).filter(System.name == 'Sol').one()
    user = f_sheets[4]

    with pytest.raises(sqlalchemy.orm.exc.NoResultFound):
        session.query(cogdb.schema.Drop).filter_by(user_id=user.id,
                                                   system_id=system.id).one()

    old_fort = system.fort_status
    drop = cogdb.query.fort_add_drop(session, system=system, user=user, amount=400)
    session.commit()

    assert drop.amount == 400
    assert system.fort_status == old_fort + 400
    assert session.query(cogdb.schema.Drop).filter_by(user_id=user.id, system_id=system.id).one()


def test_fort_order_get(session, f_systems, f_fortorders):
    names = []
    for system in cogdb.query.fort_order_get(session):
        assert isinstance(system, System)
        names += [system.name]
    assert names == [order.system_name for order in f_fortorders]


def test_fort_order_set(session, f_systems, f_fortorders):
    cogdb.query.fort_order_drop(session, cogdb.query.fort_order_get(session))
    assert cogdb.query.fort_order_get(session) == []

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.fort_order_set(session, ['Cubeo'])

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.fort_order_set(session, ['Sol', 'Sol', 'Sol'])

    expect = ['Sol', 'LPM 229']
    cogdb.query.fort_order_set(session, expect)
    assert [sys.name for sys in cogdb.query.fort_order_get(session)] == expect


def test_fort_order_drop(session, f_systems, f_fortorders):
    systems = cogdb.query.fort_order_get(session)
    cogdb.query.fort_order_drop(session, systems[:2])

    assert cogdb.Session().query(FortOrder).all() == [f_fortorders[2]]


def test_fortscanner_find_system_column(mock_fortsheet):
    scanner = cogdb.query.FortScanner(mock_fortsheet)
    scanner.cells = mock_fortsheet.whole_sheet()
    scanner.find_system_column() == 'F'

    with pytest.raises(cog.exc.SheetParsingError):
        scanner.cells = [[''], ['CMDR Name']]
        scanner.find_system_column()


def test_fortscanner_merits(mock_fortsheet):
    scanner = cogdb.query.FortScanner(mock_fortsheet)
    scanner.scan()

    session = cogdb.Session()
    fort1 = session.query(cogdb.schema.Drop).all()[0]
    assert fort1.amount == 2222
    assert fort1.system.name == 'Frey'
    assert fort1.user.name == 'Toliman'


def test_fortscanner_systems(mock_fortsheet):
    scanner = cogdb.query.FortScanner(mock_fortsheet)
    scanner.scan()
    result = [sys.name for sys in scanner.systems()]
    assert result == SYSTEMS[:6] + ['Othime']


def test_fortscanner_users(mock_fortsheet):
    scanner = cogdb.query.FortScanner(mock_fortsheet)
    scanner.cells = scanner.gsheet.whole_sheet()
    result = [suser.name for suser in scanner.users(SheetCattle, EFaction.hudson)]
    assert result == USERS


def test_umscanner_systems(mock_umsheet):
    scanner = cogdb.query.UMScanner(mock_umsheet)
    scanner.cells = scanner.gsheet.whole_sheet()
    system = scanner.systems()[0]
    assert system.name == 'Burr'
    assert isinstance(system, UMExpand)


def test_umscanner_users(mock_umsheet):
    scanner = cogdb.query.UMScanner(gsheet=mock_umsheet)
    scanner.cells = scanner.gsheet.whole_sheet()
    users = scanner.users(SheetUM, EFaction.hudson)
    result = [user.name for user in users]
    expect = ['Haphollas', 'Rico Char', 'MalvadoDiablo', 'Harmsus', 'Otorno', 'Blackneto',
              'Paul Redpath', 'Xxxreaper752xxx ', 'FRENZY86', 'Sardaukar17', 'SpongeDoc',
              'ActionFace', 'ilNibbio', 'Tomis[XB1]', 'UEG LONE', 'tfcheps', 'xxSNEAKELLAMAxx',
              'Alexander Astropath', 'Rimos', 'Shepron', 'Willa', 'North Man', 'Tiddymun',
              'Horizon', 'Phantom50Elite', 'BaronGreenback', 'Fod4u2', 'Eastbourne',
              'KineticTrauma', 'CyberCarnivore', 'Renegade Bovine', 'crazyjay', 'harlequin_420th',
              'NascentChemist', 'Oskiboy[PC/XB1]', 'Muaddib', 'DRAGON DARKO', 'Gaz Cullen']
    assert result == expect
    assert isinstance(users[0], SheetUM)


def test_umscanner_merits(session, mock_umsheet):
    scanner = cogdb.query.UMScanner(gsheet=mock_umsheet)
    scanner.scan()
    session.commit()

    holds = session.query(cogdb.schema.Hold).all()
    hold = [hold for hold in holds if
            hold.system.name == 'Burr' and hold.user.name == 'Tomis[XB1]'][0]
    assert hold.held == 7330
    assert hold.redeemed == 890


def test_um_find_system(session, f_systemsum):
    system = cogdb.query.um_find_system(session, 'Cemplangpa')
    assert system.name == 'Cemplangpa'

    system = cogdb.query.um_find_system(session, 'Cemp')
    assert system.name == 'Cemplangpa'

    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.um_find_system(session, 'NotThere')

    with pytest.raises(cog.exc.MoreThanOneMatch):
        cogdb.query.um_find_system(session, 'r')


def test_um_get_systems(session, f_systemsum):
    systems = [system.name for system in cogdb.query.um_get_systems(session)]
    assert 'Cemplangpa' not in systems

    systems = [system.name for system in
               cogdb.query.um_get_systems(session, exclude_finished=False)]
    assert 'Cemplangpa' in systems


def test_um_reset_held(session, f_dusers, f_sheets, f_systemsum, f_holds):
    sheet = f_sheets[1]
    assert sheet.merit_summary() == 'Holding 2600, Redeemed 11350'
    cogdb.query.um_reset_held(session, sheet)
    assert sheet.merit_summary() == 'Holding 0, Redeemed 11350'


def test_um_redeem_merits(session, f_dusers, f_sheets, f_systemsum, f_holds):
    sheet = f_sheets[1]
    assert sheet.merit_summary() == 'Holding 2600, Redeemed 11350'
    cogdb.query.um_redeem_merits(session, sheet)
    assert sheet.merit_summary() == 'Holding 0, Redeemed 13950'


def test_um_add_hold(session, f_dusers, f_sheets, f_systemsum, db_cleanup):
    sheet = f_sheets[1]
    system = f_systemsum[0]
    assert not session.query(Hold).all()

    cogdb.query.um_add_hold(session, held=600, system=system, user=sheet)
    hold = session.query(Hold).filter_by(system_id=system.id, user_id=sheet.id).one()
    assert hold.held == 600

    hold = cogdb.query.um_add_hold(session, held=2000, system=system, user=sheet)
    hold = session.query(Hold).filter_by(system_id=system.id, user_id=sheet.id).one()
    assert hold.held == 2000


def test_um_all_held_merits(session, f_dusers, f_sheets, f_systemsum, f_holds):
    expect = [
        ['CMDR', 'Cemplangpa', 'Pequen', 'Burr', 'AF Leopris', 'Empty'],
        ['rjwhite', 450, 2400, 0, 0, 0],
        ['GearsandCogs', 0, 400, 2200, 0, 0]
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

    assert session.query(Admin).all()[-1].id == f_dusers[-1].id


def test_add_channel_perm(session, f_cperms):
    cogdb.query.add_channel_perm(session, 'Status', 'Gears Hideout', 'general')

    obj = session.query(ChannelPerm).filter_by(cmd='Status', channel='general').one()
    assert obj.cmd == 'Status'

    with pytest.raises(cog.exc.InvalidCommandArgs):
        try:
            cogdb.query.add_channel_perm(session, 'Drop', 'Gears Hideout', 'operations')
        finally:
            session.rollback()


def test_add_role_perm(session, f_rperms):
    cogdb.query.add_role_perm(session, 'Status', 'Gears Hideout', 'Cookie Tyrant')

    obj = session.query(RolePerm).filter_by(cmd='Status', role='Cookie Tyrant').one()
    assert obj.cmd == 'Status'

    with pytest.raises(cog.exc.InvalidCommandArgs):
        try:
            cogdb.query.add_role_perm(session, 'Drop', 'Gears Hideout', 'FRC Member')
        finally:
            session.rollback()


def test_remove_channel_perm(session, f_cperms):
    perm = f_cperms[0]
    cogdb.query.remove_channel_perm(session, perm.cmd, perm.server, perm.channel)

    assert not session.query(ChannelPerm).all()

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.remove_channel_perm(session, perm.cmd, perm.server, perm.channel)


def test_remove_role_perm(session, f_rperms):
    perm = f_rperms[0]
    cogdb.query.remove_role_perm(session, perm.cmd, perm.server, perm.role)

    assert not session.query(RolePerm).all()

    with pytest.raises(cog.exc.InvalidCommandArgs):
        cogdb.query.remove_role_perm(session, perm.cmd, perm.server, perm.role)


def test_check_perms(session, f_cperms, f_rperms):
    ops_channel = Channel("operations")
    server = Server('Gears Hideout')
    server.channels = [ops_channel]
    ops_channel.server = server
    roles = [Role('FRC Member'), Role('Winters')]
    author = Member('Gears', roles)
    msg = Message('!drop', author, server, ops_channel, None)

    cogdb.query.check_perms(msg, mock.Mock(cmd='Drop'))  # Silent pass

    with pytest.raises(cog.exc.InvalidPerms):
        msg.author.roles = [Role('Winters')]
        cogdb.query.check_perms(msg, mock.Mock(cmd='Drop'))

    with pytest.raises(cog.exc.InvalidPerms):
        msg.author.roles = roles
        msg.channel.name = 'not_pperations'
        cogdb.query.check_perms(msg, mock.Mock(cmd='Drop'))


def test_check_channel_perms(session, f_cperms):
    # Silently pass if no raise
    cogdb.query.check_channel_perms(session, 'Drop', 'Gears Hideout', 'operations')
    cogdb.query.check_channel_perms(session, 'Time', 'Does Not Matter', 'operations')

    with pytest.raises(cog.exc.InvalidPerms):
        cogdb.query.check_channel_perms(session, 'Drop', 'Gears Hideout', 'not_operations')


def test_check_role_perms(session, f_rperms):
    # Silently pass if no raise
    cogdb.query.check_role_perms(session, 'Drop', 'Gears Hideout',
                                 [Role('FRC Member'), Role('FRC Vet')])
    cogdb.query.check_role_perms(session, 'Time', 'Does Not Matter',
                                 [Role('Winters', None)])

    with pytest.raises(cog.exc.InvalidPerms):
        cogdb.query.check_role_perms(session, 'Drop', 'Gears Hideout',
                                     [Role('FRC Robot'), Role('Cookies')])


def test_complete_control_name():
    assert cogdb.query.complete_control_name("lush") == "Lushertha"

    assert cogdb.query.complete_control_name("pupp", True) == "18 Puppis"

    with pytest.raises(cog.exc.NoMatch):
        assert cogdb.query.complete_control_name("not_there")

    with pytest.raises(cog.exc.MoreThanOneMatch):
        assert cogdb.query.complete_control_name("lhs")
