"""
Test the schema for the database.
"""
from __future__ import absolute_import, print_function
import copy

import pytest

import cog.exc
import cogdb
import cogdb.schema
from cogdb.schema import (DUser, System, Drop, Hold,
                          SheetRow, SheetCattle, SheetUM,
                          SystemUM, UMControl, UMExpand, UMOppose,
                          EFaction, ESheetType, kwargs_um_system, kwargs_fort_system,
                          Admin, ChannelPerm, RolePerm)

from tests.data import SYSTEMS_DATA, SYSTEMSUM_DATA, SYSTEMUM_EXPAND


def test_empty_tables_all(session, f_dusers, f_sheets, f_systems, f_drops, f_systemsum, f_holds):
    classes = [DUser, SheetRow, System, SystemUM, Drop, Hold]
    for cls in classes:
        assert session.query(cls).all()

    cogdb.schema.empty_tables(session, perm=True)
    session.commit()

    for cls in classes:
        assert session.query(cls).all() == []


def test_empty_tables_not_all(session, f_dusers, f_sheets, f_systems, f_drops, f_systemsum, f_holds):
    classes = [DUser, SheetRow, System, SystemUM, Drop, Hold]
    for cls in classes:
        assert session.query(cls).all()

    cogdb.schema.empty_tables(session, perm=False)

    classes.remove(DUser)
    for cls in classes:
        assert session.query(cls).all() == []
    assert session.query(DUser).all()


def test_admin_remove(session, f_dusers, f_admins):
    first, second = f_admins
    with pytest.raises(cog.exc.InvalidPerms):
        second.remove(session, first)

    first.remove(session, second)
    assert len(session.query(Admin).all()) == 1


def test_admin__repr__(session, f_dusers, f_admins):
    assert repr(f_admins[0]) == "Admin(id='1000', date=datetime.datetime(2017, 9, 26, 13, 34, 39))"


def test_admin__str__(session, f_dusers, f_admins):
    assert repr(f_admins[0]) == "Admin(id='1000', date=datetime.datetime(2017, 9, 26, 13, 34, 39))"


def test_admin__eq__(session, f_dusers, f_admins):
    first, second = f_admins

    assert first == Admin(id="1000")
    assert first != second


def test_channelperm__repr__(session, f_cperms):
    perm = f_cperms[0]
    assert repr(perm) == "ChannelPerm(cmd='Drop', channel='operations')"


def test_channelperm__str__(session, f_cperms):
    perm = f_cperms[0]
    assert str(perm) == "ChannelPerm(cmd='Drop', channel='operations')"


def test_channelperm__eq__(session, f_cperms):
    perm = f_cperms[0]
    assert perm == ChannelPerm(cmd=perm.cmd, channel=perm.channel)
    assert perm != ChannelPerm(cmd=perm.cmd, channel='NoMatch')


def test_roleperm__repr__(session, f_rperms):
    perm = f_rperms[0]
    assert repr(perm) == "RolePerm(cmd='Drop', role='FRC Member')"


def test_roleperm__str__(session, f_rperms):
    perm = f_rperms[0]
    assert str(perm) == "RolePerm(cmd='Drop', role='FRC Member')"


def test_roleperm__eq__(session, f_rperms):
    perm = f_rperms[0]
    assert perm == RolePerm(cmd=perm.cmd, role=perm.role)
    assert perm != RolePerm(cmd=perm.cmd, role='NoMatch')


def test_duser__eq__(f_dusers, f_sheets):
    duser = f_dusers[0]
    assert duser != DUser(id='1111', display_name='test user',
                          pref_name='test user')
    assert duser == DUser(id=duser.id, display_name='test user',
                          pref_name='test user')


def test_duser__repr__(f_dusers, f_sheets):
    duser = f_dusers[0]
    assert repr(duser) == "DUser(id='1000', display_name='GearsandCogs', "\
                          "pref_name='GearsandCogs', pref_cry='', faction='hudson')"
    assert duser == eval(repr(duser))


def test_duser__str__(f_dusers, f_sheets):
    duser = f_dusers[0]
    assert str(duser) == "DUser(id='1000', display_name='GearsandCogs', "\
                         "pref_name='GearsandCogs', pref_cry='', faction='hudson')"


def test_duser_get_sheet(session, f_dusers, f_sheets):
    duser = f_dusers[0]
    assert not duser.get_sheet(session, ESheetType.undermine, faction=EFaction.winters)
    assert duser.get_sheet(session, ESheetType.cattle)
    assert isinstance(duser.get_sheet(session, ESheetType.cattle), SheetCattle)


def test_duser_cattle(session, f_dusers, f_sheets):
    duser = f_dusers[0]
    cattle = session.query(SheetCattle).filter(SheetCattle.name == duser.pref_name).one()

    assert duser.cattle(session) == cattle
    assert isinstance(duser.cattle(session), SheetCattle)
    duser.switch_faction()
    assert not duser.cattle(session)


def test_duser_undermine(session, f_dusers, f_sheets):
    duser = f_dusers[0]
    undermine = session.query(SheetUM).filter(SheetUM.name == duser.pref_name).one()

    assert duser.undermine(session) == undermine
    assert isinstance(duser.undermine(session), SheetUM)
    duser.switch_faction()
    assert not duser.undermine(session)


def test_duser_switch_faction(f_dusers, f_sheets):
    duser = f_dusers[0]

    assert duser.faction == EFaction.hudson
    duser.switch_faction()
    assert duser.faction == EFaction.winters
    duser.switch_faction(EFaction.hudson)
    assert duser.faction == EFaction.hudson


def test_sheetrow__eq__(f_dusers, f_sheets):
    sheet = f_sheets[0]
    equal = SheetCattle(name='GearsandCogs', type=ESheetType.cattle, faction=EFaction.hudson,
                        row=15, cry='Gears are forting late!')
    assert sheet == equal
    equal.name = 'notGears'
    assert sheet != equal


def test_sheetrow__repr__(f_dusers, f_sheets):
    sheet = f_sheets[0]
    assert repr(sheet) == "SheetCattle(name='GearsandCogs', type='SheetCattle', "\
                          "faction='hudson', row=15, cry='Gears are forting late!')"

    assert sheet == eval(repr(sheet))


def test_sheetrow__str__(f_dusers, f_sheets):
    sheet = f_sheets[0]
    assert str(sheet) == "id=1, SheetCattle(name='GearsandCogs', type='SheetCattle', "\
                         "faction='hudson', row=15, cry='Gears are forting late!')"


def test_sheetcattle_merit_summary(session, f_dusers, f_sheets, f_systems, f_drops):
    sheet = f_sheets[0]
    total = 0
    for drop in session.query(Drop).filter_by(user_id=sheet.id).all():
        total += drop.amount
    assert sheet.merit_summary() == 'Dropped {}'.format(total)


def test_sheetum_merit_summary(session, f_dusers, f_sheets, f_systemsum, f_holds):
    sheet = [sheet for sheet in f_sheets if sheet.name == 'GearsandCogs' and
             sheet.type == ESheetType.undermine][0]
    held, redeemed = 0, 0
    for hold in session.query(Hold).filter_by(user_id=sheet.id).all():
        held += hold.held
        redeemed += hold.redeemed
    assert sheet.merit_summary() == 'Holding {}, Redeemed {}'.format(held, redeemed)


def test_system__eq__(f_systems):
    system = f_systems[0]

    assert system == System(name='Frey')
    assert system != System(name='Sol')


def test_system__repr__(f_systems):
    system = f_systems[0]

    assert repr(system) == "System(name='Frey', fort_status=4910, trigger=4910, "\
                           "um_status=0, undermine=0.0, distance=116.99, "\
                           "notes='', sheet_col='G', sheet_order=1)"
    assert system == eval(repr(system))


def test_system__str__(f_systems):
    system = f_systems[0]

    assert str(system) == "id=1, cmdr_merits=0, System(name='Frey', fort_status=4910, "\
                          "trigger=4910, um_status=0, undermine=0.0, distance=116.99, "\
                          "notes='', sheet_col='G', sheet_order=1)"


def test_system_display(f_systems):
    system = f_systems[0]
    assert system.display() == '**Frey** 4910/4910 :Fortified:'

    system.fort_status = 4000
    assert system.display() == '**Frey** 4000/4910 :Fortifying: (910 left)'
    assert system.display(miss=False) == '**Frey** 4000/4910 :Fortifying:'


def test_system_set_status(f_systems):
    system = f_systems[0]
    assert system.fort_status == 4910
    assert system.um_status == 0

    system.set_status('4000')
    assert system.fort_status == 4000
    assert system.um_status == 0

    system.set_status('2200:2000')
    assert system.fort_status == 2200
    assert system.um_status == 2000


def test_system_current_status(f_systems):
    system = f_systems[0]
    assert system.current_status == 4910


def test_system_skip(f_systems):
    system = f_systems[0]
    assert system.skip is False

    system.notes = 'Leave for now.'
    assert system.skip is True


def test_system_is_fortified(f_systems):
    system = f_systems[0]
    system.fort_status = system.trigger
    assert system.is_fortified is True

    system.fort_status = system.fort_status // 2
    assert system.is_fortified is False


def test_system_is_undermined(f_systems):
    system = f_systems[0]

    system.undermine = 1.0
    assert system.is_undermined is True

    system.undermine = 0.4
    assert system.is_undermined is False


def test_system_missing(f_systems):
    system = f_systems[0]

    system.fort_status = system.trigger - 1000
    assert system.missing == 1000

    system.fort_status = system.trigger
    assert system.missing == 0

    system.fort_status = system.trigger + 1000
    assert system.missing == 0


def test_system_completion(f_systems):
    system = f_systems[0]
    assert system.completion == '100.0'
    system.trigger = 0
    assert system.completion == '0.0'


def test_system_table_row(f_systems):
    system = f_systems[0]
    system.notes = 'Leave'
    assert system.table_row == ('Fort', 'Frey', '   0', '4910/4910 (100.0%/0.0%)', 'Leave')


def test_prepsystem_dispay(f_prepsystem):
    assert f_prepsystem.display() == "Prep: **Rhea** 5100/10000 :Fortifying: "\
                                     "Atropos"


def test_prepsystem_is_fortified(f_prepsystem):
    f_prepsystem.fort_status = 10 * f_prepsystem.trigger
    assert not f_prepsystem.is_fortified


def test_drop__eq__(f_dusers, f_sheets, f_systems, f_drops):
    user = f_sheets[0]
    system = f_systems[0]
    drop = f_drops[0]
    assert drop == Drop(amount=700, user_id=user.id, system_id=system.id)
    assert drop.user == user
    assert drop.system == system


def test_drop__repr__(f_dusers, f_sheets, f_systems, f_drops):
    drop = f_drops[0]
    assert repr(drop) == "Drop(system_id=1, user_id=1, amount=700)"
    assert drop == eval(repr(drop))


def test_drop__str__(f_dusers, f_sheets, f_systems, f_drops):
    drop = f_drops[0]
    assert str(drop) == "id=1, system='Frey', user='GearsandCogs', "\
                        "Drop(system_id=1, user_id=1, amount=700)"


def test_hold__eq__(f_dusers, f_sheets, f_systemsum, f_holds):
    user = f_sheets[1]
    system = f_systemsum[0]
    hold = f_holds[0]

    assert hold == Hold(held=0, redeemed=4000, user_id=user.id, system_id=system.id)
    assert hold.user == user
    assert hold.system == system


def test_hold__repr__(f_dusers, f_sheets, f_systemsum, f_holds):
    hold = f_holds[0]
    assert repr(hold) == "Hold(system_id=1, user_id=2, held=0, redeemed=4000)"
    assert hold == eval(repr(hold))


def test_hold__str__(f_dusers, f_sheets, f_systemsum, f_holds):
    hold = f_holds[0]
    assert str(hold) == "id=1, system='Cemplangpa', user='GearsandCogs', "\
                        "Hold(system_id=1, user_id=2, held=0, redeemed=4000)"


def test_systemum__repr__(f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    assert repr(system) == "SystemUM(name='Cemplangpa', type='control', sheet_col='D', "\
                           "goal=14878, security='Medium', notes='', "\
                           "progress_us=15000, progress_them=1.0, "\
                           "close_control='Sol', map_offset=1380)"
    assert system == eval(repr(system))


def test_systemum__str__(f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    assert str(system) == "id=1, cmdr_merits=6450, SystemUM(name='Cemplangpa', "\
                          "type='control', sheet_col='D', "\
                          "goal=14878, security='Medium', notes='', "\
                          "progress_us=15000, progress_them=1.0, "\
                          "close_control='Sol', map_offset=1380)"
    assert system == eval(repr(system))


def test_systemum_display(f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    assert system.display() == \
        "Control: **Cemplangpa**, Security: Medium, Hudson Control: Sol\n"\
        "        Completion: 101%, Missing: -122\n"


def test_systemum__eq__(f_dusers, f_sheets, f_systemsum, f_holds):

    expect = SystemUM.factory(kwargs_um_system(SYSTEMSUM_DATA[0], 'F'))
    system = f_systemsum[0]

    assert system == expect


def test_systemum_cmdr_merits(session, f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    assert system.cmdr_merits == 6450


def test_systemum_missing(f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    system.progress_us = 0
    assert system.missing == 7048

    system.progress_us = 15000
    system.map_offset = 0
    assert system.missing == -122


def test_systemum_is_undermined(session, f_dusers, f_sheets, f_systemsum, f_holds):
    control = f_systemsum[1]
    assert not control.is_undermined
    control.progress_us = control.goal
    assert control.is_undermined

    exp = f_systemsum[-1]
    assert not exp.is_undermined


def test_systemum_set_status(f_systemsum):
    system = f_systemsum[0]

    system.set_status('4000')
    assert system.progress_us == 4000
    assert system.progress_them == 1.0

    system.set_status('2200:55')
    assert system.progress_us == 2200
    assert system.progress_them == 0.55


def test_systemum_completion(f_dusers, f_sheets, f_systemsum, f_holds):
    system = f_systemsum[0]

    assert system.completion == 'Completion: 101%'
    system.goal = 0
    assert system.completion == 'Completion: 0%'


def test_umexpand_completion(f_dusers, f_sheets, f_systemsum, f_holds):
    system = [system for system in f_systemsum if isinstance(system, UMExpand)][0]
    assert system.completion == 'Behind by 3500%'

    system.exp_trigger = 0
    assert system.completion == 'Behind by 3500%'


def test_umoppose_descriptor(f_dusers, f_sheets, f_systemsum, f_holds):
    system = [system for system in f_systemsum if isinstance(system, UMOppose)][0]
    assert system.descriptor == 'Opposing expansion'

    system.notes = 'AD expansion'
    assert system.descriptor == 'Opposing AD'


def test_kwargs_system_um():
    expect = {
        'close_control': 'Dongkum',
        'exp_trigger': 0,
        'goal': 364298,
        'name': 'Burr',
        'notes': '',
        'progress_us': 161630,
        'progress_them': 35.0,
        'security': 'Low',
        'sheet_col': 'D',
        'cls': UMExpand,
        'map_offset': 76548,
    }
    sys_cols = copy.deepcopy(SYSTEMUM_EXPAND)
    assert kwargs_um_system(sys_cols, 'D') == expect

    sys_cols[0][0] = 'Opp.'
    expect['cls'] = UMOppose
    assert kwargs_um_system(sys_cols, 'D') == expect

    sys_cols[0][0] = ''
    expect['cls'] = UMControl
    assert kwargs_um_system(sys_cols, 'D') == expect

    expect['map_offset'] = 0
    sys_cols[0] = sys_cols[0][:-1]
    assert kwargs_um_system(sys_cols, 'D') == expect

    with pytest.raises(cog.exc.SheetParsingError):
        kwargs_um_system([], 'D')


def test_kwargs_fort_system():
    expect = {
        'distance': 116.99,
        'fort_status': 4910,
        'name': 'Frey',
        'notes': '',
        'sheet_col': 'F',
        'sheet_order': 1,
        'trigger': 4910,
        'um_status': 0,
        'undermine': 0.0,
        'fort_override': 0.7,
    }
    assert kwargs_fort_system(SYSTEMS_DATA[0], 1, 'F') == expect

    with pytest.raises(cog.exc.SheetParsingError):
        kwargs_fort_system(['' for _ in range(0, 10)], 1, 'A')

    with pytest.raises(cog.exc.SheetParsingError):
        kwargs_fort_system([], 1, 'A')

    with pytest.raises(cog.exc.SheetParsingError):
        kwargs_fort_system(SYSTEMS_DATA[0][:-2], 1, 'A')


def test_parse_int():
    assert cogdb.schema.parse_int('') == 0
    assert cogdb.schema.parse_int('2') == 2
    assert cogdb.schema.parse_int(5) == 5


def test_parse_float():
    assert cogdb.schema.parse_float('') == 0.0
    assert cogdb.schema.parse_float('2') == 2.0
    assert cogdb.schema.parse_float(0.5) == 0.5
