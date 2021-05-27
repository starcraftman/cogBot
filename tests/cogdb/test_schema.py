"""
Test the schema for the database.
"""
import copy
import datetime

import pytest

import cog.exc
import cogdb
import cogdb.schema
from cogdb.schema import (DiscordUser, FortSystem, FortDrop, FortUser, FortOrder,
                          UMSystem, UMExpand, UMOppose, UMUser, UMHold,
                          AdminPerm, ChannelPerm, RolePerm,
                          kwargs_um_system, kwargs_fort_system)

from tests.data import SYSTEMS_DATA, SYSTEMSUM_DATA, SYSTEMUM_EXPAND
DB_CLASSES = [DiscordUser, FortUser, FortSystem, FortDrop, UMUser, UMSystem, UMHold]


def test_empty_tables_all(session, f_dusers, f_fort_testbed, f_um_testbed):
    for cls in DB_CLASSES:
        assert session.query(cls).all()

    cogdb.schema.empty_tables(session, perm=True)
    session.commit()

    for cls in DB_CLASSES:
        assert session.query(cls).all() == []


def test_empty_tables_not_all(session, f_dusers, f_fort_testbed, f_um_testbed):
    for cls in DB_CLASSES:
        assert session.query(cls).all()

    cogdb.schema.empty_tables(session, perm=False)

    classes = DB_CLASSES[:]
    classes.remove(DiscordUser)
    for cls in classes:
        assert session.query(cls).all() == []
    assert session.query(DiscordUser).all()


def test_admin_remove(session, f_dusers, f_admins):
    first, second = f_admins
    with pytest.raises(cog.exc.InvalidPerms):
        second.remove(session, first)

    first.remove(session, second)
    assert len(session.query(AdminPerm).all()) == 1


def test_admin__repr__(session, f_dusers, f_admins):
    assert repr(f_admins[0]) == "AdminPerm(id=1, date=datetime.datetime(2017, 9, 26, 13, 34, 39, 721018))"


def test_admin__str__(session, f_dusers, f_admins):
    assert repr(f_admins[0]) == "AdminPerm(id=1, date=datetime.datetime(2017, 9, 26, 13, 34, 39, 721018))"


def test_admin__eq__(session, f_dusers, f_admins):
    first, second = f_admins

    assert first == AdminPerm(id=1, date=datetime.datetime(2017, 9, 26, 13, 34, 39))
    assert first != second


def test_channelperm__repr__(session, f_cperms):
    perm = f_cperms[0]
    assert repr(perm) == "ChannelPerm(cmd='Drop', server_id=10, channel_id=2001)"


def test_channelperm__str__(session, f_cperms):
    perm = f_cperms[0]
    assert str(perm) == "ChannelPerm(cmd='Drop', server_id=10, channel_id=2001)"


def test_channelperm__eq__(session, f_cperms):
    perm = f_cperms[0]
    assert perm == ChannelPerm(cmd=perm.cmd, server_id=perm.server_id, channel_id=perm.channel_id)
    assert perm != ChannelPerm(cmd=perm.cmd, server_id=perm.server_id, channel_id=999999)


def test_roleperm__repr__(session, f_rperms):
    perm = f_rperms[0]
    assert repr(perm) == "RolePerm(cmd='Drop', server_id=10, role_id=3001)"


def test_roleperm__str__(session, f_rperms):
    perm = f_rperms[0]
    assert str(perm) == "RolePerm(cmd='Drop', server_id=10, role_id=3001)"


def test_roleperm__eq__(session, f_rperms):
    perm = f_rperms[0]
    assert perm == RolePerm(cmd=perm.cmd, server_id=perm.server_id, role_id=perm.role_id)
    assert perm != RolePerm(cmd=perm.cmd, server_id=perm.server_id, role_id=999999)


def test_fortorder__repr__(session, f_fortorders):
    order_sol = f_fortorders[0]
    assert repr(order_sol) == "FortOrder(order=1, system_name='Sol')"


def test_fortorder__str__(session, f_fortorders):
    order_sol = f_fortorders[0]
    assert str(order_sol) == "FortOrder(order=1, system_name='Sol')"


def test_fortorder__eq__(session, f_fortorders):
    order_sol = f_fortorders[0]
    assert order_sol == FortOrder(order=1, system_name='Sol')
    assert order_sol != FortOrder(order=1, system_name='Aornum')


def test_duser__eq__(f_dusers):
    duser = f_dusers[0]
    assert duser != DiscordUser(id=9, display_name='User1', pref_name='User1')
    assert duser == DiscordUser(id=duser.id, display_name='User1', pref_name='User1')


def test_duser__repr__(f_dusers):
    duser = f_dusers[0]
    assert repr(duser) == "DiscordUser(id=1, display_name='User1', pref_name='User1', pref_cry='')"
    assert duser == eval(repr(duser))


def test_duser__str__(f_dusers):
    duser = f_dusers[0]
    assert str(duser) == "DiscordUser(id=1, display_name='User1', pref_name='User1', pref_cry='')"


def test_duser_fort_relationships(f_dusers, f_fort_testbed):
    duser = f_dusers[0]
    assert duser.fort_user == FortUser(id=1, name='User1', row=15, cry='User1 are forting late!')
    assert duser.fort_merits == [FortDrop(id=1, system_id=1, user_id=1, amount=700), FortDrop(id=2, system_id=2, user_id=1, amount=400)]


def test_duser_um_relationships(f_dusers, f_um_testbed):
    duser = f_dusers[0]
    assert duser.um_user == UMUser(id=1, name='User1', row=18, cry='We go pew pew!')
    assert duser.um_merits == [UMHold(id=1, system_id=1, user_id=1, held=0, redeemed=4000), UMHold(id=2, system_id=2, user_id=1, held=400, redeemed=1550), UMHold(id=3, system_id=3, user_id=1, held=2200, redeemed=5800)]


def test_duser_total_merits(f_dusers, f_um_testbed, f_fort_testbed):
    assert f_dusers[0].total_merits == 15050


def test_duser_total_fort_merits(f_dusers, f_um_testbed, f_fort_testbed):
    assert f_dusers[0].total_fort_merits == 1100


def test_duser_total_um_merits(f_dusers, f_um_testbed, f_fort_testbed):
    assert f_dusers[0].total_um_merits == 13950


def test_fortuser__eq__(f_dusers, f_fort_testbed):
    f_user = f_fort_testbed[0][0]
    equal = FortUser(id=1, name='User1', row=22, cry='')
    assert f_user == equal
    equal.name = 'notUser1'
    assert f_user != equal


def test_fortuser__repr__(f_dusers, f_fort_testbed):
    f_user = f_fort_testbed[0][0]
    assert repr(f_user) == "FortUser(id=1, name='User1', row=15, cry='User1 are forting late!')"


def test_fortuser__str__(f_dusers, f_fort_testbed):
    f_user = f_fort_testbed[0][0]
    assert str(f_user) == "dropped=1100, FortUser(id=1, name='User1', row=15, cry='User1 are forting late!')"


def test_fortuser_dropped(f_dusers, f_fort_testbed):
    f_user = f_fort_testbed[0][0]
    assert f_user.dropped == 1100


def test_fortuser_dropped_expression(session, f_dusers, f_fort_testbed):
    users = [x[0] for x in session.query(FortUser.name).filter(FortUser.dropped > 1500)]

    assert users == ['User2', 'User3']


def test_fortuser_merit_summary(f_dusers, f_fort_testbed):
    f_user = f_fort_testbed[0][0]
    assert f_user.merit_summary() == "Dropped 1100"


def test_fortuser_relationships(f_dusers, f_fort_testbed):
    f_user = f_fort_testbed[0][0]
    assert f_user.discord_user == DiscordUser(id=1, display_name='User1', pref_name='User1', pref_cry='')
    drops = [
        FortDrop(id=1, system_id=1, user_id=1, amount=700),
        FortDrop(id=2, system_id=2, user_id=1, amount=400)
    ]
    assert f_user.merits == drops


def test_fortsystem__eq__(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]

    assert system == FortSystem(name='Frey')
    assert system != FortSystem(id=system.id, name='Sol')


def test_fortsystem__repr__(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]

    expect = "FortSystem(id=1, name='Frey', fort_status=4910, "\
        "trigger=4910, fort_override=0.7, um_status=0, undermine=0.0, distance=116.99, "\
        "notes='', sheet_col='G', sheet_order=1)"
    assert repr(system) == expect
    assert system == eval(repr(system))


def test_fortsystem__str__(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]

    expect = "cmdr_merits=3700, FortSystem(id=1, name='Frey', fort_status=4910, "\
        "trigger=4910, fort_override=0.7, um_status=0, undermine=0.0, distance=116.99, "\
        "notes='', sheet_col='G', sheet_order=1)"
    assert str(system) == expect


def test_fortsystem_cmdr_merits(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.cmdr_merits == 3700


def test_fortsystem_cmdr_merits_expression(session, f_dusers, f_fort_testbed):
    result = [x[0] for x in session.query(FortSystem.name).filter(FortSystem.cmdr_merits > 3000).all()]
    assert result == ['Frey']


def test_fortsystem_display(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.display() == '**Frey** 4910/4910 :Fortified:'

    system.fort_status = 4000
    assert system.display() == '**Frey** 4000/4910 :Fortifying: (910 left)'
    assert system.display(miss=False) == '**Frey** 4000/4910 :Fortifying:'


def test_fortsystem_display_details(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.display_details() == """**Frey**
```Completion  | 100.0%
CMDR Merits | 3700/4910
Fort Status | 4910/4910
UM Status   | 0 (0.00%)
Notes       |```"""

    system.fort_status = 4000
    assert system.display_details() == """**Frey**
```Completion  | 81.5% (910 left)
CMDR Merits | 3700/4910
Fort Status | 4000/4910
UM Status   | 0 (0.00%)
Notes       |```"""


def test_fortsystem_set_status(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.fort_status == 4910
    assert system.um_status == 0

    system.set_status('4000')
    assert system.fort_status == 4000
    assert system.um_status == 0

    system.set_status('2200:2000')
    assert system.fort_status == 2200
    assert system.um_status == 2000


def test_fortsystem_current_status(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.current_status == 4910


def test_fortsystem_skip(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.skip is False

    system.notes = 'Leave for now.'
    assert system.skip is True


def test_fortsystem_skip_expression(session, f_dusers, f_fort_testbed):
    skips = session.query(FortSystem.name).filter(FortSystem.skip).all()

    assert [x[0] for x in skips] == ["Sol", "Phra Mool"]


def test_fortsystem_is_medium(session, f_dusers, f_fort_testbed):
    first = f_fort_testbed[1][0]
    assert not first.is_medium

    system = session.query(FortSystem).filter(FortSystem.name == "Othime").one()
    assert system.is_medium


def test_fortsystem_is_medium_expression(session, f_dusers, f_fort_testbed):
    mediums = session.query(FortSystem.name).filter(FortSystem.is_medium).all()

    assert [x[0] for x in mediums] == ["Othime"]


def test_fortsystem_is_fortified(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    system.fort_status = system.trigger
    assert system.is_fortified is True

    system.fort_status = system.fort_status // 2
    assert system.is_fortified is False


def test_fortsystem_is_undermined(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]

    system.undermine = 1.0
    assert system.is_undermined is True

    system.undermine = 0.4
    assert system.is_undermined is False


def test_fortsystem_is_undermined_expression(session, f_dusers, f_fort_testbed):
    umed = session.query(FortSystem.name).filter(FortSystem.is_undermined).all()

    assert [x[0] for x in umed] == ["WW Piscis Austrini", "LPM 229"]


def test_fortsystem_missing(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]

    system.fort_status = system.trigger - 1000
    assert system.missing == 1000

    system.fort_status = system.trigger
    assert system.missing == 0

    system.fort_status = system.trigger + 1000
    assert system.missing == 0


def test_fortsystem_completion(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.completion == '100.0'
    system.trigger = 0
    assert system.completion == '0.0'


def test_fortsystem_table_row(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    system.notes = 'Leave'
    assert system.table_row == ('Fort', 'Frey', '   0', '4910/4910 (100.0%/0.0%)', 'Leave')


def test_prepsystem_dispay(f_dusers, f_fort_testbed):
    f_prep = f_fort_testbed[1][-1]
    assert f_prep.display() == "Prep: **Rhea** 5100/10000 :Fortifying: Atropos"


def test_prepsystem_is_fortified(f_dusers, f_fort_testbed):
    f_prep = f_fort_testbed[1][-1]
    f_prep.fort_status = 10 * f_prep.trigger
    assert not f_prep.is_fortified


def test_drop__eq__(f_dusers, f_fort_testbed):
    user, system, drop = f_fort_testbed[0][0], f_fort_testbed[1][0], f_fort_testbed[2][0]
    assert drop == FortDrop(amount=700, user_id=user.id, system_id=system.id)
    assert drop.user == user
    assert drop.system == system


def test_drop__repr__(f_dusers, f_fort_testbed):
    user, system, drop = f_fort_testbed[0][0], f_fort_testbed[1][0], f_fort_testbed[2][0]
    assert repr(drop) == "FortDrop(id=1, system_id={}, user_id={}, amount=700)".format(
        system.id, user.id)
    assert drop == eval(repr(drop))


def test_drop__str__(f_dusers, f_fort_testbed):
    user, system, drop = f_fort_testbed[0][0], f_fort_testbed[1][0], f_fort_testbed[2][0]
    assert str(drop) == "system='Frey', user='User1', "\
                        "FortDrop(id=1, system_id={}, user_id={}, amount=700)".format(
        system.id, user.id)


def test_umuser__eq__(f_dusers, f_um_testbed):
    f_user = f_um_testbed[0][0]
    equal = UMUser(id=1, name='User1', row=22, cry='')
    assert f_user == equal
    equal.name = 'notUser1'
    assert f_user != equal


def test_umuser__repr__(f_dusers, f_um_testbed):
    f_user = f_um_testbed[0][0]
    assert repr(f_user) == "UMUser(id=1, name='User1', row=18, cry='We go pew pew!')"


def test_umuser__str__(f_dusers, f_um_testbed):
    f_user = f_um_testbed[0][0]
    assert str(f_user) == "held=2600, redeemed=11350, UMUser(id=1, name='User1', row=18, cry='We go pew pew!')"


def test_umuser_held(f_dusers, f_um_testbed):
    f_user = f_um_testbed[0][0]
    assert f_user.held == 2600


def test_umuser_held_expression(session, f_dusers, f_um_testbed):
    users = [x[0] for x in session.query(UMUser.name).filter(UMUser.held > 2800)]
    assert users == ['User2']


def test_umuser_redeemed(f_dusers, f_um_testbed):
    f_user = f_um_testbed[0][0]
    assert f_user.redeemed == 11350


def test_umuser_redeemed_expression(session, f_dusers, f_um_testbed):
    users = [x[0] for x in session.query(UMUser.name).filter(UMUser.redeemed > 10000)]
    assert users == ['User1']


def test_umuser_merit_summary(f_dusers, f_um_testbed):
    f_user = f_um_testbed[0][0]
    assert f_user.merit_summary() == "Holding 2600, Redeemed 11350"


def test_umuser_relationships(f_dusers, f_um_testbed):
    f_user = f_um_testbed[0][0]
    assert f_user.discord_user == DiscordUser(id=1, display_name='User1', pref_name='User1', pref_cry='')
    holds = [
        UMHold(id=1, system_id=1, user_id=1, held=0, redeemed=4000),
        UMHold(id=2, system_id=2, user_id=1, held=400, redeemed=1550),
        UMHold(id=3, system_id=3, user_id=1, held=2200, redeemed=5800),
    ]
    assert f_user.merits == holds


def test_umsystem__repr__(f_dusers, f_um_testbed):
    system = f_um_testbed[1][0]

    assert repr(system) == "UMSystem(id=1, name='Cemplangpa', sheet_col='D', "\
                           "goal=14878, security='Medium', notes='', "\
                           "progress_us=15000, progress_them=1.0, "\
                           "close_control='Sol', priority='Medium', map_offset=1380)"
    assert system == eval(repr(system))


def test_umsystem__str__(f_dusers, f_um_testbed):
    system = f_um_testbed[1][0]

    assert str(system) == "cmdr_merits=6450, UMSystem(id=1, name='Cemplangpa', "\
                          "sheet_col='D', goal=14878, security='Medium', notes='', "\
                          "progress_us=15000, progress_them=1.0, "\
                          "close_control='Sol', priority='Medium', map_offset=1380)"
    assert system == eval(repr(system))


def test_umsystem_display(f_dusers, f_um_testbed):
    system = f_um_testbed[1][0]

    assert system.display() == """```Control            | Cemplangpa [M sec]
101%               | Merits Leading 122
Our Progress 15000 | Enemy Progress 100%
Nearest Hudson     | Sol
Priority           | Medium
Power              |```"""


def test_umsystem__eq__(f_dusers, f_um_testbed):

    expect = UMSystem.factory(kwargs_um_system(SYSTEMSUM_DATA[0], 'F'))
    system = f_um_testbed[1][0]

    assert system == expect


def test_umsystem_cmdr_merits(session, f_dusers, f_um_testbed):
    system = f_um_testbed[1][0]

    assert system.cmdr_merits == 6450


def test_umsystem_cmdr_merits_expression(session, f_dusers, f_um_testbed):
    result = [x[0] for x in session.query(UMSystem.name).filter(UMSystem.cmdr_merits > 5000).all()]
    assert result == ['Burr', 'Cemplangpa']


def test_umsystem_missing(f_dusers, f_um_testbed):
    system = f_um_testbed[1][0]

    system.progress_us = 0
    assert system.missing == 7048

    system.progress_us = 15000
    system.map_offset = 0
    assert system.missing == -122


def test_umsystem_is_undermined(session, f_dusers, f_um_testbed):
    control = f_um_testbed[1][1]
    assert not control.is_undermined
    control.progress_us = control.goal
    assert control.is_undermined

    exp = f_um_testbed[1][-1]
    assert not exp.is_undermined


def test_umsystem_set_status(f_dusers, f_um_testbed):
    system = f_um_testbed[1][0]

    system.set_status('4000')
    assert system.progress_us == 4000
    assert system.progress_them == 1.0

    system.set_status('2200:55')
    assert system.progress_us == 2200
    assert system.progress_them == 0.55


def test_umsystem_completion(f_dusers, f_um_testbed):
    system = f_um_testbed[1][0]

    assert system.completion == '101%'
    system.goal = 0
    assert system.completion == '0%'


def test_umexpand_completion(f_dusers, f_um_testbed):
    system = [system for system in f_um_testbed[1] if isinstance(system, UMExpand)][0]
    assert system.completion == 'Behind by 3500%'

    system.exp_trigger = 0
    assert system.completion == 'Behind by 3500%'


def test_umoppose_descriptor(f_dusers, f_um_testbed):
    system = [system for system in f_um_testbed[1] if isinstance(system, UMOppose)][0]
    assert system.descriptor == 'Opposing expansion'

    system.notes = 'AD expansion'
    assert system.descriptor == 'Opposing AD'


def test_hold__eq__(f_dusers, f_um_testbed):
    user, system, hold = f_um_testbed[0][0], f_um_testbed[1][0], f_um_testbed[2][0]

    assert hold == UMHold(held=0, redeemed=4000, user_id=user.id, system_id=system.id)
    assert hold.user == user
    assert hold.system == system


def test_hold__repr__(f_dusers, f_um_testbed):
    user, system, hold = f_um_testbed[0][0], f_um_testbed[1][0], f_um_testbed[2][0]
    assert repr(hold) == "UMHold(id=1, system_id={}, user_id={}, held=0, redeemed=4000)".format(
        system.id, user.id)
    assert hold == eval(repr(hold))


def test_hold__str__(f_dusers, f_um_testbed):
    user, system, hold = f_um_testbed[0][0], f_um_testbed[1][0], f_um_testbed[2][0]
    assert str(hold) == "system='Cemplangpa', user='User1', "\
                        "UMHold(id=1, system_id={}, user_id={}, held=0, redeemed=4000)".format(
        system.id, user.id)


def test_kos__repr__(f_kos):
    assert repr(f_kos[0]) == "KOS(id=1, cmdr='good_guy', faction='Hudson', reason='Very good', is_friendly=1)"


def test_kos__str__(f_kos):
    assert str(f_kos[0]) == "KOS(id=1, cmdr='good_guy', faction='Hudson', reason='Very good', is_friendly=1)"


def test_kos__eq__(f_kos):
    assert f_kos[0] == f_kos[0]
    assert f_kos[0] != f_kos[1]


def test_kos_friendly(f_kos):
    assert f_kos[0].friendly == 'FRIENDLY'
    assert f_kos[2].friendly == 'KILL'


def test_kwargs_system_um():
    expect = {
        'close_control': 'Dongkum',
        'cls': UMExpand,
        'exp_trigger': 0,
        'goal': 364298,
        'map_offset': 76548,
        'name': 'Burr',
        'notes': '',
        'priority': 'Medium',
        'progress_them': 35.0,
        'progress_us': 161630,
        'security': 'Low',
        'sheet_col': 'D',
    }
    sys_cols = copy.deepcopy(SYSTEMUM_EXPAND)
    assert kwargs_um_system(sys_cols, 'D') == expect

    sys_cols[0][0] = 'Opp.'
    expect['cls'] = UMOppose
    assert kwargs_um_system(sys_cols, 'D') == expect

    sys_cols[0][0] = ''
    expect['cls'] = UMSystem
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
