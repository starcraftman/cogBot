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
                          AdminPerm, ChannelPerm, RolePerm, EUMSheet,
                          kwargs_um_system, kwargs_fort_system)

from tests.data import SYSTEMS_DATA, SYSTEMSUM_DATA, SYSTEMUM_EXPAND
DB_CLASSES = [DiscordUser, FortUser, FortSystem, FortDrop, UMUser, UMSystem, UMHold]


OCR_TOO_HIGH = 999999


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
    assert repr(f_admins[0]) == "AdminPerm(id=1, date=datetime.datetime(2017, 9, 26, 13, 34, 39))"


def test_admin__str__(session, f_dusers, f_admins):
    assert repr(f_admins[0]) == "AdminPerm(id=1, date=datetime.datetime(2017, 9, 26, 13, 34, 39))"


def test_admin__eq__(session, f_dusers, f_admins):
    first, second = f_admins

    assert first == AdminPerm(id=1, date=datetime.datetime(2017, 9, 26, 13, 34, 39))
    assert first != second


def test_channelperm__repr__(session, f_cperms):
    perm = f_cperms[0]
    assert repr(perm) == "ChannelPerm(cmd='drop', guild_id=10, channel_id=2001)"


def test_channelperm__str__(session, f_cperms):
    perm = f_cperms[0]
    assert str(perm) == "ChannelPerm(cmd='drop', guild_id=10, channel_id=2001)"


def test_channelperm__eq__(session, f_cperms):
    perm = f_cperms[0]
    assert perm == ChannelPerm(cmd=perm.cmd, guild_id=perm.guild_id, channel_id=perm.channel_id)
    assert perm != ChannelPerm(cmd=perm.cmd, guild_id=perm.guild_id, channel_id=999999)


def test_roleperm__repr__(session, f_rperms):
    perm = f_rperms[0]
    assert repr(perm) == "RolePerm(cmd='drop', guild_id=10, role_id=3001)"


def test_roleperm__str__(session, f_rperms):
    perm = f_rperms[0]
    assert str(perm) == "RolePerm(cmd='drop', guild_id=10, role_id=3001)"


def test_roleperm__eq__(session, f_rperms):
    perm = f_rperms[0]
    assert perm == RolePerm(cmd=perm.cmd, guild_id=perm.guild_id, role_id=perm.role_id)
    assert perm != RolePerm(cmd=perm.cmd, guild_id=perm.guild_id, role_id=999999)


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


def test_duser_mention(f_dusers):
    duser = f_dusers[0]
    assert duser.mention == "<@1>"


def test_duser_mention_expression(session, f_dusers):
    result = session.query(DiscordUser.id).filter(DiscordUser.mention.ilike("%1>")).scalar()
    assert result == 1


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
    assert system.display() == '**Frey** 4910/4910 :Fortified: - 116.99Ly'

    system.fort_status = 4000
    assert system.display() == '**Frey** 4000/4910 :Fortifying: (910 left) - 116.99Ly'
    assert system.display(miss=False) == '**Frey** 4000/4910 :Fortifying: - 116.99Ly'


def test_fortsystem_display_details(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.display_details() == """**Frey**
```Completion  | 100.0%
CMDR Merits | 3700/4910
Fort Status | 4910/4910
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


def test_fortsystem_current_status_expression(session, f_dusers, f_fort_testbed):
    systems = session.query(FortSystem).\
        filter(FortSystem.current_status >= 4000).\
        order_by(FortSystem.id).\
        all()
    assert len(systems) == 4
    assert systems[0].current_status == 4910


def test_fortsystem_priority(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.is_priority is False

    system.notes = 'Priority for small ships'
    assert system.is_priority is True


def test_fortsystem_priority_expression(session, f_dusers, f_fort_testbed):
    priorities = session.query(FortSystem.name).filter(FortSystem.is_priority).all()

    assert [x[0] for x in priorities] == ["Othime"]


def test_fortsystem_skip(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.is_skipped is False

    system.notes = 'Leave for now.'
    assert system.is_skipped is True


def test_fortsystem_skip_expression(session, f_dusers, f_fort_testbed):
    skips = session.query(FortSystem.name).filter(FortSystem.is_skipped).all()

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


def test_fortsystem_is_fortified_expression(session, f_dusers, f_fort_testbed):
    sys = session.query(FortSystem).filter(FortSystem.is_fortified).first()
    assert sys.name == 'Frey'
    assert sys.is_fortified


def test_fortsystem_is_undermined(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]

    system.undermine = 1.0
    assert system.is_undermined is True

    system.undermine = 0.4
    assert system.is_undermined is False


def test_fortsystem_is_undermined_expression(session, f_dusers, f_fort_testbed):
    umed = session.query(FortSystem.name).filter(FortSystem.is_undermined).all()
    assert [x[0] for x in umed] == ["WW Piscis Austrini", "LPM 229"]


def test_fortsystem_is_deferred(session, f_dusers, f_fort_testbed):
    system = session.query(FortSystem).filter(FortSystem.name == 'Dongkum').one()
    assert system.is_deferred


def test_fortsystem_is_deferred_expression(session, f_dusers, f_fort_testbed):
    deferred = session.query(FortSystem.name).filter(FortSystem.is_deferred).all()
    assert deferred[0][0] == 'Dongkum'


def test_fortsystem_missing(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]

    system.fort_status = system.trigger - 1000
    assert system.missing == 1000

    system.fort_status = system.trigger
    assert system.missing == 0

    system.fort_status = system.trigger + 1000
    assert system.missing == 0


def test_fortsystem_missing_expression(session, f_dusers, f_fort_testbed):
    result = session.query(FortSystem.name, FortSystem.missing).\
        filter(FortSystem.missing > 1000).\
        first()
    assert result[0] == 'Nurundere'
    assert result[1] == 3003


def test_fortsystem_completion(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    assert system.completion == '100.0'
    system.trigger = 0
    assert system.completion == '0.0'


def test_fortsystem_table_row(f_dusers, f_fort_testbed):
    system = f_fort_testbed[1][0]
    system.notes = 'Leave'
    assert system.table_row == ('Fort', 'Frey', '   0', '4910/4910 (100.0%/0.0%)', 'Leave')


def test_prepsystem_display(f_dusers, f_fort_testbed):
    f_prep = f_fort_testbed[1][-1]
    assert f_prep.display() == "Prep: **Rhea** 5100/10000 :Fortifying: Atropos - 65.55Ly"


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
    assert repr(f_user) == "UMUser(id=1, sheet_src=EUMSheet.main, name='User1', row=18, cry='We go pew pew!')"


def test_umuser__str__(f_dusers, f_um_testbed):
    f_user = f_um_testbed[0][0]
    assert str(f_user) == "held=2600, redeemed=11350, UMUser(id=1, sheet_src=EUMSheet.main, name='User1', row=18, cry='We go pew pew!')"


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

    assert repr(system) == "UMSystem(id=1, sheet_src=EUMSheet.main, name='Cemplangpa', sheet_col='D', "\
                           "goal=14878, security='Medium', notes='', "\
                           "progress_us=15000, progress_them=1.0, "\
                           "close_control='Sol', priority='Medium', map_offset=1380)"
    assert system == eval(repr(system))


def test_umsystem__str__(f_dusers, f_um_testbed):
    system = f_um_testbed[1][0]

    assert str(system) == "cmdr_merits=6450, UMSystem(id=1, sheet_src=EUMSheet.main, "\
                          "name='Cemplangpa', sheet_col='D', goal=14878, security='Medium', notes='', "\
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
    system_names = session.query(UMSystem.name).\
        filter(UMSystem.cmdr_merits > 5000,
               UMSystem.sheet_src == EUMSheet.main).\
        order_by(UMSystem.name).\
        all()
    result = [x[0] for x in system_names]
    assert result == ['Burr', 'Cemplangpa']


def test_umsystem_missing(f_dusers, f_um_testbed):
    system = f_um_testbed[1][0]

    system.progress_us = 0
    assert system.missing == 7048

    system.progress_us = 15000
    system.map_offset = 0
    assert system.missing == -122


def test_umsystem_missing_expression(session, f_dusers, f_um_testbed):
    systems = session.query(UMSystem.name, UMSystem.missing).\
        filter(UMSystem.missing == 10000).\
        all()
    assert systems == [('Empty', 10000)]


def test_umsystem_is_undermined(session, f_dusers, f_um_testbed):
    control = f_um_testbed[1][1]
    assert not control.is_undermined
    control.progress_us = control.goal
    assert control.is_undermined

    exp = f_um_testbed[1][-1]
    assert not exp.is_undermined


def test_umsystem_is_undermined_expression(session, f_dusers, f_um_testbed):
    systems = session.query(UMSystem.name, UMSystem.is_undermined).\
        filter(UMSystem.is_undermined).\
        all()
    assert systems == [('Cemplangpa', True)]


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
    assert repr(hold) == "UMHold(id=1, sheet_src=EUMSheet.main, system_id={}, user_id={}, held=0, redeemed=4000)".format(
        system.id, user.id)
    assert hold == eval(repr(hold))


def test_hold__str__(f_dusers, f_um_testbed):
    user, system, hold = f_um_testbed[0][0], f_um_testbed[1][0], f_um_testbed[2][0]
    assert str(hold) == "system='Cemplangpa', user='User1', "\
                        "UMHold(id=1, sheet_src=EUMSheet.main, system_id={}, user_id={}, held=0, redeemed=4000)".format(
        system.id, user.id)


def test_kos__repr__(f_kos):
    assert repr(f_kos[0]) == "KOS(id=1, cmdr='good_guy', squad='Hudson', reason='Very good', is_friendly=True)"


def test_kos__str__(f_kos):
    assert str(f_kos[0]) == "KOS(id=1, cmdr='good_guy', squad='Hudson', reason='Very good', is_friendly=True)"


def test_kos__eq__(f_kos):
    assert f_kos[0] == f_kos[0]
    assert f_kos[0] != f_kos[1]


def test_kos_friendly(f_kos):
    assert f_kos[0].friendly == 'FRIENDLY'
    assert f_kos[2].friendly == 'KILL'


def test_tracksystem__repr__(f_track_testbed):
    track_system = f_track_testbed[0][0]

    assert repr(track_system) == "TrackSystem(system='Nanomam', distance=15)"


def test_tracksystem__str__(f_track_testbed):
    track_system = f_track_testbed[0][0]

    assert str(track_system) == "Tracking systems <= 15ly from Nanomam"


def test_tracksystemcached__repr__(f_track_testbed):
    track_system = f_track_testbed[1][0]

    assert repr(track_system) == "TrackSystemCached(system='44 chi Draconis', overlaps_with='Nanomam')"


def test_tracksystemcached_add_overlap(f_track_testbed):
    track_system = f_track_testbed[1][0]

    track_system.add_overlap("Tollan")
    track_system.add_overlap("Adeo")
    assert track_system.overlaps_with == "Nanomam, Tollan, Adeo"


def test_tracksystemcached_remove_overlap(f_track_testbed):
    track_system = f_track_testbed[1][0]
    track_system.add_overlap("Tollan")
    track_system.add_overlap("Adeo")
    assert track_system.overlaps_with == "Nanomam, Tollan, Adeo"

    assert not track_system.remove_overlap("tolLan")
    assert not track_system.remove_overlap("ADEO")
    assert track_system.remove_overlap("Nanomam")
    assert track_system.overlaps_with == ""


def test_trackbyid__repr__(f_track_testbed):
    track_id = f_track_testbed[2][0]

    expect = "TrackByID(id='J3J-WVT', squad='CLBF', system='Rana', last_system='Nanomam', override=False, updated_at=datetime.datetime(2000, 1, 10, 0, 0))"
    assert repr(track_id) == expect


def test_trackbyid__str__(f_track_testbed):
    track_id = f_track_testbed[2][0]

    expect = "J3J-WVT [CLBF] jumped **Nanomam** => **Rana**."
    assert str(track_id) == expect


def test_trackbyid_table_line(f_track_testbed):
    track_id = f_track_testbed[2][0]

    assert track_id.table_line() == ('J3J-WVT', 'CLBF', 'Rana', 'Nanomam')


def test_trackbyid_spotted(f_track_testbed):
    track_id = f_track_testbed[2][0]

    track_id.spotted("Cubeo")
    assert track_id.system == "Cubeo"
    assert track_id.last_system == "Rana"


def test_ocrtracker__str__(f_ocr_testbed):
    tracker = f_ocr_testbed[0][0]

    assert str(tracker) == "Frey: **Fort 0**  **UM 0**, updated at 2021-08-25 02:33:00"


def test_ocrtracker__repr__(f_ocr_testbed):
    tracker = f_ocr_testbed[0][0]

    assert repr(tracker) == "OCRTracker(id=1, system='Frey', fort=0, um=0, updated_at=datetime.datetime(2021, 8, 25, 2, 33))"


def test_ocrtracker_update(f_ocr_testbed):
    tracker = f_ocr_testbed[0][0]

    with pytest.raises(cog.exc.ValidationFail):
        tracker.update(fort=9999, um=8888)

    tracker.update(fort=-1, um=8888, updated_at=tracker.updated_at + datetime.timedelta(minutes=1))
    assert tracker.um == 8888
    assert tracker.fort == 0


def test_ocrtracker_validate_fort(f_ocr_testbed):
    tracker = f_ocr_testbed[0][0]

    with pytest.raises(cog.exc.ValidationFail):
        tracker.fort = -1
    with pytest.raises(cog.exc.ValidationFail):
        tracker.fort = OCR_TOO_HIGH


def test_ocrtracker_validate_um(f_ocr_testbed):
    tracker = f_ocr_testbed[0][0]

    with pytest.raises(cog.exc.ValidationFail):
        tracker.um = -1
    with pytest.raises(cog.exc.ValidationFail):
        tracker.um = OCR_TOO_HIGH


def test_ocrtracker_validate_updated_at(f_ocr_testbed):
    tracker = f_ocr_testbed[0][0]

    with pytest.raises(cog.exc.ValidationFail):
        tracker.updated_at = 0
    with pytest.raises(cog.exc.ValidationFail):
        tracker.updated_at = datetime.datetime(1000, 1, 1)


def test_ocrtrigger_update(f_ocr_testbed):
    trigger = f_ocr_testbed[1][0]

    with pytest.raises(cog.exc.ValidationFail):
        trigger.update(fort_trigger=9999, um_trigger=8888)

    trigger.update(fort_trigger=-1, um_trigger=8888, updated_at=trigger.updated_at + datetime.timedelta(minutes=1))
    assert trigger.um_trigger == 8888
    assert trigger.fort_trigger == 500


def test_ocrtrigger__str__(f_ocr_testbed):
    trigger = f_ocr_testbed[1][0]

    assert str(trigger) == "Frey: 500:1000 with income of 50 and upkeep 24, updated at 2021-08-25 02:33:00"


def test_ocrtrigger__repr__(f_ocr_testbed):
    trigger = f_ocr_testbed[1][0]

    assert repr(trigger) == "OCRTrigger(id=1, system='Frey', fort_trigger=500, um_trigger=1000, base_income=50, last_upkeep=24, updated_at=datetime.datetime(2021, 8, 25, 2, 33))"


def test_ocrtrigger_validate_fort_trigger(f_ocr_testbed):
    trigger = f_ocr_testbed[1][0]

    with pytest.raises(cog.exc.ValidationFail):
        trigger.fort_trigger = -1
    with pytest.raises(cog.exc.ValidationFail):
        trigger.fort_trigger = OCR_TOO_HIGH


def test_ocrtrigger_validate_um_trigger(f_ocr_testbed):
    trigger = f_ocr_testbed[1][0]

    with pytest.raises(cog.exc.ValidationFail):
        trigger.um_trigger = -1
    with pytest.raises(cog.exc.ValidationFail):
        trigger.um_trigger = OCR_TOO_HIGH


def test_ocrtrigger_validate_base_income(f_ocr_testbed):
    trigger = f_ocr_testbed[1][0]

    with pytest.raises(cog.exc.ValidationFail):
        trigger.base_income = -1
    with pytest.raises(cog.exc.ValidationFail):
        trigger.base_income = OCR_TOO_HIGH


def test_ocrtrigger_validate_last_upkeep(f_ocr_testbed):
    trigger = f_ocr_testbed[1][0]

    with pytest.raises(cog.exc.ValidationFail):
        trigger.last_upkeep = -1
    with pytest.raises(cog.exc.ValidationFail):
        trigger.last_upkeep = OCR_TOO_HIGH


def test_ocrtrigger_validate_updated_at(f_ocr_testbed):
    trigger = f_ocr_testbed[1][0]

    with pytest.raises(cog.exc.ValidationFail):
        trigger.updated_at = 0
    with pytest.raises(cog.exc.ValidationFail):
        trigger.updated_at = datetime.datetime(1000, 1, 1)


def test_ocrprep__str__(f_ocr_testbed):
    prep = f_ocr_testbed[2][0]

    assert str(prep) == "Rhea: 0, updated at 2021-08-25 02:33:00"


def test_ocrprep__repr__(f_ocr_testbed):
    prep = f_ocr_testbed[2][0]

    assert repr(prep) == "OCRPrep(id=1, system='Rhea', merits=0, updated_at=datetime.datetime(2021, 8, 25, 2, 33))"


def test_ocrprep_update(f_ocr_testbed):
    prep = f_ocr_testbed[2][0]

    with pytest.raises(cog.exc.ValidationFail):
        prep.update(merits=9999)

    prep.update(merits=-1, updated_at=prep.updated_at + datetime.timedelta(minutes=1))
    assert prep.merits == 0

    prep.update(merits=7777, updated_at=prep.updated_at + datetime.timedelta(minutes=1))
    assert prep.merits == 7777


def test_ocrprep_validate_merits(f_ocr_testbed):
    prep = f_ocr_testbed[2][0]

    with pytest.raises(cog.exc.ValidationFail):
        prep.merits = -1
    with pytest.raises(cog.exc.ValidationFail):
        prep.merits = OCR_TOO_HIGH


def test_ocrprep_validate_updated_at(f_ocr_testbed):
    prep = f_ocr_testbed[2][0]

    with pytest.raises(cog.exc.ValidationFail):
        prep.updated_at = 0
    with pytest.raises(cog.exc.ValidationFail):
        prep.updated_at = datetime.datetime(1000, 1, 1)


def test_global__repr__(f_global_testbed):
    globe = f_global_testbed[0]

    expect = "Global(id=1, cycle=240, consolidation=77)"
    assert repr(globe) == expect


def test_global__str__(f_global_testbed):
    globe = f_global_testbed[0]

    expect = "Cycle 240: Consolidation Vote: 77%"
    assert str(globe) == expect


def test_vote__repr__(f_vote_testbed):
    vote = f_vote_testbed[0]

    expect = 'Vote(id=1, vote=<EVoteType.cons: 1>, amount=1, updated_at=datetime.datetime(2021, 8, 25, 2, 33))'
    assert repr(vote) == expect


def test_vote__str__(f_dusers, f_vote_testbed):
    vote = f_vote_testbed[0]

    expect = "**User1**: voted 1 Cons."
    assert str(vote) == expect.format(vote.updated_at)


def test_vote_update_amount(f_vote_testbed):
    vote = f_vote_testbed[0]
    vote.update_amount(5)
    assert vote.amount == 6


def test_global_update(f_global_testbed):
    globe = f_global_testbed[0]

    with pytest.raises(cog.exc.ValidationFail):
        globe.update(consolidation=9999)

    globe.update(cycle=100, consolidation=80, updated_at=globe.updated_at + datetime.timedelta(minutes=1))
    assert globe.cycle == 100
    assert globe.consolidation == 80


def test_global_validate_cycle(f_global_testbed):
    globe = f_global_testbed[0]

    with pytest.raises(cog.exc.ValidationFail):
        globe.cycle = -1


def test_global_validate_consolidation(f_global_testbed):
    globe = f_global_testbed[0]

    with pytest.raises(cog.exc.ValidationFail):
        globe.consolidation = -1
    with pytest.raises(cog.exc.ValidationFail):
        globe.consolidation = 111


def test_global_validate_updated_at(f_global_testbed):
    globe = f_global_testbed[0]

    with pytest.raises(cog.exc.ValidationFail):
        globe.updated_at = 0
    with pytest.raises(cog.exc.ValidationFail):
        globe.updated_at = datetime.datetime(1000, 1, 1)


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
        'sheet_src': EUMSheet.main,
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
