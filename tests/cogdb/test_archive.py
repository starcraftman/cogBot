"""
Tests for cogdb.archive
"""
import pytest

import cogdb
from cogdb.schema import (
    DiscordUser, EFortType, EUMSheet,
)
from cogdb.archive import (
    AFortSystem, AFortDrop, AFortUser, AFortPrep,
    AUMSystem, AUMExpand, AUMOppose, AUMUser, AUMHold,
)

DB_CLASSES = [DiscordUser, AFortUser, AFortSystem, AFortDrop, AUMUser, AUMSystem, AUMHold]
CYCLE = 300


@pytest.fixture
def f_archive_testbed(session):
    """
    Fixture to insert some test SheetRows.

    Returns: (users, systems, drops)
    """
    dusers = session.query(DiscordUser).all()
    assert dusers

    users = (
        AFortUser(id=dusers[0].id, cycle=CYCLE, name=dusers[0].pref_name, row=15, cry='User1 are forting late!'),
        AFortUser(id=dusers[1].id, cycle=CYCLE, name=dusers[1].pref_name, row=16, cry=''),
        AFortUser(id=dusers[2].id, cycle=CYCLE, name=dusers[2].pref_name, row=17, cry='User3 is the boss'),
    )
    systems = (
        AFortSystem(id=1, cycle=CYCLE, name='Frey', fort_status=4910, trigger=4910, fort_override=0.7, um_status=0, undermine=0.0, distance=116.99, notes='', sheet_col='G', sheet_order=1),
        AFortSystem(id=2, cycle=CYCLE, name='Nurundere', fort_status=5422, trigger=8425, fort_override=0.6, um_status=0, undermine=0.0, distance=99.51, notes='', sheet_col='H', sheet_order=2),
        AFortSystem(id=3, cycle=CYCLE, name='LHS 3749', fort_status=1850, trigger=5974, um_status=0, undermine=0.0, distance=55.72, notes='', sheet_col='I', sheet_order=3),
        AFortSystem(id=4, cycle=CYCLE, name='Sol', fort_status=2500, trigger=5211, um_status=2250, undermine=0.0, distance=28.94, notes='Leave For Grinders', sheet_col='J', sheet_order=4),
        AFortSystem(id=5, cycle=CYCLE, name='Dongkum', fort_status=7000, trigger=7239, um_status=0, undermine=0.0, distance=81.54, notes='', sheet_col='K', sheet_order=5),
        AFortSystem(id=6, cycle=CYCLE, name='Alpha Fornacis', fort_status=0, trigger=6476, um_status=0, undermine=0.0, distance=67.27, notes='', sheet_col='L', sheet_order=6),
        AFortSystem(id=7, cycle=CYCLE, name='Phra Mool', fort_status=0, trigger=7968, um_status=0, undermine=0.0, distance=93.02, notes='Skip it now', sheet_col='M', sheet_order=7),
        AFortSystem(id=8, cycle=CYCLE, name='Othime', fort_status=0, trigger=7367, um_status=0, undermine=0.0, distance=83.68, notes='Priority for S/M ships (no L pads)', sheet_col='AF', sheet_order=26),
        AFortSystem(id=9, cycle=CYCLE, name='WW Piscis Austrini', fort_status=0, trigger=8563, um_status=0, undermine=1.2, distance=101.38, notes='', sheet_col='BK', sheet_order=57),
        AFortSystem(id=10, cycle=CYCLE, name='LPM 229', fort_status=0, trigger=9479, um_status=0, undermine=1.0, distance=112.98, notes='', sheet_col='BL', sheet_order=58),
        AFortPrep(id=1000, cycle=CYCLE, name='Rhea', trigger=10000, fort_status=5100, um_status=0, undermine=0.0, distance=65.55, notes='Atropos', sheet_col='D', sheet_order=0),
        AFortPrep(id=1001, cycle=CYCLE, name='PrepDone', trigger=10000, fort_status=12500, um_status=0, undermine=0.0, distance=65.55, notes='Atropos', sheet_col='E', sheet_order=0),
    )
    drops = (
        AFortDrop(id=1, cycle=CYCLE, amount=700, user_id=users[0].id, system_id=systems[0].id),
        AFortDrop(id=2, cycle=CYCLE, amount=400, user_id=users[0].id, system_id=systems[1].id),
        AFortDrop(id=3, cycle=CYCLE, amount=1200, user_id=users[1].id, system_id=systems[0].id),
        AFortDrop(id=4, cycle=CYCLE, amount=1800, user_id=users[2].id, system_id=systems[0].id),
        AFortDrop(id=5, cycle=CYCLE, amount=800, user_id=users[1].id, system_id=systems[1].id),
    )
    session.add_all(users + systems)
    session.flush()
    session.add_all(drops)
    session.commit()

    users = (
        AUMUser(id=dusers[0].id, cycle=CYCLE, name=dusers[0].pref_name, row=18, cry='We go pew pew!'),
        AUMUser(id=dusers[1].id, cycle=CYCLE, name=dusers[1].pref_name, row=19, cry='Shooting time'),
        AUMUser(id=dusers[2].id, cycle=CYCLE, name=dusers[2].pref_name, sheet_src=EUMSheet.snipe, row=18, cry='Sniping away'),
    )
    systems = (
        AUMSystem(id=1, name='Cemplangpa', sheet_col='D', goal=14878, security='Medium', notes='',
                  progress_us=15000, progress_them=1.0, close_control='Sol', priority='Medium',
                  map_offset=1380, cycle=CYCLE),
        AUMSystem(id=2, name='Pequen', sheet_col='F', goal=12500, security='Anarchy', notes='',
                  progress_us=10500, progress_them=0.5, close_control='Atropos', priority='Low',
                  map_offset=0, cycle=CYCLE),
        AUMExpand(id=3, name='Burr', sheet_col='H', goal=364298, security='Low', notes='',
                  progress_us=161630, progress_them=35.0, close_control='Dongkum', priority='Medium',
                  map_offset=76548, cycle=CYCLE),
        AUMOppose(id=4, name='AF Leopris', sheet_col='J', goal=59877, security='Low', notes='',
                  progress_us=47739, progress_them=1.69, close_control='Atropos', priority='low',
                  map_offset=23960, cycle=CYCLE),
        AUMSystem(id=5, name='Empty', sheet_col='K', goal=10000, security='Medium', notes='',
                  progress_us=0, progress_them=0.0, close_control='Rana', priority='Low',
                  map_offset=0, cycle=CYCLE),
        AUMSystem(id=6, name='LeaveIt', sheet_col='L', goal=10000, security='Medium', notes='',
                  progress_us=9000, progress_them=0.0, close_control='Rana', priority='Leave For Now',
                  map_offset=0, sheet_src=EUMSheet.main, cycle=CYCLE),
        AUMSystem(id=10007, name='ToSnipe', sheet_col='D', goal=100000, security='Medium', notes='',
                  progress_us=0, progress_them=0.0, close_control='Rana', priority='Low',
                  map_offset=0, sheet_src=EUMSheet.snipe, cycle=CYCLE),
    )
    holds = (
        AUMHold(id=1, cycle=CYCLE, held=0, redeemed=4000, user_id=dusers[0].id, system_id=systems[0].id),
        AUMHold(id=2, cycle=CYCLE, held=400, redeemed=1550, user_id=dusers[0].id, system_id=systems[1].id),
        AUMHold(id=3, cycle=CYCLE, held=2200, redeemed=5800, user_id=dusers[0].id, system_id=systems[2].id),
        AUMHold(id=4, cycle=CYCLE, held=450, redeemed=2000, user_id=dusers[1].id, system_id=systems[0].id),
        AUMHold(id=5, cycle=CYCLE, held=2400, redeemed=0, user_id=dusers[1].id, system_id=systems[1].id),
        AUMHold(id=6, cycle=CYCLE, held=0, redeemed=1200, user_id=dusers[1].id, system_id=systems[2].id),
        AUMHold(id=7, cycle=CYCLE, sheet_src=EUMSheet.snipe, held=5000, redeemed=1200, user_id=dusers[2].id, system_id=systems[-1].id),
    )
    session.add_all(users + systems)
    session.flush()
    session.add_all(holds)
    session.commit()

    yield

    session.rollback()
    for cls in (AFortDrop, AFortSystem, AFortUser, AUMHold, AUMSystem, AUMUser):
        session.query(cls).delete()
    session.commit()


def test_empty_tables_all(session, f_dusers, f_archive_testbed):
    for cls in DB_CLASSES:
        assert session.query(cls).all()

    cogdb.archive.empty_tables(session, perm=True)
    session.commit()

    for cls in DB_CLASSES:
        assert session.query(cls).all() == []


def test_empty_tables_not_all(session, f_dusers, f_archive_testbed):
    for cls in DB_CLASSES:
        assert session.query(cls).all()

    cogdb.archive.empty_tables(session, perm=False)

    classes = DB_CLASSES[:]
    classes.remove(DiscordUser)
    for cls in classes:
        assert session.query(cls).all() == []
    assert session.query(DiscordUser).all()


def test_fortuser__eq__(f_dusers, f_archive_testbed, session):
    fuser = session.query(AFortUser).filter(AFortUser.id == 1).one()
    equal = AFortUser(id=1, cycle=CYCLE, name='User1', row=22, cry='')
    assert fuser == equal

    equal.cycle = 200
    assert fuser != equal
    equal.cycle = CYCLE

    equal.name = 'notUser1'
    assert fuser != equal


def test_afortuser__repr__(f_dusers, f_archive_testbed, session):
    fuser = session.query(AFortUser).filter(AFortUser.id == 1).one()
    assert repr(fuser) == f"AFortUser(id=1, cycle={CYCLE}, name='User1', row=15, cry='User1 are forting late!')"


def test_fortsystem__eq__(f_dusers, f_archive_testbed, session):
    system = session.query(AFortSystem).filter(AFortSystem.id == 1).one()

    assert system == AFortSystem(cycle=CYCLE, name='Frey')
    assert system != AFortSystem(cycle=CYCLE, name='Sol')
    assert system != AFortSystem(cycle=1, name='Frey')


def test_fortsystem__repr__(f_dusers, f_archive_testbed, session):
    system = session.query(AFortSystem).filter(AFortSystem.id == 1).one()

    expect = f"AFortSystem(id=1, cycle={CYCLE}, name='Frey', fort_status=4910, "\
        "trigger=4910, fort_override=0.7, um_status=0, undermine=0.0, distance=116.99, "\
        "notes='', sheet_col='G', sheet_order=1)"
    assert repr(system) == expect


def test_drop__eq__(f_dusers, f_archive_testbed, session):
    user = session.query(AFortUser).filter(AFortUser.id == 1).one()
    system = session.query(AFortSystem).filter(AFortSystem.id == 1).one()
    drop = session.query(AFortDrop).filter(AFortDrop.id == 1).one()
    assert drop == AFortDrop(amount=700, cycle=CYCLE, user_id=user.id, system_id=system.id)


def test_drop__repr__(f_dusers, f_archive_testbed, session):
    user = session.query(AFortUser).filter(AFortUser.id == 1).one()
    system = session.query(AFortSystem).filter(AFortSystem.id == 1).one()
    drop = session.query(AFortDrop).filter(AFortDrop.id == 1).one()
    assert repr(drop) == f"AFortDrop(id=1, system_id={system.id}, user_id={user.id}, cycle={CYCLE}, amount=700)"
    assert drop == eval(repr(drop))


def test_umuser__eq__(f_dusers, f_archive_testbed, session):
    umuser = session.query(AUMUser).filter(AUMUser.id == 1).one()
    equal = AUMUser(id=1, cycle=CYCLE, sheet_src=EUMSheet.main, name='User1', row=22, cry='')
    assert umuser == equal
    equal.name = 'notUser1'
    assert umuser != equal


def test_umuser__repr__(f_dusers, f_archive_testbed, session):
    umuser = session.query(AUMUser).filter(AUMUser.id == 1).one()
    assert repr(umuser) == f"AUMUser(id=1, sheet_src=EUMSheet.main, cycle={CYCLE}, name='User1', row=18, cry='We go pew pew!')"


def test_umsystem__eq__(f_dusers, f_archive_testbed, session):
    system = session.query(AUMSystem).filter(AUMSystem.id == 1).one()
    equal = AUMSystem(id=1, cycle=CYCLE, sheet_src=EUMSheet.main, name='Cemplangpa')
    assert system == equal

    equal.name = 'notUser1'
    assert system != equal
    equal.name = 'Cemplangpa'

    equal.cycle = 1
    assert system != equal


def test_umsystem__repr__(f_dusers, f_archive_testbed, session):
    system = session.query(AUMSystem).filter(AUMSystem.id == 1).one()

    assert repr(system) == f"AUMSystem(id=1, sheet_src=EUMSheet.main, cycle={CYCLE}, name='Cemplangpa', sheet_col='D', "\
                           "goal=14878, security='Medium', notes='', "\
                           "progress_us=15000, progress_them=1.0, "\
                           "close_control='Sol', priority='Medium', map_offset=1380)"
    assert system == eval(repr(system))


def test_hold__eq__(f_dusers, f_archive_testbed, session):
    user = session.query(AUMUser).filter(AUMUser.id == 1).one()
    system = session.query(AUMSystem).filter(AUMSystem.id == 1).one()
    hold = session.query(AUMHold).filter(AUMHold.id == 1).one()

    assert hold == AUMHold(held=0, sheet_src=EUMSheet.main, cycle=CYCLE, redeemed=4000, user_id=user.id, system_id=system.id)


def test_hold__repr__(f_dusers, f_archive_testbed, session):
    user = session.query(AUMUser).filter(AUMUser.id == 1).one()
    system = session.query(AUMSystem).filter(AUMSystem.id == 1).one()
    hold = session.query(AUMHold).filter(AUMHold.id == 1).one()
    assert repr(hold) == f"AUMHold(id=1, sheet_src=EUMSheet.main, system_id={system.id}, user_id={user.id}, cycle={CYCLE}, held=0, redeemed=4000)"
    assert hold == eval(repr(hold))
