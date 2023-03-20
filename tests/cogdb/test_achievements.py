"""
Tests for cogdb.achievements
"""
import time

import pytest

import cogdb.achievements
from cogdb.achievements import Achievement, AchievementType
import pvp.schema


def test_achievement__repr__(f_achievements, eddb_session):
    expect = 'Achievement(id=1, discord_id=1, achievement_type_id=1, created_at=1671655377)'
    assert expect == str(eddb_session.query(Achievement).first())


def test_achievementtype__repr__(f_achievements, eddb_session):
    expect = "AchievementType(id=1, role_name='FirstKill', role_colour='b20000', role_description='First confirmed kill reported.', check_func='hello', check_kwargs='{}', created_at=1671655377)"
    assert expect == str(eddb_session.query(AchievementType).first())


def test_achievementtype_check(f_achievements, eddb_session):
    found = eddb_session.query(AchievementType).filter(AchievementType.id == 2).one()
    kwargs = {
        'stats': {
            'kills': 12,
        }
    }
    assert found.check(**kwargs)


def test_achievementtype_hex_value(f_achievements, eddb_session):
    assert 11665408 == eddb_session.query(AchievementType).first().hex_value


def test_remove_achievement_type(f_achievements, eddb_session):
    assert eddb_session.query(AchievementType).filter(AchievementType.role_name == 'EvenDozen').all()
    cogdb.achievements.remove_achievement_type(eddb_session, role_name='EvenDozen')
    assert not eddb_session.query(AchievementType).filter(AchievementType.role_name == 'EvenDozen').all()
    assert not eddb_session.query(Achievement).filter(Achievement.achievement_type_id == 2).all()


def test_add_achievement_type(f_achievements, eddb_session):
    kwargs = {
        'role_name': 'NewRole',
        'role_colour': 'FFFFFF',
        'role_description': 'A new role to apply.',
        'check_func': 'check_new_role',
        'check_kwargs': '{}',
    }
    cogdb.achievements.update_achievement_type(eddb_session, **kwargs)
    eddb_session.commit()

    last = eddb_session.query(AchievementType).order_by(AchievementType.id.desc()).first()
    assert last.role_name == 'NewRole'


def test_update_achievement_type(f_achievements, eddb_session):
    kwargs = {
        'role_name': 'EvenDozen',
        'new_role_name': 'NotEvenDozen',
        'role_colour': '222222',
        'role_description': 'A lazily made role.',
        'check_func': 'check_old_role',
        'check_kwargs': "{'test': 5}",
    }
    cogdb.achievements.update_achievement_type(eddb_session, **kwargs)
    eddb_session.commit()

    first = eddb_session.query(AchievementType).filter(AchievementType.id == 2).first()
    assert first.role_name == 'NotEvenDozen'
    assert first.check_func == 'check_old_role'


def test_update_achievement_type_collision(f_achievements, eddb_session):
    kwargs = {
        'role_name': 'EvenDozen',
        'new_role_name': 'FirstKill',
        'role_colour': '222222',
        'role_description': 'A lazily made role.',
        'check_func': 'check_old_role',
        'check_kwargs': "{'test': 5}",
    }
    with pytest.raises(ValueError):
        cogdb.achievements.update_achievement_type(eddb_session, **kwargs)


def test_update_achievement_type_missing(f_achievements, eddb_session):
    kwargs = {
        'role_name': 'NewRole',
        'check_func': 'check_new_role',
        'check_kwargs': "{}",
    }
    with pytest.raises(ValueError):
        cogdb.achievements.update_achievement_type(eddb_session, **kwargs)


def test_check_pvpstat_greater(f_pvp_testbed, eddb_session):
    kwargs = {
        'stat_name': 'kills',
        'amount': 12,
        'stats': {
            'kills': 12,
        }
    }
    assert cogdb.achievements.check_pvpstat_greater(**kwargs)

    kwargs = {
        'stat_name': 'kills',
        'amount': 12,
        'stats': {
            'kills': 2,
        }
    }
    assert not cogdb.achievements.check_pvpstat_greater(**kwargs)


def test_check_pvpcmdr_in_events(f_pvp_testbed, eddb_session):
    kwargs = {
        'discord_id': 1,
        'eddb_session': eddb_session,
        'pvp_event': 'PVPKill',
        'target_cmdr': "LeSuck",
    }
    assert cogdb.achievements.check_pvpcmdr_in_events(**kwargs)


def test_check_pvpcmdr_age(f_pvp_testbed, eddb_session):
    kwargs = {
        'discord_id': 1,
        'eddb_session': eddb_session,
        'required_days': 10,
    }
    assert cogdb.achievements.check_pvpcmdr_age(**kwargs)

    cmdr = eddb_session.query(pvp.schema.PVPCmdr).\
        filter(pvp.schema.PVPCmdr.id == 1).\
        one()
    cmdr.created_at = time.time() - 3600 * 24 * 5

    kwargs = {
        'discord_id': 1,
        'eddb_session': eddb_session,
        'required_days': 10,
    }
    assert not cogdb.achievements.check_pvpcmdr_age(**kwargs)


def test_check_duser_forts(f_dusers, f_fort_testbed, session):
    kwargs = {
        'discord_id': 1,
        'session': session,
        'amount': 1100,
    }
    assert cogdb.achievements.check_duser_forts(**kwargs)

    kwargs = {
        'discord_id': 1,
        'session': session,
        'amount': 5000,
    }
    assert not cogdb.achievements.check_duser_forts(**kwargs)


def test_check_duser_um(f_dusers, f_um_testbed, session):
    kwargs = {
        'discord_id': 1,
        'session': session,
        'amount': 12000,
    }
    assert cogdb.achievements.check_duser_um(**kwargs)

    kwargs = {
        'discord_id': 1,
        'session': session,
        'amount': 15000,
    }
    assert not cogdb.achievements.check_duser_um(**kwargs)


def test_check_duser_snipe(f_dusers, f_um_testbed, session):
    kwargs = {
        'discord_id': 3,
        'session': session,
        'amount': 2000,
    }
    assert cogdb.achievements.check_duser_snipe(**kwargs)

    kwargs = {
        'discord_id': 3,
        'session': session,
        'amount': 8000,
    }
    assert not cogdb.achievements.check_duser_snipe(**kwargs)
