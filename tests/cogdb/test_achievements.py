"""
Tests for cogdb.achievements
"""
import cogdb.achievements
from cogdb.achievements import Achievement, AchievementType


def test_achievement__repr__(f_achievements, eddb_session):
    expect = 'Achievement(id=1, discord_id=1, achievement_type_id=1, created_at=1671655377)'
    assert expect == str(eddb_session.query(Achievement).first())


def test_achievementtype__repr__(f_achievements, eddb_session):
    expect = "AchievementType(id=1, name='FirstKill', description='First confirmed kill reported.', created_at=1671655377)"
    assert expect == str(eddb_session.query(AchievementType).first())
