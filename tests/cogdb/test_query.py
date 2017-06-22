"""
Test cogdb.query module.
"""
from __future__ import absolute_import, print_function
import json

import mock
import pytest

import cog.exc
from tests.cogdb import CELLS, FMT_CELLS, SYSTEMS, USERS
from tests.cogdb.test_schema import db_cleanup, db_data
import cogdb
import cogdb.schema
import cogdb.query


def test_subseq_match():
    assert cogdb.query.subseq_match('alx', 'alex')

    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.subseq_match('not', 'alex')

    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.subseq_match('longneedle', 'alex')

    assert cogdb.query.subseq_match('ALEr', 'Alexander')

    with pytest.raises(cog.exc.NoMatch):
        assert cogdb.query.subseq_match('ALEr', 'Alexander', ignore_case=False)


def test_fuzzy_find():
    assert cogdb.query.fuzzy_find('Alex', USERS) == 'Alexander Astropath'
    with pytest.raises(cog.exc.MoreThanOneMatch):
        assert cogdb.query.fuzzy_find('Aa', USERS)
    with pytest.raises(cog.exc.NoMatch):
        assert cogdb.query.fuzzy_find('zzzz', SYSTEMS)

    assert cogdb.query.fuzzy_find('WW p', SYSTEMS) == 'WW Piscis Austrini'
    with pytest.raises(cog.exc.MoreThanOneMatch):
        assert cogdb.query.fuzzy_find('tu', SYSTEMS) == 'Tun'
    assert cogdb.query.fuzzy_find('tun', SYSTEMS) == 'Tun'


def test_first_system_column():
    col_start = cogdb.query.first_system_column(json.loads(FMT_CELLS))
    assert col_start == 'F'

    cells = json.loads(FMT_CELLS)
    cells['sheets'][0]['data'][0]['rowData'][0]['values'] = []
    with pytest.raises(cog.exc.SheetParsingError):
        assert cogdb.query.first_system_column(cells)


def test_first_user_row():
    cells = [
        ['', 'First column!'],
        ['', 'High', 342033, 243333, 13200, 'UPDATE>>>',
         'UPDATE>>>', 189, 'TRUE', 'CMDR Name', 'Some user'],
        ['', 'Third column, too faar.'],
        ['', 'Fourth column ...'],
        ['', 'Cinco'],
    ]
    assert cogdb.query.first_user_row(cells) == ('B', 11)

    miss_col = cells[:1] + cells[2:]
    with pytest.raises(cog.exc.SheetParsingError):
        assert cogdb.query.first_user_row(miss_col)


def test_sheetscanner_forts():
    u_col, u_row = cogdb.query.first_user_row(CELLS)
    scanner = cogdb.query.SheetScanner(CELLS, 'F', u_col, u_row)
    systems = scanner.systems()
    users = scanner.users()

    # Manually set since not in db.
    for ind, sys in enumerate(systems):
        sys.id = ind + 1
    for ind, usr in enumerate(users):
        usr.id = ind + 1

    forts = scanner.forts(systems, users)
    fort1, fort2 = forts[0], forts[1]

    assert fort1.amount == 2222
    assert fort1.system_id == 1
    assert fort1.user_id == 2

    assert fort2.amount == 80
    assert fort2.system_id == 1
    assert fort2.user_id == 4


def test_sheetscanner_systems():
    u_col, u_row = cogdb.query.first_user_row(CELLS)
    scanner = cogdb.query.SheetScanner(CELLS, 'F', u_col, u_row)
    result = [sys.name for sys in scanner.systems()]
    assert result == SYSTEMS[:6] + ['Othime']


def test_sheetscanner_users():
    u_col, u_row = cogdb.query.first_user_row(CELLS)
    scanner = cogdb.query.SheetScanner(CELLS, 'F', u_col, u_row)
    result = [user.sheet_name for user in scanner.users()]
    assert result == USERS


@db_cleanup
@db_data
def test_get_othime():
    session = cogdb.Session()
    assert cogdb.query.get_othime(session).name == 'Othime'


@db_cleanup
@db_data
def test_get_all_systems():
    session = cogdb.Session()
    systems = cogdb.query.get_all_systems(session)
    assert len(systems) == 7
    assert systems[0].name == 'Frey'
    assert systems[-1].name == 'Othime'


@db_cleanup
@db_data
def test_get_systems_not_othime():
    session = cogdb.Session()
    systems = cogdb.query.get_systems_not_othime(session)
    assert len(systems) == 6
    assert systems[0].name == 'Frey'
    assert systems[-1].name == 'Alpha Fornacis'


@db_cleanup
@db_data
def test_get_all_users():
    session = cogdb.Session()
    users = cogdb.query.get_all_users(session)
    assert len(users) == 15
    assert users[0].sheet_name == 'Alexander Astropath'


@db_cleanup
@db_data
def test_find_current_target():
    session = cogdb.Session()
    assert cogdb.query.find_current_target(session) == 1


@db_cleanup
@db_data
def test_get_fort_targets():
    session = cogdb.Session()
    targets = cogdb.query.get_fort_targets(session, cogdb.query.find_current_target(session))
    assert [sys.name for sys in targets] == ['Nurundere', 'Othime']


@db_cleanup
@db_data
def test_get_next_fort_targets():
    session = cogdb.Session()
    targets = cogdb.query.get_next_fort_targets(session, cogdb.query.find_current_target(session))
    assert [sys.name for sys in targets] == ["LHS 3749", "Dongkum", "Alpha Fornacis"]

    targets = cogdb.query.get_next_fort_targets(session, cogdb.query.find_current_target(session),
                                                count=2)
    assert [sys.name for sys in targets] == ["LHS 3749", "Dongkum"]


@db_cleanup
@db_data
def test_get_all_systems_by_state():
    session = cogdb.Session()
    systems = cogdb.query.get_all_systems(session)
    systems[1].fort_status = 8425
    systems[1].undermine = 1.0
    systems[4].undermine = 1.9
    session.commit()

    # print(systems[1].is_fortified, systems[1].is_undermined, systems[2].is_undermined())
    systems = cogdb.query.get_all_systems_by_state(session)
    assert [sys.name for sys in systems['cancelled']] == ["Nurundere"]
    assert [sys.name for sys in systems['fortified']] == ["Frey"]
    assert [sys.name for sys in systems['left']] == ["LHS 3749", "Sol", "Alpha Fornacis", "Othime"]
    assert [sys.name for sys in systems['undermined']] == ["Dongkum"]
    assert [sys.name for sys in systems['skipped']] == ['Sol']


@db_cleanup
@db_data
def test_get_system_by_name():
    session = cogdb.Session()
    sys = cogdb.query.get_system_by_name(session, 'Sol')
    assert isinstance(sys, cogdb.schema.System)
    assert sys.name == 'Sol'

    sys = cogdb.query.get_system_by_name(session, 'alp')
    assert sys.name == 'Alpha Fornacis'


@db_cleanup
@db_data
def test_get_discord_user_by_id():
    session = cogdb.Session()
    duser = cogdb.query.get_discord_user_by_id(session, '1111')
    assert isinstance(duser, cogdb.schema.DUser)
    assert duser.display_name == 'GearsandCogs'

    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.get_discord_user_by_id(session, '0')


@db_cleanup
@db_data
def test_get_sheet_user_by_name():
    session = cogdb.Session()
    user = cogdb.query.get_sheet_user_by_name(session, 'Shepron')
    assert isinstance(user, cogdb.schema.SUser)
    assert user.sheet_name == 'Shepron'

    user = cogdb.query.get_sheet_user_by_name(session, 'gear')
    assert user.sheet_name == 'GearsandCogs'


@db_cleanup
@db_data
def test_get_or_create_sheet_user_no_create():
    member = mock.Mock()
    member.sheet_name = 'GearsandCogs'

    session = cogdb.Session()
    suser = cogdb.query.get_or_create_sheet_user(session, member)
    assert suser.sheet_row == 22


@db_cleanup
@db_data
@mock.patch('cog.sheets.callback_add_user')
def test_get_or_create_sheet_user_create(mock_callback):
    member = mock.Mock()
    member.sheet_name = 'notGears'

    session = cogdb.Session()
    suser = cogdb.query.get_or_create_sheet_user(session, member)
    assert suser.sheet_name == 'notGears'
    assert suser.sheet_row == session.query(cogdb.schema.SUser).all()[-2].sheet_row + 1
    assert mock_callback.called


@db_cleanup
@db_data
def test_get_or_create_duser_no_create():
    member = mock.Mock()
    member.id = '1111'
    duser = cogdb.query.get_or_create_duser(member)
    assert duser.display_name == 'GearsandCogs'


@db_cleanup
@db_data
def test_get_or_create_duser_create():
    member = mock.Mock()
    member.id = '1112'
    member.display_name = 'someuser'
    duser = cogdb.query.get_or_create_duser(member)
    assert duser.display_name == member.display_name

    session = cogdb.Session()
    last_user = session.query(cogdb.schema.DUser).all()[-1]
    assert last_user.display_name == member.display_name


@db_cleanup
@db_data
def test_add_suser():
    session = cogdb.Session()
    mock_call = mock.Mock()
    user = cogdb.query.add_suser(session, mock_call.callback, 'NewestUser')
    assert cogdb.query.get_all_users(session)[-1].sheet_name == 'NewestUser'
    mock_call.callback.assert_called_with(user)


@db_cleanup
def test_add_duser():
    member = mock.Mock()
    member.id = '1111'
    member.display_name = 'NewestUser'
    session = cogdb.Session()
    duser = cogdb.query.add_duser(session, member, capacity=50)
    assert cogdb.query.get_discord_user_by_id(session, '1111') == duser


@db_cleanup
@db_data
def test_add_fort():
    import sqlalchemy.orm.exc
    session = cogdb.Session()
    system = cogdb.query.get_all_systems(session)[2]
    user = cogdb.query.get_all_users(session)[4]

    with pytest.raises(sqlalchemy.orm.exc.NoResultFound):
        session.query(cogdb.schema.Fort).filter_by(user_id=user.id, system_id=system.id).one()
    old_fort = system.fort_status
    old_cmdr = system.cmdr_merits
    mock_call = mock.Mock()

    fort = cogdb.query.add_fort(session, mock_call.callback, system=system, user=user, amount=400)

    mock_call.callback.assert_called_with(fort)
    assert fort.amount == 400
    assert system.fort_status == old_fort + 400
    assert system.cmdr_merits == old_cmdr + 400
    assert session.query(cogdb.schema.Fort).filter_by(user_id=user.id, system_id=system.id).one()
