"""
Test cogdb.query module.
"""
from __future__ import absolute_import, print_function
import copy

import mock
import pytest

import cog.exc
from tests.cogdb import FMT_CELLS, SYSTEMS, USERS
from tests.cogdb.test_schema import db_cleanup, db_data, mock_sheet, mock_um_sheet
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


def test_sheetscanner_find_system_column(mock_sheet):
    scanner = cogdb.query.SheetScanner(mock_sheet)
    assert scanner.system_col == 'F'

    mock_sheet.get_with_formatting.return_value = copy.deepcopy(FMT_CELLS)
    mock_sheet.get_with_formatting.return_value['sheets'][0]['data'][0]['rowData'][0]['values'] = []
    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.query.SheetScanner(mock_sheet)


def test_sheetscanner_find_user_row(mock_sheet):
    cells = [
        ['', 'First column!'],
        ['', 'High', 342033, 243333, 13200, 'UPDATE>>>',
         'UPDATE>>>', 189, 'TRUE', 'CMDR Name', 'Some user'],
        ['', 'Third column, too faar.'],
        ['', 'Fourth column ...'],
        ['', 'Cinco'],
    ]
    mock_sheet.whole_sheet.return_value = cells
    scanner = cogdb.query.SheetScanner(mock_sheet)
    assert (scanner.user_col, scanner.user_row) == ('B', 11)

    mock_sheet.whole_sheet.return_value = cells[:1] + cells[2:]
    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.query.SheetScanner(mock_sheet)


def test_sheetscanner_forts(mock_sheet):
    try:
        session = cogdb.Session()
        scanner = cogdb.query.SheetScanner(mock_sheet)
        scanner.scan(session)

        fort1 = session.query(cogdb.schema.Fort).all()[0]
        assert fort1.amount == 2222
        assert fort1.system.name == 'Frey'
        assert fort1.system_id == 1
        assert fort1.suser.name == 'Toliman'
        assert fort1.user_id == 2
    finally:
        cogdb.schema.drop_all_tables()


def test_sheetscanner_systems(mock_sheet):
    scanner = cogdb.query.SheetScanner(mock_sheet)
    result = [sys.name for sys in scanner.systems()]
    assert result == SYSTEMS[:6] + ['Othime']


def test_sheetscanner_users(mock_sheet):
    scanner = cogdb.query.SheetScanner(mock_sheet)
    result = [suser.name for suser in scanner.users()]
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
def test_get_all_sheet_users():
    session = cogdb.Session()
    users = cogdb.query.get_all_sheet_users(session)
    assert len(users) == 15
    assert users[0].name == 'Alexander Astropath'


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
    suser = cogdb.query.get_sheet_user_by_name(session, 'Shepron')
    assert isinstance(suser, cogdb.schema.SUser)
    assert suser.name == 'Shepron'

    suser = cogdb.query.get_sheet_user_by_name(session, 'gear')
    assert suser.name == 'GearsandCogs'


@db_cleanup
@db_data
def test_check_sheet_user_no_create():
    member = mock.Mock()
    member.pref_name = 'GearsandCogs'
    create_hook = mock.Mock()

    session = cogdb.Session()
    suser = cogdb.query.check_sheet_user(session, member, create_hook)
    assert suser.row == 22
    assert not create_hook.called


@db_cleanup
@db_data
def test_check_sheet_user_create():
    member = mock.Mock()
    member.pref_name = 'notGears'
    create_hook = mock.Mock()
    create_hook = mock.Mock()

    session = cogdb.Session()
    suser = cogdb.query.check_sheet_user(session, member, create_hook)
    assert suser.row == 26
    assert suser.name == 'notGears'
    assert suser.row == session.query(cogdb.schema.SUser).all()[-2].row + 1
    assert create_hook.called


@db_cleanup
@db_data
def test_check_discord_user_no_create():
    session = cogdb.Session()
    member = mock.Mock()
    member.id = '1111'
    duser = cogdb.query.check_discord_user(session, member)
    assert duser.display_name == 'GearsandCogs'


@db_cleanup
@db_data
def test_check_discord_user_create():
    session = cogdb.Session()
    member = mock.Mock()
    member.id = '1112'
    member.display_name = 'someuser'
    duser = cogdb.query.check_discord_user(session, member)
    assert duser.display_name == member.display_name

    last_user = session.query(cogdb.schema.DUser).all()[-1]
    assert last_user.display_name == member.display_name


@db_cleanup
@db_data
def test_add_sheet_user():
    session = cogdb.Session()
    cogdb.query.add_sheet_user(session, 'NewestUser')
    assert cogdb.query.get_all_sheet_users(session)[-1].name == 'NewestUser'


@db_cleanup
def test_add_discord_user():
    member = mock.Mock()
    member.id = '1111'
    member.display_name = 'NewestUser'
    session = cogdb.Session()
    duser = cogdb.query.add_discord_user(session, member, capacity=50)
    assert cogdb.query.get_discord_user_by_id(session, '1111') == duser


@db_cleanup
@db_data
def test_add_fort():
    import sqlalchemy.orm.exc
    session = cogdb.Session()
    system = cogdb.query.get_all_systems(session)[2]
    suser = cogdb.query.get_all_sheet_users(session)[4]

    with pytest.raises(sqlalchemy.orm.exc.NoResultFound):
        session.query(cogdb.schema.Fort).filter_by(user_id=suser.id,
                                                   system_id=system.id).one()
    old_fort = system.fort_status
    old_cmdr = system.cmdr_merits

    fort = cogdb.query.add_fort(session, system=system, suser=suser, amount=400)

    assert fort.amount == 400
    assert system.fort_status == old_fort + 400
    assert system.cmdr_merits == old_cmdr + 400
    assert session.query(cogdb.schema.Fort).filter_by(user_id=suser.id,
                                                      system_id=system.id).one()
