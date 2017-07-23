"""
Test cogdb.query module.
"""
from __future__ import absolute_import, print_function
import copy

import sqlalchemy.orm.exc
import mock
import pytest

import cog.exc
import cogdb
from cogdb.schema import (DUser, System, SheetRow, SheetCattle, SheetUM,
                          Hold, UMExpand, EFaction, ESheetType)
import cogdb.query

from tests.data import CELLS_FORT_FMT, SYSTEMS, USERS


def test_subseq_match():
    assert cogdb.query.subseq_match('alx', 'alex')

    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.subseq_match('not', 'alex')

    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.subseq_match('longneedle', 'alex')

    assert cogdb.query.subseq_match('ALEr', 'Alexander')

    with pytest.raises(cog.exc.NoMatch):
        cogdb.query.subseq_match('ALEr', 'Alexander', ignore_case=False)


def test_substr_match():
    assert cogdb.query.substr_match('ale', 'alex')

    assert not cogdb.query.substr_match('not', 'alex')

    assert not cogdb.query.substr_match('longneedle', 'alex')

    assert cogdb.query.substr_match('ALEX', 'Alexander')
    assert cogdb.query.substr_match('nde', 'Alexander')

    assert not cogdb.query.substr_match('ALe', 'Alexander', ignore_case=False)


def test_fuzzy_find():
    assert cogdb.query.fuzzy_find('Alex', USERS) == 'Alexander Astropath'
    print(SYSTEMS)
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


def test_add_sheet(session, f_dusers):
    test_name = 'Jack'
    test_cry = 'I do not cry'
    cogdb.query.add_sheet(session, test_name, cry=test_cry, faction=EFaction.winters,
                          type=ESheetType.um, start_row=5)
    latest = session.query(SheetRow).all()[-1]
    assert latest.name == test_name
    assert latest.cry == test_cry
    assert latest.row == 5
    assert latest.type == ESheetType.um

    test_name = 'John'
    test_cry = 'I cry'
    cogdb.query.add_sheet(session, test_name, cry=test_cry, faction=EFaction.winters,
                          type=ESheetType.um, start_row=5)
    latest = session.query(SheetRow).all()[-1]
    assert latest.name == test_name
    assert latest.cry == test_cry
    assert latest.row == 6
    assert latest.type == ESheetType.um


def test_fort_get_othime(session, f_systems):
    assert cogdb.query.fort_get_othime(session).name == 'Othime'


def test_fort_get_systems(session, f_systems):
    systems = cogdb.query.fort_get_systems(session)
    assert len(systems) == 7
    assert systems[0].name == 'Frey'
    assert systems[-1].name == 'Othime'


def test_fort_get_preps(session, f_prepsystem):
    systems = cogdb.query.fort_get_preps(session)
    assert [system.name for system in systems] == ['Muncheim']


def test_fort_get_systems_not_othime(session, f_systems):
    systems = cogdb.query.fort_get_systems(session, not_othime=True)
    assert len(systems) == 6
    assert systems[0].name == 'Frey'
    assert systems[-1].name == 'Alpha Fornacis'


def test_fort_get_systems_by_state(session, f_systems):
    systems = cogdb.query.fort_get_systems(session)
    systems[1].fort_status = 8425
    systems[1].undermine = 1.0
    systems[4].undermine = 1.9
    session.commit()

    systems = cogdb.query.fort_get_systems_by_state(session)
    assert [sys.name for sys in systems['cancelled']] == ["Nurundere"]
    assert [sys.name for sys in systems['fortified']] == ["Frey"]
    assert [sys.name for sys in systems['left']] == ["LHS 3749", "Sol", "Alpha Fornacis", "Othime"]
    assert [sys.name for sys in systems['undermined']] == ["Dongkum"]
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
    assert [sys.name for sys in targets] == ['Nurundere', 'Othime', 'Muncheim']


def test_fort_get_next_targets(session, f_systems):
    targets = cogdb.query.fort_get_next_targets(session)
    assert [sys.name for sys in targets] == ["LHS 3749"]

    targets = cogdb.query.fort_get_next_targets(session, count=2)
    assert [sys.name for sys in targets] == ["LHS 3749", "Dongkum"]


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


def test_sheetscanner_find_system_column(mock_sheet):
    scanner = cogdb.query.FortScanner(mock_sheet)
    assert scanner.system_col == 'F'

    mock_sheet.get_with_formatting.return_value = copy.deepcopy(CELLS_FORT_FMT)
    mock_sheet.get_with_formatting.return_value['sheets'][0]['data'][0]['rowData'][0]['values'] = []
    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.query.FortScanner(mock_sheet)


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
    scanner = cogdb.query.FortScanner(mock_sheet)
    assert (scanner.user_col, scanner.user_row) == ('B', 11)

    mock_sheet.whole_sheet.return_value = cells[:1] + cells[2:]
    with pytest.raises(cog.exc.SheetParsingError):
        cogdb.query.FortScanner(mock_sheet)


def test_sheetscanner_merits(mock_sheet, db_cleanup):
    session = cogdb.Session()
    scanner = cogdb.query.FortScanner(mock_sheet)
    scanner.scan(session)

    fort1 = session.query(cogdb.schema.Drop).all()[0]
    assert fort1.amount == 2222
    assert fort1.system.name == 'Frey'
    assert fort1.system_id == 1
    assert fort1.user.name == 'Toliman'
    assert fort1.user_id == 2


def test_sheetscanner_systems(mock_sheet):
    scanner = cogdb.query.FortScanner(mock_sheet)
    result = [sys.name for sys in scanner.systems()]
    assert result == SYSTEMS[:6] + ['Othime']


def test_sheetscanner_users(mock_sheet):
    scanner = cogdb.query.FortScanner(mock_sheet)
    result = [suser.name for suser in scanner.users(SheetCattle, EFaction.hudson)]
    assert result == USERS


def test_umscanner_systems(mock_umsheet):
    scanner = cogdb.query.UMScanner(mock_umsheet)
    system = scanner.systems()[0]
    assert system.name == 'Burr'
    assert isinstance(system, UMExpand)


def test_umscanner_users(mock_umsheet):
    scanner = cogdb.query.UMScanner(mock_umsheet)
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
    scanner = cogdb.query.UMScanner(mock_umsheet)
    scanner.scan(session)
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
    assert sheet.merits == 'Holding 2600, Redeemed 11350'
    cogdb.query.um_reset_held(session, sheet)
    assert sheet.merits == 'Holding 0, Redeemed 11350'


def test_um_redeem_merits(session, f_dusers, f_sheets, f_systemsum, f_holds):
    sheet = f_sheets[1]
    assert sheet.merits == 'Holding 2600, Redeemed 11350'
    cogdb.query.um_redeem_merits(session, sheet)
    assert sheet.merits == 'Holding 0, Redeemed 13950'


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
