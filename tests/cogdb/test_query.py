"""
Test cogdb.query module.
"""
from __future__ import absolute_import, print_function
import json

import mock
import pytest

import cog.exc
from tests.cogdb.test_schema import db_cleanup
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


def db_data(function):
    """
    Wrap a test and setup database with dummy data.
    """
    def call():
        session = cogdb.Session()

        user_col, user_row = cogdb.query.first_user_row(CELLS)
        scanner = cogdb.query.SheetScanner(CELLS, 'F', user_col, user_row)
        systems = scanner.systems()
        users = scanner.users()
        session.add_all(systems + users)
        session.commit()

        forts = scanner.forts(systems, users)
        session.add_all(forts)
        session.commit()

        function()
    return call


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


@db_cleanup
@db_data
def test_get_system_by_name():
    session = cogdb.Session()
    sys = cogdb.query.get_system_by_name(session, 'Sol')
    assert isinstance(sys, cogdb.schema.System)
    assert sys.name == 'Sol'


@db_cleanup
@db_data
def test_get_sheet_user_by_name():
    session = cogdb.Session()
    user = cogdb.query.get_sheet_user_by_name(session, 'Shepron')
    assert isinstance(user, cogdb.schema.User)
    assert user.sheet_name == 'Shepron'


@db_cleanup
@db_data
def test_add_user():
    session = cogdb.Session()
    mock_call = mock.Mock()
    user = cogdb.query.add_user(session, mock_call.callback, 'NewestUser')
    assert cogdb.query.get_all_users(session)[-1].sheet_name == 'NewestUser'
    mock_call.callback.assert_called_with(user)


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


# def get_all_systems_by_state(session):
    # """
    # Return a dictionary that lists the systems states below:

        # left:
        # fortified:
        # undermined:
        # cancelled:
    # """
    # states = {
        # 'cancelled': [],
        # 'fortified': [],
        # 'left': [],
        # 'undermined': [],
    # }

    # for system in get_all_systems(session):
        # if system.is_fortified and system.is_undermined:
            # states['cancelled'].append(system)
        # elif system.is_undermined:
            # states['undermined'].append(system)
        # elif system.is_fortified:
            # states['fortified'].append(system)
        # else:
            # states['left'].append(system)

    # return states


SYSTEMS = [
    "Frey", "Nurundere", "LHS 3749", "Sol", "Dongkum", "Alpha Fornacis",
    "Phra Mool", "LP 291-34", "Wat Yu", "Rana", "Adeo", "Mariyacoch",
    "LTT 15449", "Gliese 868", "Shoujeman", "Anlave", "Atropos", "16 Cygni",
    "Abi", "LHS 3447", "Lalande 39866", "Phanes", "NLTT 46621", "Othime",
    "Aornum", "Wolf 906", "LP 580-33", "BD+42 3917", "37 Xi Bootis", "Mulachi",
    "Wolf 25", "LHS 6427", "39 Serpentis", "Bhritzameno", "Gilgamesh",
    "Epsilon Scorpii", "Ross 33", "Kaushpoos", "LHS 142", "Venetic", "LHS 1541",
    "Parutis", "Wolf 867", "Vega", "Groombridge 1618", "Lushertha", "LHS 3885",
    "G 250-34", "Tun", "Lung", "LHS 3577", "LTT 15574", "GD 219", "LHS 1197",
    "WW Piscis Austrini", "LPM 229"
]
USERS = [
    "Alexander Astropath", "Toliman", "TiddyMun", "Oskiboy[XB1/PC]",
    "Winna09", "Shepron", "Grimbald", "Haphollas", "Gary Brain", "Ricshah",
    "Rico Char", "GearsandCogs", "NotRjwhite", "Rumrunner", "A Name With Spaces"
]
CELLS = [
    [
        "",
        "Fortification Priority:",
        "Total Fortification Triggers:",
        "Missing Fortification Merits:",
        "Total CMDR Merits (incl. prep)",
        "FORTIFICATION ORDER: ",
        "Fortify from the left to the right",
        "Battle Cattle CC Projection ",
        "Import Data:",
        "Your Battle Cattle Battle Cry",
        "FHS Gloria holding the line",
        "",
        "Beware the hollow square",
        "",
        "",
        "",
        "The Grim"
    ],
    [
        "",
        "High",
        351047,
        342324,
        13045,
        "UPDATE>>>",
        "UPDATE>>>",
        272.2999999999997,
        True,
        "CMDR Name",
        "Alexander Astropath",
        "Toliman",
        "TiddyMun",
        "Oskiboy[XB1/PC]",
        "Winna09",
        "Shepron",
        "Grimbald",
        "Haphollas",
        "Gary Brain",
        "Ricshah",
        "Rico Char",
        "GearsandCogs",
        "NotRjwhite",
        "Rumrunner",
        "A Name With Spaces"
    ],
    [
        "",
        "% Completion:",
        "Trigger:",
        "Missing:",
        "CMDR Merits:",
        " Fortification Status:",
        "Undermine Status:",
        "Distance from HQ:",
        "Notes:",
        "Merits",
        3800,
        2452,
        800,
        80,
        750,
        816,
        520,
        240,
        650,
        100,
        86,
        2100,
        0,
        401,
        250,
    ],
    [
        "",
        0,
        10000,
        10000,
        0,
        "",
        "",
        "-",
        "",
        "TBA"
    ],
    [
        "",
        0,
        10000,
        10000,
        0,
        "",
        "",
        "-",
        "",
        "TBA"
    ],
    [
        "",
        1,
        4910,
        0,
        4322,
        4910,
        0,
        116.99,
        "",
        "Frey",
        "",
        2222,
        "",
        80,
        750,
        750,
        520
    ],
    [
        "",
        1,
        8425,
        0,
        6371,
        4350,
        "",
        99.51,
        "",
        "Nurundere",
        3800,
        230,
        "",
        "",
        "",
        "",
        "",
        240,
        "",
        "",
        "",
        2100,
        "",
        1
    ],
    [
        "",
        1,
        5974,
        0,
        750,
        750,
        "",
        55.72,
        "",
        "LHS 3749",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        500,
        "",
        "",
        "",
        "",
        "",
        250
    ],
    [
        "",
        0.5657263481097679,
        5211,
        2263,
        400,
        400,
        "",
        28.94,
        "Leave For Grinders",
        "Sol",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        400
    ],
    [
        "",
        1,
        7239,
        0,
        0,
        0,
        "",
        81.54,
        "",
        "Dongkum"
    ],
    [
        "",
        1,
        6476,
        0,
        0,
        "",
        "",
        67.27,
        "",
        "Alpha Fornacis"
    ],
    [
        "",
        1,
        7367,
        0,
        1050,
        "1095",
        "",
        83.68,
        "",
        "Othime"
    ],
]
FMT_CELLS = """{
    "sheets": [
        {
            "data": [
                {
                    "rowData": [
                        {
                            "values": [
                                {
                                    "effectiveFormat": {
                                        "backgroundColor": {
                                            "blue": 0.047058824,
                                            "green": 0.1254902,
                                            "red": 0.52156866
                                        }
                                    },
                                    "effectiveValue": {
                                        "stringValue": "Your Battle Cattle Battle Cry"
                                    }
                                },
                                {
                                    "effectiveFormat": {
                                        "backgroundColor": {
                                            "blue": 0.047058824,
                                            "green": 0.1254902,
                                            "red": 0.52156866
                                        }
                                    },
                                    "effectiveValue": {
                                        "stringValue": "CMDR Name"
                                    }
                                },
                                {
                                    "effectiveFormat": {
                                        "backgroundColor": {
                                            "blue": 0.047058824,
                                            "green": 0.1254902,
                                            "red": 0.52156866
                                        }
                                    },
                                    "effectiveValue": {
                                        "stringValue": "Merits"
                                    }
                                },
                                {
                                    "effectiveFormat": {
                                        "backgroundColor": {
                                            "blue": 0.7647059,
                                            "green": 0.4862745,
                                            "red": 0.5568628
                                        }
                                    },
                                    "effectiveValue": {
                                        "stringValue": "TBA"
                                    }
                                },
                                {
                                    "effectiveFormat": {
                                        "backgroundColor": {
                                            "blue": 0.7647059,
                                            "green": 0.4862745,
                                            "red": 0.5568628
                                        }
                                    },
                                    "effectiveValue": {
                                        "stringValue": "TBA"
                                    }
                                },
                                {
                                    "effectiveFormat": {
                                        "backgroundColor": {
                                            "blue": 0.92156863,
                                            "green": 0.61960787,
                                            "red": 0.42745098
                                        }
                                    },
                                    "effectiveValue": {
                                        "stringValue": "Frey"
                                    }
                                }
                            ]
                        }
                    ],
                    "rowMetadata": [
                        {
                            "pixelSize": 21
                        }
                    ],
                    "startRow": 9
                }
            ]
        }
    ]
}
"""
