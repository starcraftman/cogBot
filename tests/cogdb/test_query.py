"""
Test cogdb.query module.
"""
from __future__ import absolute_import, print_function
import json

import pytest

import cog.exc
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
    assert result == SYSTEMS[:6]


def test_sheetscanner_users():
    u_col, u_row = cogdb.query.first_user_row(CELLS)
    scanner = cogdb.query.SheetScanner(CELLS, 'F', u_col, u_row)
    result = [user.sheet_name for user in scanner.users()]
    assert result == USERS


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
