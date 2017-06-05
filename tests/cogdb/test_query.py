"""
Test cogdb.query module.
"""
from __future__ import absolute_import, print_function
import json

import pytest

import cog.exc
import cogdb.query


SYSTEMS = ["Frey", "Nurundere", "LHS 3749", "Sol", "Dongkum", "Alpha Fornacis",
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

USERS = ["Alexander Astropath", "Toliman", "TiddyMun", "Oskiboy[XB1/PC]",
         "Winna09", "Shepron", "Grimbald", "Haphollas", "Gary Brain", "Ricshah",
         "Rico Char", "GearsandCogs", "NotRjwhite", "Rumrunner", "A Name With Spaces"
        ]


class CouldNotMatchOne(Exception):
    pass


def fuzzy_find(needle, stack):
    """
    Simple fuzzy find based on needle.

    If matches more than one element in stack, raise CouldNotMatchOne
    """
    # FIXME: Very simple stub, change later to subsequence.
    new_stack = []
    for line in stack:
        if needle in line:
            new_stack.append(line)

    if len(new_stack) != 1:
        raise CouldNotMatchOne

    return new_stack[0]


def test_fuzzy_find():
    assert fuzzy_find('Alex', USERS) == 'Alexander Astropath'


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
