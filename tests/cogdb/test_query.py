"""
Test cogdb.query module.
"""
from __future__ import absolute_import, print_function
import json

import pytest

import cog.exc
import cogdb.query


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
