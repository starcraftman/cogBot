"""
Store all test data here.
"""
import json

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

CELLS_FORT = [
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
CELLS_FORT_FMT = json.loads("""{
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
""")

CELLS_UM = [
    [
        "You can keep track of your merits on the sheet even if you haven't turned "
        'the merits in. This is to optimize our collective undermining. If you lose '
        'the merits, remove them so we know to go fix it!',
        '',
        'Cycle 108: The Battle for Burr',
        'Total',
        'wa',
        'Total',
        'GREEN MEANS DONE (>100%)',
        'RED MEANS OVERDONE (>110%)',
        'HOW TO SORT',
        'UPDATE THESE when turning in merits >',
        '',
        '',
        'Insert witty motto here',
        'Holding 414% in merits right now',
        '',
        'Democracy one bullet at a time',
        '',
        'FNS Eternal Wanderer reporting in',
        "I'm so dumb. so is the CMDR 3 rows down.",
        '',
        '',
        'Who, me?',
        '',
        '',
        '',
        'Killed by a white dwarf',
        '',
        650,
        '',
        'Federal Privateers',
        'Always have a highwake target locked',
        '',
        '',
        '',
        'd1/6530',
        '',
        'Ant Hill Mob(XB)',
        'Ant Hill Mob(XB)',
        'Ant Hill Mob(XB)',
        'Ant Hill Mob(XB)',
        'Ant Hill Mob(XB)',
        'Ant Hill Mob(XB)',
        '',
        'Make the Federation Great Again',
        '',
        '',
        '',
        '',
        '',
        '',
        'Federation corvette class cruiser reporting in'
    ],
    [
        '',
        '',
        '',
        365297.5,
        160472,
        128277.5,
        'SECURITY LEVEL & NOTES:',
        'CLOSEST HUDSON CTRL SYSTEM:',
        'Commanders',
        '',
        '',
        'How to put in merits >',
        100000,
        'Haphollas',
        'Rico Char',
        'MalvadoDiablo',
        'Harmsus',
        'Otorno',
        'Blackneto',
        'Paul Redpath',
        'Xxxreaper752xxx ',
        'FRENZY86',
        'Sardaukar17',
        'SpongeDoc',
        'ActionFace',
        'ilNibbio',
        'Tomis[XB1]',
        'UEG LONE',
        'tfcheps',
        'xxSNEAKELLAMAxx',
        'Alexander Astropath',
        'Rimos',
        'Shepron',
        'Willa',
        'North Man',
        'Tiddymun',
        'Horizon',
        'Phantom50Elite',
        'BaronGreenback',
        'Fod4u2',
        'Eastbourne',
        'KineticTrauma',
        'CyberCarnivore',
        'Renegade Bovine',
        'crazyjay',
        'harlequin_420th',
        'NascentChemist',
        'Oskiboy[PC/XB1]',
        'Muaddib',
        'DRAGON DARKO',
        'Gaz Cullen'
    ],
    [
        '',
        '',
        '% Completion:',
        'Trigger/Merit goal:',
        'CMDR Merits:',
        'Missing:',
        '',
        '',
        'CMDR Merits',
        'UM PROGRESS (GalMap)',
        'FORT/EXPAND % (GalMap)',
        '',
        'GalMap Offset',
        28750,
        2760,
        6540,
        5450,
        0,
        5790,
        3390,
        2850,
        5340,
        2740,
        3670,
        300,
        0,
        8220,
        2270,
        1010,
        520,
        5770,
        7440,
        3290,
        5250,
        7790,
        5170,
        8852,
        5670,
        3000,
        2500,
        30,
        0,
        0,
        4080,
        4710,
        1020,
        2750,
        3940,
        510,
        6080,
        3020,
        0,
        0,
        0
    ],
    [
        'Exp. trigger',
        6939,
        'behind by -84%',
        364297.5,
        160472,
        127277.5,
        'Sec: Low',
        'Dongkum',
        'Burr',
        161630,
        35,
        'Held merits',
        76548,
        28750,
        '',
        0,
        5450,
        '',
        470,
        '',
        '',
        5340,
        '',
        2190,
        '',
        '',
        7330,
        '',
        '',
        '',
        5770,
        '',
        3290,
        '',
        7790,
        '',
        '',
        960,
        '',
        '',
        30,
        '',
        '',
        4080,
        '',
        '',
        '',
        3940
    ],
    [
        '% safety margin',
        0.5,
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '<- Exp Progress',
        '<- Opp %',
        'Redeemed merits',
        '',
        '',
        2760,
        6540,
        '',
        '',
        5320,
        3390,
        2850,
        '',
        2740,
        1480,
        300,
        '',
        890,
        2270,
        1010,
        520,
        '',
        7440,
        '',
        5250,
        '',
        5170,
        8852,
        4710,
        3000,
        2500,
        '',
        '',
        '',
        '',
        4710,
        1020,
        2750,
        '',
        510,
        6080,
        3020
    ],
    [
        '',
        '',
        0,
        1000,
        0,
        1000,
        "#N/A (Did not find value 'Control System Template' in VLOOKUP evaluation.)",
        "#N/A (Did not find value 'Control System Template' in VLOOKUP evaluation.)",
        'Control System Template',
        '',
        '',
        'Held merits'
    ],
    ['', '', '', '', '', '', '', '', '', '', '', 'Redeemed merits'],
    [
        'Opp. trigger',
        '',
        '#DIV/0! (Function DIVIDE parameter 2 cannot be zero.)',
        0,
        0,
        0,
        "#N/A (Did not find value 'Expansion Template' in VLOOKUP evaluation.)",
        "#N/A (Did not find value 'Expansion Template' in VLOOKUP evaluation.)",
        'Expansion Template',
        '',
        '',
        'Held merits'
    ],
    [
        '% safety margin',
        0.5,
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        'Redeemed merits'
    ]
]

SYSTEMS_DATA = [
    ['', 0.7, 4910, 0, 4322, 4910, 0, 116.99, '', 'Frey'],
    ['', 0.6, 8425, 0, 3844, 5422, 0, 99.51, '', 'Nurundere'],
    ['', 0, 5974, 0, 0, 0, 0, 55.72, '', 'LHS 3749'],
    ['', 0, 5211, 0, 0, 2500, 0, 28.94, 'Leave for grinders', 'Sol'],
    ['', 0, 7239, 0, 0, 0, 0, 81.54, '', 'Dongkum'],
    ['', 0, 6476, 0, 0, 0, 0, 67.27, '', 'Alpha Fornacis'],
    ['', 0, 7367, 0, 0, 0, 0, 83.68, 'Priority for S/M ships (no L pads)', 'Othime'],
]
SYSTEMSUM_DATA = [
    [
        ['', 0, 0, 14878, 13950, -452, 'Sec: Medium', 'Sol', 'Cemplangpa', 15000, 1, 0, 1380],
        [0, 0, 0, 0, 0, 0, ''],
    ],
    [
        ['', 0, 0, 12500, 10500, 2000, 'Sec: Anarchy', 'Atropos', 'Pequen', 10500, 0.5, 0, 0],
        [0, 0, 0, 0, 0, 0, ''],
    ],
    [
        ['Exp', 0, 0, 364298, 160472, 127278, 'Sec: Low', 'Dongkum', 'Burr',
         161630, 35, 0, 76548],
        [0, 0, 0, 0, 0, 0, ''],
    ],
    [
        ['Opp', 0, 0, 59877, 10470, 12147, 'Sec: Low', 'Atropos', 'AF Leopris',
         47739, 1.69, 0, 23960],
        [0, 0, 0, 0, 0, 0, ''],
    ],
]
SYSTEMUM_EXPAND = [
    ['Exp', 0, 0, 364298, 160472, 127278, 'Sec: Low', 'Dongkum', 'Burr', 161630, 35, 0, 76548],
    [0, 0, 0, 0, 0, 0, ''],
]
SYSTEMUM_OPPOSE = [
    ['Opp', 0, 0, 59877, 10470, 12147, 'Sec: Low', 'Atropos', 'AF Leopris', 47739, 1.69, 0, 23960],
    [0, 0, 0, 0, 0, 0, ''],
]
SYSTEMUM_CONTROL = [
    ['', 0, 0, 14878, 13950, -452, 'Sec: Medium', 'Unknown', 'Cemplangpa', 13830, 1, 0, 1380],
    [0, 0, 0, 0, 0, 0, ''],
]
