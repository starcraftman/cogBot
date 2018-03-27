"""
Scout lists and logic to generate a route plan.
"""
from __future__ import absolute_import, print_function

# import cogdb
# import cogdb.eddb
# import cogdb.query
# import cogdb.side
# import cog.inara
# import cog.jobs
# import cog.tbl
# import cog.util

ROUND = {
    1: [
        "Epsilon Scorpii",
        "39 Serpentis",
        "Parutis",
        "Mulachi",
        "Aornum",
        "WW Piscis Austrini",
        "LHS 142",
        "LHS 6427",
        "LP 580-33",
        "BD+42 3917",
        "Venetic",
        "Kaushpoos",
    ],
    2: [
        "Atropos",
        "Alpha Fornacis",
        "Rana",
        "Anlave",
        "NLTT 46621",
        "16 Cygni",
        "Adeo",
        "LTT 15449",
        "LHS 3447",
        "Lalande 39866",
        "Abi",
        "Gliese 868",
        "Othime",
        "Phra Mool",
        "Wat Yu",
        "Shoujeman",
        "Phanes",
        "Dongkum",
        "Nurundere",
        "LHS 3749",
        "Mariyacoch",
        "Frey",
    ],
    3: [
        "GD 219",
        "Wolf 867",
        "Gilgamesh",
        "LTT 15574",
        "LHS 3885",
        "Wolf 25",
        "LHS 6427",
        "LHS 1541",
        "LHS 1197",
    ]
}
TEMPLATE = """__**Scout List**__
Total Distance: **{}**ly

```**REQUESTING NEW RECON OBJECTIVES __{} {}__  , {}**

If you are running more than one system, do them in this order and you'll not ricochet the whole galaxy. Also let us know if you want to be a member of the FRC Scout Squad!
@here @FRC Scout

{}

:o7:```"""
INTERACT = """Will generate scout list with the following systems:\nkj

{}

To add system: type system name **NOT** in list
To remove system: type system name in list
To generate list: reply with **stop**

__This message will delete itself on success or 30s timeout.__"""
