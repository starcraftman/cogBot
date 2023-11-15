"""
Common imports for all of cogdb.eddb module.
"""
import sqlalchemy.ext.declarative

# The maximum lengths for strings stored in the db
LEN = {
    "allegiance": 18,
    "cmdr_name": 25,
    "ship_name": 30,
    "commodity": 34,
    "commodity_category": 20,
    "commodity_group": 30,
    "economy": 18,
    "eddn": 25,
    "faction": 90,
    "faction_happiness": 12,
    "faction_state": 23,
    "government": 18,
    "module": 40,
    "module_category": 20,  # Name of group of similar groups like limpets, weapons
    "module_group": 36,  # Name of module group, i.e. "Beam Laser"
    "module_symbol": 50,  # Information about module
    "power": 21,
    "power_abv": 6,
    "power_state": 18,
    "pvp_name": 50,
    "pvp_fname": 80,
    "pvp_hash": 150,
    "security": 8,
    "settlement_security": 10,
    "settlement_size": 3,
    "ship": 25,
    "station": 45,
    "station_pad": 4,
    "station_type": 24,
    "system": 50,
    "weapon_mode": 6,
}
LEN["spy_location"] = 5 + LEN["system"] + LEN["station"]

# The base for all EDDB databases tables.
Base = sqlalchemy.ext.declarative.declarative_base()
