"""
Tests for cogdb.dbi
"""
import cogdb.dbi

def test_make_parser():
    args = cogdb.dbi.make_parser().parse_args(['-r', '-c', '--ids'])
    assert not args.yes
    assert args.recreate


def test_confirm_msg():
    expect = """The following steps will take place.

    Preserve the current galaxy_stations.json to a backup
    Download and extract the latest spansh dump
    Update the ID maps for the dump
    Update the cached modules and commodity information.
    Recreate all EDDB, spy and spansh tables and preload data.
    Parse all the information present in current galaxy_stations.json
    Replace the following possibly existing EDDB tables with that information:
        cogdb.eddb.{System, Faction, Influence, Station, StationFeatures, StationEconomy, FactionActiveState}
        cogdb.spansh.{SModuleSold, SCommodityPricing}

Please confirm with yes or no: """
    args = cogdb.dbi.make_parser().parse_args(['-r', '-c', '-f', '--commodities'])
    assert expect == cogdb.dbi.confirm_msg(args)
