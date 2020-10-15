"""
Tests for cogdb.scanners
"""
import os
import pytest

import cog.exc
import cogdb.scanners
from cogdb.schema import (FortSystem, FortDrop, FortUser,
                          UMSystem, UMUser, UMHold, KOS)
from cogdb.scanners import (FortScanner, UMScanner, KOSScanner)


@pytest.mark.asyncio
async def test_fortscanner_update_cells(f_asheet_fortscanner):
    fscan = FortScanner(f_asheet_fortscanner)

    assert not fscan.cells_row_major

    await fscan.update_cells()

    assert fscan.cells_row_major[2][0] == "Total Fortification Triggers:"
    assert fscan.cells_col_major[0][2] == "Total Fortification Triggers:"


def test_fortscanner_flush_to_db(session, f_asheet_fortscanner,
                                 f_dusers, f_fort_testbed, db_cleanup):
    fscan = FortScanner(f_asheet_fortscanner)
    assert session.query(FortUser).all()
    assert session.query(FortSystem).all()
    assert session.query(FortDrop).all()

    objs = [FortSystem(name='Test')]
    fscan.flush_to_db(session, [objs])

    assert not session.query(FortUser).all()
    assert session.query(FortSystem).all()
    assert not session.query(FortDrop).all()


@pytest.mark.asyncio
async def test_fortscanner_users(f_asheet_fortscanner):
    fscan = FortScanner(f_asheet_fortscanner)

    await fscan.update_cells()
    users = fscan.users()

    assert len(users) == 22
    assert users[-1].name == "gears"


@pytest.mark.asyncio
async def test_fortscanner_update_system_column(f_asheet_fortscanner):
    fscan = FortScanner(f_asheet_fortscanner)

    assert not fscan.cells_row_major

    await fscan.update_cells()
    fscan.update_system_column()

    assert fscan.system_col == "G"


@pytest.mark.asyncio
async def test_fortscanner_fort_systems(f_asheet_fortscanner):
    fscan = FortScanner(f_asheet_fortscanner)

    await fscan.update_cells()
    fscan.update_system_column()
    f_systems = fscan.fort_systems()

    assert len(f_systems) == 75
    assert f_systems[-1].name == "Mantxe"
    assert f_systems[-1].sheet_col == "CC"


@pytest.mark.asyncio
async def test_fortscanner_prep_systems(f_asheet_fortscanner):
    fscan = FortScanner(f_asheet_fortscanner)

    await fscan.update_cells()
    fscan.update_system_column()
    f_systems = fscan.prep_systems()

    assert len(f_systems) == 1
    assert f_systems[-1].name == "Rhea"
    assert f_systems[-1].sheet_col == "D"


@pytest.mark.asyncio
async def test_fortscanner_drops(f_asheet_fortscanner):
    fscan = FortScanner(f_asheet_fortscanner)

    await fscan.update_cells()
    fscan.update_system_column()
    f_users = fscan.users()
    f_systems = fscan.fort_systems()
    f_merits = fscan.drops(f_systems, f_users)

    assert len(f_merits) == 33
    assert f_merits[0].system_id == 1
    assert f_merits[0].user_id == 2


@pytest.mark.asyncio
async def test_fortscanner_parse_sheet(f_asheet_fortscanner, f_dusers_many, session, db_cleanup):
    fscan = FortScanner(f_asheet_fortscanner)

    assert not session.query(FortSystem).all()
    assert not session.query(FortUser).all()
    assert not session.query(FortDrop).all()

    await fscan.update_cells()
    fscan.parse_sheet(session)

    assert session.query(FortSystem).all()
    assert session.query(FortUser).all()
    assert session.query(FortDrop).all()


@pytest.mark.asyncio
async def test_fortscanner_send_batch(f_asheet_fortscanner):
    data = FortScanner.update_sheet_user_dict(22, "cog is great", "gears")
    fscan = FortScanner(f_asheet_fortscanner)

    await fscan.send_batch(data)

    assert fscan.asheet.batch_update_sent == data


def test_fortscanner_update_sheet_user_dict():
    data = FortScanner.update_sheet_user_dict(22, "cog is great", "gears")
    assert data == [{"range": "A22:B22", "values": [["cog is great", "gears"]]}]


def test_fortscanner_update_systems_dict():
    data = FortScanner.update_system_dict("G", 5000, 2222)
    assert data == [{"range": "G6:G7", "values": [[5000], [2222]]}]


def test_fortscanner_update_drop_dict():
    data = FortScanner.update_drop_dict("G", 22, 7000)
    assert data == [{"range": "G22:G22", "values": [[7000]]}]


#  # Sanity check for fixture, I know not needed.
@pytest.mark.asyncio
async def test_umscanner_update_cells(f_asheet_umscanner):
    fscan = UMScanner(f_asheet_umscanner)

    assert not fscan.cells_row_major

    await fscan.update_cells()

    assert fscan.cells_row_major[2][0] == "Cycle ?: The Empire Sucks Placeholder"
    assert fscan.cells_col_major[0][2] == "Cycle ?: The Empire Sucks Placeholder"


@pytest.mark.asyncio
async def test_umscanner_users(f_asheet_umscanner):
    fscan = UMScanner(f_asheet_umscanner)

    await fscan.update_cells()
    users = fscan.users()

    assert len(users) == 28
    assert users[-1].name == "gears"


@pytest.mark.asyncio
async def test_umscanner_systems(f_asheet_umscanner):
    fscan = UMScanner(f_asheet_umscanner)

    await fscan.update_cells()
    systems = fscan.systems()

    assert systems[-1].name == "Albisiyatae"
    assert systems[-1].sheet_col == "L"


@pytest.mark.asyncio
async def test_umscanner_holds(f_asheet_umscanner):
    fscan = UMScanner(f_asheet_umscanner)

    await fscan.update_cells()
    users = fscan.users()
    systems = fscan.systems()
    merits = fscan.holds(systems, users)

    assert len(merits) == 46
    assert merits[-1].system_id == 5
    assert merits[-1].user_id == 14
    assert merits[-1].redeemed == 900


@pytest.mark.asyncio
async def test_umscanner_parse_sheet(f_asheet_umscanner, f_dusers_many, session, db_cleanup):
    fscan = UMScanner(f_asheet_umscanner)

    assert not session.query(UMUser).all()
    assert not session.query(UMSystem).all()
    assert not session.query(UMHold).all()

    await fscan.update_cells()
    fscan.parse_sheet(session)

    assert session.query(UMUser).all()
    assert session.query(UMSystem).all()
    assert session.query(UMHold).all()


def test_umscanner_update_systemsum_dict():
    data = UMScanner.update_systemum_dict("G", 7000, 500, 4300)
    assert data == [{"range": "G10:G13", "values": [[7000], [500], ["Hold Merits"], [4300]]}]


def test_umscanner_update_hold_dict():
    data = UMScanner.update_hold_dict("G", 22, 750, 3000)
    assert data == [{"range": "G22:H22", "values": [[750, 3000]]}]


# Sanity check for fixture, I know not needed.
@pytest.mark.asyncio
async def test_kosscanner_update_cells(f_asheet_kos):
    fscan = KOSScanner(f_asheet_kos)

    assert not fscan.cells_row_major

    await fscan.update_cells()

    assert fscan.cells_row_major[2][0] == "WildWetWalrus1"
    assert fscan.cells_col_major[0][2] == "WildWetWalrus1"


@pytest.mark.asyncio
async def test_kosscanner_kos_entries(f_asheet_kos):
    fscan = KOSScanner(f_asheet_kos)

    await fscan.update_cells()
    ents = fscan.kos_entries()

    assert [x for x in ents if x.cmdr == "NewkTV"]


@pytest.mark.asyncio
async def test_kosscanner_parse_sheet(f_asheet_kos, session, db_cleanup):
    fscan = KOSScanner(f_asheet_kos)

    assert not session.query(KOS).all()

    await fscan.update_cells()
    fscan.parse_sheet(session)

    assert session.query(KOS).all()


@pytest.mark.asyncio
async def test_kosscanner_parse_sheet_dupes(f_asheet_kos, session, db_cleanup):
    fscan = KOSScanner(f_asheet_kos)

    assert not session.query(KOS).all()

    await fscan.update_cells()
    fscan.cells_row_major = fscan.cells_row_major[:3] + fscan.cells_row_major[1:3]

    with pytest.raises(cog.exc.SheetParsingError):
        fscan.parse_sheet(session)


def test_kosscanner_kos_report_dict():
    data = KOSScanner.kos_report_dict(75, "Gears", "Cookies", 0, "KILL")
    assert data == [{"range": "A75:D75", "values": [["Gears", "Cookies", 0, "KILL"]]}]


@pytest.mark.skipif(not os.environ.get('ALL_TESTS'), reason="Slow scanner testing all scanners.")
@pytest.mark.asyncio
async def test_init_scanners():
    scanners = await cogdb.scanners.init_scanners()

    assert isinstance(scanners['hudson_cattle'], FortScanner)
    assert isinstance(scanners['hudson_undermine'], UMScanner)
    assert isinstance(scanners['hudson_kos'], KOSScanner)
