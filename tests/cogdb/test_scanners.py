# pylint: disable=redefined-outer-name,missing-function-docstring,unused-argument
"""
Tests for cogdb.scanners
"""
import datetime
import os

import aiomock
import pytest
import sqlalchemy as sqla

import cog.exc
import cogdb.scanners
from cogdb.schema import (FortSystem, FortDrop, FortUser,
                          UMSystem, UMUser, UMHold, KOS, TrackByID,
                          EUMSheet)
import cogdb.spy_squirrel as spy
from cogdb.scanners import (FortScanner, UMScanner, KOSScanner, RecruitsScanner, CarrierScanner,
                            GalScanner)

SENT = []


@pytest.mark.asyncio
async def test_fortscanner_update_cells(f_asheet_fortscanner):
    fscan = FortScanner(f_asheet_fortscanner)

    assert not fscan.cells_row_major

    await fscan.update_cells()

    assert fscan.cells_row_major[2][0] == "Total Fortification Triggers:"
    assert fscan.cells_col_major[0][2] == "Total Fortification Triggers:"


def test_fortscanner__repr__(session, f_asheet_fortscanner,
                             f_dusers, f_fort_testbed, db_cleanup):
    fscan = FortScanner(f_asheet_fortscanner)
    assert "FortScanner" in repr(fscan)


def test_fortscanner_drop_db_entries(session, f_asheet_fortscanner,
                                     f_dusers, f_fort_testbed, db_cleanup):
    fscan = FortScanner(f_asheet_fortscanner)
    assert session.query(FortUser).all()
    assert session.query(FortSystem).all()
    assert session.query(FortDrop).all()

    fscan.drop_db_entries(session)

    assert not session.query(FortUser).all()
    assert not session.query(FortSystem).all()
    assert not session.query(FortDrop).all()


def test_fortscanner_drop_db_entries_error(session, f_asheet_fortscanner,
                                           f_dusers, f_fort_testbed, db_cleanup):
    fscan = FortScanner(f_asheet_fortscanner)
    assert session.query(FortUser).all()
    assert session.query(FortSystem).all()
    assert session.query(FortDrop).all()
    session.close()

    # Simulate drop from external source or some critical error!
    sqla.orm.session.close_all_sessions()
    with cogdb.engine.connect() as con:
        con.execute(sqla.sql.text('drop table hudson_fort_merits'))

    with cogdb.session_scope(cogdb.Session) as new_session:
        fscan.drop_db_entries(new_session)

    assert not session.query(FortUser).all()
    assert not session.query(FortSystem).all()
    assert not session.query(FortDrop).all()


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


@pytest.mark.asyncio
async def test_fortscanner_get_batch(f_asheet_fortscanner):
    fscan = FortScanner(f_asheet_fortscanner)
    f_asheet_fortscanner.batch_get.async_return_value = ['This is a test', 'Message']

    returned_data = await fscan.get_batch(['A1:B2'])
    expected_returned_data = ['This is a test', 'Message']

    assert returned_data == expected_returned_data


def test_fortscanner_update_sheet_user_dict():
    data = FortScanner.update_sheet_user_dict(22, "cog is great", "gears")
    assert data == [{"range": "A22:B22", "values": [["cog is great", "gears"]]}]


def test_fortscanner_update_systems_dict():
    data = FortScanner.update_system_dict("G", 5000, 2222)
    assert data == [{"range": "G6:G7", "values": [[5000], [2222]]}]


def test_fortscanner_update_drop_dict():
    data = FortScanner.update_drop_dict("G", 22, 7000)
    assert data == [{"range": "G22:G22", "values": [[7000]]}]


def test_fortscanner_update_import_mode_dict():
    data = FortScanner.update_import_mode_dict("B9:B9", 'FALSE')
    assert data == [{"range": "B9:B9", "values": [['FALSE']]}]


def test_umscanner__repr__(session, f_asheet_umscanner, f_dusers, f_um_testbed):
    umscan = UMScanner(f_asheet_umscanner)
    assert "UMScanner" in repr(umscan)


def test_umscanner_drop_db_entries(session, f_asheet_umscanner, f_dusers, f_um_testbed):
    umscan = UMScanner(f_asheet_umscanner)
    assert session.query(UMUser).filter(UMUser.sheet_src == EUMSheet.main).all()
    assert session.query(UMUser).filter(UMUser.sheet_src == EUMSheet.snipe).all()
    assert session.query(UMSystem).filter(UMSystem.sheet_src == EUMSheet.main).all()
    assert session.query(UMSystem).filter(UMSystem.sheet_src == EUMSheet.snipe).all()
    assert session.query(UMHold).filter(UMHold.sheet_src == EUMSheet.main).all()
    assert session.query(UMHold).filter(UMHold.sheet_src == EUMSheet.snipe).all()

    umscan.drop_db_entries(session)

    assert not session.query(UMUser).filter(UMUser.sheet_src == EUMSheet.main).all()
    assert session.query(UMUser).filter(UMUser.sheet_src == EUMSheet.snipe).all()
    assert not session.query(UMSystem).filter(UMSystem.sheet_src == EUMSheet.main).all()
    assert session.query(UMSystem).filter(UMSystem.sheet_src == EUMSheet.snipe).all()
    assert not session.query(UMHold).filter(UMHold.sheet_src == EUMSheet.main).all()
    assert session.query(UMHold).filter(UMHold.sheet_src == EUMSheet.snipe).all()


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


def test_umscanner_update_systemsum_priority_dict():
    data = UMScanner.update_systemum_priority_dict("G", "Medium Priority")
    assert data == [{'range': 'H8:H8', 'values': [['Medium Priority']]}]


def test_umscanner_update_systemsum_dict():
    data = UMScanner.update_systemum_dict("G", 7000, 500, 4300)
    assert data == [{"range": "G10:G13", "values": [[7000], ["50000.00%"], ["Hold Merits"], [4300]]}]


def test_umscanner_update_hold_dict():
    data = UMScanner.update_hold_dict("G", 22, 750, 3000)
    assert data == [{"range": "G22:H22", "values": [[750, 3000]]}]


@pytest.mark.asyncio
async def test_umscanner_slide_templates(f_asheet_umscanner):
    systems = []
    value_to_add = [
        {"sys_name": "Frey", "power": "Yuri Grom", "trigger": "12345", "priority": "Normal"},
        {"sys_name": "Lawd 26", "power": "Edmund Mahon", "trigger": "22222", "priority": "Normal"},
    ]
    for i in range(17):
        systems.append(await f_asheet_umscanner.values_col(i))
    systems = [systems[i][:13] for i in range(len(systems))]
    returned_data = UMScanner.slide_templates([systems[3:]], value_to_add)
    expected_return = [{
        'range': 'N1:13',
        'values': [
            ['', '', '', '', '', '', 'Opp. trigger', '% safety margin'],
            ['', '', '', '', '', '', '', '50%'],
            ['0', '', '0', '', '0', '', '#DIV/0!', ''],
            ['12345', '', '22222', '', '1,000', '', '0', ''],
            ['0', '', '0', '', '0', '', '0', ''],
            ['1,000', '', '1,000', '', '1,000', '', '0', ''],
            ['Sec: N/A', 'Yuri Grom', 'Sec: N/A', 'Edmund Mahon', 'Sec: N/A', '', 'Sec: N/A', ''],
            ['', 'Normal', '', 'Normal', '', '', '', ''],
            ['Frey', '', 'Lawd 26', '', 'Control System Template', '', 'Expansion Template', ''],
            [0, '', '0', '', '', '', '', ''],
            [0, '', '0', '', '', '', '', ''],
            [
                'Held merits',
                'Redeemed merits',
                'Held merits',
                'Redeemed merits',
                'Held merits',
                'Redeemed merits',
                'Held merits',
                'Redeemed merits'
            ],
            ['', '', '', '', '', '', '', '']
        ]
    }]

    assert returned_data == expected_return


@pytest.mark.asyncio
async def test_umscanner_slide_formula_by_offset(f_umformula_values):
    sheet_values = f_umformula_values

    expected = [
        'Opp. trigger',
        5905,
        '=if(max(D$10,D$5+D$13)/D$2-D$11<0,concatenate("behind by '
        '",round(100*(max(D$10,D$5+D$13)/D$2-D$11)),"%"),concatenate("leading by '
        '",round(100*(MAX(D$10,D$5+D$13)/D$2-D$11)),"%"))',
        '=D2*D11*(1+E2)',
        '=SUM(D14:E)',
        '=IF(D$10 > D$5+D$13, D$4 - D$10, D$4 - D$5-D$13)',
        "=MAX(SUM(D$14:D),E$10)-SUM(D$14:D)["
    ]

    sheet_values[2][6] = "=MAX(SUM(F$14:F),G$10)-SUM(F$14:F)["
    results = UMScanner.slide_formula_by_offset(sheet_values[2], -2)
    assert results[:7] == expected


@pytest.mark.asyncio
async def test_umscanner_remove_um_system(f_umformula_values):
    payload = UMScanner.remove_um_system(f_umformula_values, 'Pequen')
    expected = [
        '',
        '',
        '=IF(J$10 > J$5+J$13, J$10 / J$4 * 100, ROUNDDOWN((J$5+J$13) / J$4 * 100))',
        '=J2*J11*(1+K2)',
        '=SUM(J14:K)',
        '=IF(J$10 > J$5+J$13, J$4 - J$10, J$4 - J$5-J$13)',
        'Sec: Low',
        'Muncheim',
        'Albisiyatae',
        '',
        '',
        'Held merits',
        '',
        '',
        '',
        1200,
        '',
        600,
        '',
        '',
        750,
        '',
        1500,
        '',
        650,
        600
    ]
    actual_values = payload[0]['values']
    for ind, e_val in enumerate(expected):
        assert e_val == actual_values[ind][6]


def test_umscanner_slide_formula_to_right():
    raw_data = [[['', '', '=IF(N$10 > N$5+N$13, N$10 / N$4 * 100, ROUNDDOWN((N$5+N$13) / N$4 * 100))',
                  1000, '=SUM(N$14:O)', '=IF(N$10 > N$5+N$13, N$4 - N$10, N$4 - N$5-N$13)',
                  '=CONCATENATE("Sec: ",IF(ISBLANK(VLOOKUP(N$9,Import!$A$2:$C,2,FALSE)),"N/A",VLOOKUP(N$9,Import!$A$2:$C,2,FALSE)))',
                  '=VLOOKUP(N$9,Import!$A$2:$C,3,FALSE)', 'Control System Template', '', '', 'Held merits',
                  '=max(SUM(O$14:O),N$10)-SUM(O$14:O)']]]

    returned_data = UMScanner.slide_formula_to_right(raw_data, 11)

    expected_returned_data = [[['', '', '=IF(P$10 > P$5+P$13, P$10 / P$4 * 100, ROUNDDOWN((P$5+P$13) / P$4 * 100))',
                                '1000', '=SUM(P$14:Q)', '=IF(P$10 > P$5+P$13, P$4 - P$10, P$4 - P$5-P$13)',
                                '=CONCATENATE("Sec: ",IF(ISBLANK(VLOOKUP(P$9,Import!$A$2:$C,2,FALSE)),"N/A",VLOOKUP(P$9,Import!$A$2:$C,2,FALSE)))',
                                '=VLOOKUP(P$9,Import!$A$2:$C,3,FALSE)', 'Control System Template', '', '', 'Held merits',
                                '=max(SUM(Q$14:Q),P$10)-SUM(Q$14:Q)']]]
    assert returned_data == expected_returned_data


def test_umscanner_slide_formula_to_left():
    column = ['', '', '=IF(N$10 > N$5+N$13, N$10 / N$4 * 100, ROUNDDOWN((N$5+N$13) / N$4 * 100))',
                  1000, '=SUM(N$14:O)', '=IF(N$10 > N$5+N$13, N$4 - N$10, N$4 - N$5-N$13)',
                  '=CONCATENATE("Sec: ",IF(ISBLANK(VLOOKUP(N$9,Import!$A$2:$C,2,FALSE)),"N/A",VLOOKUP(N$9,Import!$A$2:$C,2,FALSE)))',
                  '=VLOOKUP(N$9,Import!$A$2:$C,3,FALSE)', 'Control System Template', '', '', 'Held merits',
                  '=max(SUM(O$14:O),N$10)-SUM(O$14:O)']

    returned_data = UMScanner.slide_formula_by_offset(column, -2)

    expected_returned_data = ['', '', '=IF(L$10 > L$5+L$13, L$10 / L$4 * 100, ROUNDDOWN((L$5+L$13) / L$4 * 100))',
                              '1000', '=SUM(L$14:M)', '=IF(L$10 > L$5+L$13, L$4 - L$10, L$4 - L$5-L$13)',
                              '=CONCATENATE("Sec: ",IF(ISBLANK(VLOOKUP(L$9,Import!$A$2:$C,2,FALSE)),"N/A",VLOOKUP(L$9,Import!$A$2:$C,2,FALSE)))',
                              '=VLOOKUP(L$9,Import!$A$2:$C,3,FALSE)', 'Control System Template', '', '', 'Held merits',
                              '=max(SUM(M$14:M),L$10)-SUM(M$14:M)']
    for actual, expect in zip(returned_data, expected_returned_data):
        assert str(actual) == expect


# Sanity check for fixture, I know not needed.
@pytest.mark.asyncio
async def test_kosscanner_update_cells(f_asheet_kos):
    fscan = KOSScanner(f_asheet_kos)

    assert not fscan.cells_row_major

    await fscan.update_cells()

    assert fscan.cells_row_major[2][0] == "WildWetWalrus1"
    assert fscan.cells_col_major[0][2] == "WildWetWalrus1"


@pytest.mark.asyncio
async def test_kosscanner_find_dupe(f_asheet_kos):
    fscan = KOSScanner(f_asheet_kos)

    await fscan.update_cells()
    cnt, _ = fscan.find_dupe('Silas Kruge')
    assert cnt == 1468

    assert not fscan.find_dupe('NotThere')[0]


@pytest.mark.asyncio
async def test_kosscanner_kos_entries(f_asheet_kos):
    fscan = KOSScanner(f_asheet_kos)

    await fscan.update_cells()
    ents = fscan.kos_entries()

    assert [x for x in ents if x.cmdr == "NewkTV"]


@pytest.mark.asyncio
async def test_kosscanner_parse_sheet(f_asheet_kos, session, db_cleanup):
    assert not session.query(KOS).all()

    fscan = KOSScanner(f_asheet_kos)
    await fscan.update_cells()
    fscan.parse_sheet(session)

    assert session.query(KOS).all()
    session.close()


@pytest.mark.asyncio
async def test_kosscanner_parse_sheet_dupes(f_asheet_kos, session, db_cleanup):
    fscan = KOSScanner(f_asheet_kos)

    assert not session.query(KOS).all()

    await fscan.update_cells()
    fscan.cells_row_major = fscan.cells_row_major[:3] + fscan.cells_row_major[1:3]

    with pytest.raises(cog.exc.SheetParsingError):
        fscan.parse_sheet(session)


@pytest.mark.asyncio
async def test_kosscanner_next_free_row(f_asheet_kos, session, db_cleanup):
    fscan = KOSScanner(f_asheet_kos)

    assert not session.query(KOS).all()

    await fscan.update_cells()
    assert fscan.next_free_row() == 1469


@pytest.mark.asyncio
async def test_kosscanner_add_report_dict(f_asheet_kos, session, db_cleanup):
    fscan = KOSScanner(f_asheet_kos)

    assert not session.query(KOS).all()

    await fscan.update_cells()
    kos_info = {
        'cmdr': 'cmdr',
        'squad': 'squad',
        'reason': 'A reason',
        'is_friendly': False
    }
    payload = fscan.add_report_dict(kos_info)
    assert payload == [{'range': 'A1469:D1469', 'values': [['cmdr', 'squad', 'KILL', 'A reason']]}]


@pytest.mark.asyncio
async def test_recruitsscanner_update_first_free():
    fake_sheet = aiomock.AIOMock()
    fake_sheet.whole_sheet.async_return_value = [['1'], ['2'], ['3'], ['4'], ['5'], ['6'], ['7']]
    r_scanner = RecruitsScanner(fake_sheet)
    await r_scanner.update_cells()
    assert r_scanner.update_first_free() == 8


def test_recruitsscanner_add_recruit_dict():
    r_scanner = RecruitsScanner(None)
    r_scanner.first_free = 10
    today = datetime.date.today()

    expect = [
        {'range': 'A10:C10', 'values': [['Default', 'Default', str(today)]]},
        {'range': 'E10:E10', 'values': [['R']]},
        {'range': 'H10:H10', 'values': [['1']]},
        {'range': 'N10:O10', 'values': [['A PMF', 'A note here']]}
    ]
    data = r_scanner.add_recruit_dict(
        cmdr="Default",
        discord_name="Default",
        rank="R",
        platform="1",
        pmf="A PMF",
        notes="A note here",
    )
    assert data == expect


@pytest.mark.asyncio
async def test_carrierscanner_parse_carriers():
    fake_sheet = aiomock.AIOMock()
    fake_sheet.whole_sheet.async_return_value = [["Carrier ID", "Squadron"], ['XJ1-222', "Baddies"], ['FX3-42A', "Baddies"]]
    r_scanner = CarrierScanner(fake_sheet)
    await r_scanner.update_cells()

    expected = {
        'XJ1-222': {
            'id': 'XJ1-222',
            'squad': 'Baddies',
            'override': True
        },
        'FX3-42A': {
            'id': 'FX3-42A',
            'squad': 'Baddies',
            'override': True
        }
    }
    assert r_scanner.carriers() == expected


@pytest.mark.asyncio
async def test_carrierscanner_parse_sheet(session, f_track_testbed):
    fake_sheet = aiomock.AIOMock()
    fake_sheet.whole_sheet.async_return_value = [["Carrier ID", "Squadron"], ['XJ1-222', "Baddies"], ['FX3-42A', "Baddies"]]
    r_scanner = CarrierScanner(fake_sheet)
    await r_scanner.update_cells()

    r_scanner.parse_sheet(session)

    found = session.query(TrackByID).filter(TrackByID.id.in_(['XJ1-222', 'FX3-42A'])).all()
    assert len(found) == 2


@pytest.mark.asyncio
async def test_galscanner_update_dict(session):
    try:
        spy_systems = [
            spy.SpySystem(
                id=1, power_id=9, ed_system_id=10477373803, power_state_id=16,
                fort=4444, fort_trigger=3333, um=4555, um_trigger=5555
            ),
            spy.SpySystem(
                id=2, power_id=9, ed_system_id=83852530386, power_state_id=16,
                fort=0, fort_trigger=3333, um=0, um_trigger=5555
            ),
        ]
        with cogdb.session_scope(cogdb.EDDBSession) as eddb_session:
            eddb_session.add_all(spy_systems)
            eddb_session.commit()

            fake_sheet = aiomock.AIOMock()
            r_scanner = GalScanner(fake_sheet)

            expect = [
                {'range': 'A3:C5', 'values': [['SOL', 4444, 4555], ['RANA', 0, 0]]},
                {'range': 'L3:P5', 'values': [['SOL', 0, 0, 3333, 5555], ['RANA', 0, 0, 3333, 5555]]},
                {'range': 'R3:S5', 'values': [['SOL', 0], ['RANA', 0]]},
            ]
            assert expect == r_scanner.update_dict(systems=spy_systems)[:-1]
    finally:
        spy.empty_tables()


@pytest.mark.asyncio
async def test_galscanner_clear_cells(session):
    async def batch_clear_stub(x):
        global SENT
        SENT += [x]

    SENT.clear()
    fake_sheet = aiomock.AIOMock()
    fake_sheet.batch_clear = batch_clear_stub
    r_scanner = GalScanner(fake_sheet)

    await r_scanner.clear_cells()
    assert [['A3:C400', 'L3:P400', 'R3:S400', 'D3:E400', 'F3:H400', 'I3:K400']] == SENT


@pytest.mark.asyncio
async def test_galscanner_cycle_reset(session):
    async def batch_clear_stub(x):
        global SENT
        SENT += [x]

    SENT.clear()
    fake_sheet = aiomock.AIOMock()
    fake_sheet.batch_clear = batch_clear_stub
    r_scanner = GalScanner(fake_sheet)

    await r_scanner.cycle_reset()
    assert [['B3:K400', 'M3:P400', 'S3:S400']] == SENT


@pytest.mark.skipif(not os.environ.get('ALL_TESTS'), reason="Slow scanner testing all scanners.")
@pytest.mark.asyncio
async def test_init_scanners():
    scanners = await cogdb.scanners.init_scanners()

    assert isinstance(scanners['hudson_cattle'], FortScanner)
    assert isinstance(scanners['hudson_undermine'], UMScanner)
    assert isinstance(scanners['hudson_kos'], KOSScanner)
