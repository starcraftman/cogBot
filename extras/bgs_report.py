"""
Generate a bgs report listing each bubble, system and their faction distribution.
"""
from __future__ import absolute_import, print_function
import sqlalchemy as sqla

import cogdb
from cogdb.side import System, Faction, Influence, Government


def count_facts(factions):
    cnts = {}

    for sys, *_, gov in factions:
        try:
            cnts[sys.name]
        except KeyError:
            cnts[sys.name] = {}

        try:
            cnts[sys.name][gov.text] += 1
        except KeyError:
            cnts[sys.name][gov.text] = 1

    return cnts


def write_report():
    print("Report being generated, please wait patiently.")
    session = cogdb.SideSession()
    controls = session.query(System).\
            filter(sqla.and_(System.power_state_id == 16,
                             System.power_id == 9)).\
            all()

    fout = open('/tmp/report.txt', 'w')
    for con in controls:
        fout.write('Control: ' + con.name + '\n')
        factions = session.query(System, Influence, Faction, Government).\
                filter(sqla.and_(System.dist_to(con) <= 15,
                                 System.power_state_id != 48)).\
                filter(Influence.system_id == System.id).\
                filter(Faction.id == Influence.faction_id).\
                filter(Faction.government_id == Government.id).\
                order_by(System.name).\
                all()

        con_stats = count_facts(factions)
        fout.writelines(['{:15} {}\n'.format(key[:15], con_stats[key]) for key in con_stats])
        fout.write('\n')

    fout.close()
    print("Report written to /tmp/report.txt")


def main():
    write_report()


if __name__ == "__main__":
    main()
