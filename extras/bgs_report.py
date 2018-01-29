"""
Generate a bgs report listing each bubble, system and their faction distribution.
"""
from __future__ import absolute_import, print_function
import sqlalchemy as sqla

import cogdb
from cogdb.side import System, Faction, Influence, Government


def count_facts(factions):
    """
    Count all the factions within a bubble's systems.
    """
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
    """
    Generate a report covering breakdown of faction governments in Hudson bubbles.
    """
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


def feudal_finder(gap=15):
    """
    Find favorable feudals or patronages around a system.
    """
    session = cogdb.SideSession()
    s_name = input('System to look around: ')
    centre = session.query(System).filter(System.name == s_name).one()

    dist = 0
    while True:
        dist += gap
        print("Searching all systems <= {} from {}.".format(dist, s_name))
        matches = session.query(System, Influence, Faction, Government).\
            filter(sqla.and_(System.dist_to(centre) <= dist,
                             Influence.system_id == System.id,
                             Faction.id == Influence.faction_id,
                             Faction.government_id == Government.id,
                             Government.id.in_(['128', '144']))).\
            order_by(System.dist_to(centre)).\
            all()

        if matches:
            header = "\n{:16} {:4} {:5} {:5} {}".format(
                'System Name', 'Govt', 'Dist', 'Inf', 'Faction Name')
            print(header + '\n' + '-' * len(header))

            for sys, inf, faction, gov in matches:
                print("{:16} {} {:5.2f} {:5.2f} {}".format(sys.name[:16], gov.text[:4],
                                                           sys.dist_to(centre),
                                                           inf.influence, faction.name))

            break


def main():
    # write_report()
    feudal_finder()


if __name__ == "__main__":
    main()
