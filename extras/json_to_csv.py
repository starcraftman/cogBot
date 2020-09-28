"""
Reprocess a json file into a csv for import into a google sheet.
Only works for systems_populated.json now. Prune uneeded columns from data.
"""
import argparse
import collections
import datetime
import json

HEADER = "SYSTEM, SECURITY, POPULATION, GOVERNMENT, POWER, POWER STATE, X, Y, Z, Created (UTC): {}\n"
FMT = "{name},{security},{population},{government},{power},{power_state},{x},{y},{z}\n"


def make_parser():
    """
    Make a simple arguement parser for script.
    """
    parser = argparse.ArgumentParser(prog='json_to_csv.py', description='Convert a json to csv.')
    parser.add_argument('input', help='The the input json file to convert.')
    parser.add_argument('output', help='The path to a file to write out to.')

    return parser


def generate_csv(file_in, file_out):
    """
    Generate the required CSV file based on an input json.

    Args:
        file_in: Any valid file that is a json.
        file_out: A valid path to write to.
    """
    with open(file_in, 'rb') as json_file:
        json_content = json.load(
            json_file,
            object_pairs_hook=collections.OrderedDict)

    print("Beginning to write contents of %s to %s" % (file_in, file_out))
    with open(file_out, 'w', newline='') as csv_file:
        csv_file.write(HEADER.format(datetime.datetime.now(datetime.timezone.utc)))
        for row in json_content:
            csv_file.write(FMT.format(
                name=row['name'],
                security=row['security'],
                population=row['population'],
                government=row['government'],
                power=row['power'] if row['power'] else 0,
                power_state=row['power_state'] if row['power_state'] else 0,
                x=row['x'],
                y=row['y'],
                z=row['z']))

    print("Finished writing csvs to %s" % file_out)


def main():
    """
    The main entry.
    """
    args = make_parser().parse_args()
    generate_csv(args.input, args.output)


if __name__ == "__main__":
    main()
