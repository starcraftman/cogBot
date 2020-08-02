"""
Reprocess a json file into a csv for import into a google sheet.
"""
import argparse
import json
import csv
import collections


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
        writer = csv.DictWriter(csv_file, json_content[0].keys())
        writer.writeheader()
        writer.writerows(json_content)

    print("Finished writing csvs to %s" % file_out)


def main():
    """
    The main entry.
    """
    args = make_parser().parse_args()
    generate_csv(args.input, args.output)


if __name__ == "__main__":
    main()
