"""
Simple parser for Frontier's Player Journal written out while playing Elite: Dangerous.

Provide ability to read, parse and insert events into the database.

Reference:
    https://edcodex.info/?m=doc
"""
import json
import logging


def load_journal_possible(fname):
    """
    Load an existing json file on server and then parse the lines to validate them.
    Any lines that fail to be validated will be ignored.

    Returns JSON objects to easily use.
    """
    json_objs = []
    with open(fname, 'r', encoding='utf-8') as fin:
        lines = [x for x in fin.read().split('\n') if x]

        for line in lines:
            try:
                json_objs += [json.loads(line)]
            except json.decoder.JSONDecodeError:
                logging.getLogger(__name__).error("Failed to parse player journal line: {line}")

    return json_objs


def main():
    pass


if __name__ == "__main__":
    main()
