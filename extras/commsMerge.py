#!/usr/bin/env python
"""
Tool to merge captured comms list and the existing SCommodity entries.
There are missing names from capture, need to run the tool again when name list more complete.
"""
import json
import textdistance


def search_names(needle, names):
    """
    Substring search names based on user input.

    Args:
        needle: The original search term user provided.
        names: The haystack to search in.

    Returns: None if nothing selected otherwise a choice from names.
    """
    while True:
        matches = [x for x in names if needle in x]
        for ind, name in enumerate(matches):
            print(f"{ind}) {name}")

        text = input("Select an index from these, loop again with any string or cancel with 'no': ")
        try:
            index = int(text)
            if index < 0 or index > len(matches):
                raise ValueError
            return matches[index]
        except ValueError:
            if text.lower() == "no":
                return None

        needle = input("Type a substring to search for in candidates: ")


def main():
    """
    Main function that associates found eddn_names captured with existing
    names in the data/preload/SCommodity.json file.
    Goal is at end of process, all entries in original file should have an eddn field.
    Try to guess matches via hamming distance, otherwise fall back to user search.
    """
    eddn_names = []
    with open('./commsMiss.txt', 'r', encoding='utf-8') as fin:
        for ind, line in enumerate(fin):
            eddn_names += [line.split(' ')[-1].strip()]

    with open("data/preload/SCommodity.json", 'r', encoding='utf-8') as fin:
        comms = json.load(fin)
        for comm in comms:
            name = comm['name']

            dist_map = {x: textdistance.hamming(name.replace(' ', ''), x) for x in eddn_names}
            candidates = list(sorted(eddn_names, key=lambda x: dist_map[x]))
            print(f"Looking at distance for: {name}")
            for ind, cand in enumerate(candidates[:10]):
                print(f"{ind}) {cand}: {dist_map[cand]}")

            msg = """If any of these suitable, type the number.
If not, type part of the word to search for substring match: """
            text = input(msg)
            try:
                index = int(text)
                comm['eddn'] = candidates[index]
            except ValueError:
                print("\nResorting to substring search.")
                comm['eddn'] = search_names(text, eddn_names)

            if comm['eddn']:
                eddn_names.remove(comm['eddn'])
            print(f"Final entry: {name}")
            print(comm)
            print()

    with open("/tmp/SModule.json", 'w', encoding='utf-8') as fout:
        json.dump(comms, fout, indent=2, sort_keys=True)


if __name__ == "__main__":
    main()
