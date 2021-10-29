"""
Exists to unwind a circular import between cog.exc and cog.util.
"""
DUMMY_ATTRIBUTE = "zzzzz"  # I don't expect this attribute anywhere.


def substr_ind(seq, line, *, skip_spaces=False):
    """Find the substring indexes (start, end) of a given sequence in a line.

    If you wish to ignore case, lower case before calling.

    Args:
        seq: A sequence of characters to look for in line, may contain spaces.
        line: A line of text to scan accross.
        skip_spaces: If true, ignore spaces when matching.
    """
    if skip_spaces:
        seq = seq.replace(' ', '')

    if len(line) < len(seq):
        return []

    start = None
    count = 0
    num_required = len(seq)
    for ind, char in enumerate(line):
        if skip_spaces and char == ' ':
            continue

        if char == seq[count]:
            if count == 0:
                start = ind
            count += 1
        else:
            count = 0
            start = None

        if count == num_required:
            return [start, ind + 1]

    return []
