#!/usr/bin/env python3

"""
Select standoffs containing at least one new annotated textbound name.
"""

import sys
import os

from glob import glob
from random import shuffle
from collections import namedtuple, Counter
from argparse import ArgumentParser


Textbound = namedtuple('Textbound', 'id type start end text')


def argparser():
    ap = ArgumentParser()
    ap.add_argument('dir')
    return ap


def load_textbounds(fn, options):
    textbounds = []
    with open(fn) as f:
        for ln, l in enumerate(f, start=1):
            l = l.rstrip('\n')
            if l.startswith('T'):
                fields = l.split('\t')
                id_, type_span, text = fields
                type_, start, end = type_span.split()
                textbounds.append(Textbound(id_, type_, start, end, text))
    return textbounds


def common_name_ratio(overall_counts, names):
    common_names = set([n for n, c in overall_counts.items() if c > 10])
    return len([t for t in names if t in common_names]) / len(names)


def known_name_ratio(overall_counts, names):
    known_names = set(overall_counts.keys())
    return len([t for t in names if t in known_names]) / len(names)


def main(argv):
    args = argparser().parse_args(argv[1:])

    files = glob(os.path.join(args.dir, '*.ann'))

    textbounds_by_path = {}
    for fn in files:
        textbounds_by_path[fn] = load_textbounds(fn, args)

    seen, counts = set(), Counter()
    shuffled_files = list(files)
    shuffle(shuffled_files)
    for fn in shuffled_files:
        lc_names = [t.text.lower() for t in textbounds_by_path[fn]]
        unseen_lc_names = set(lc_names) - seen
        if len(unseen_lc_names) < 1:
            print('SKIP1:', fn, lc_names, file=sys.stderr)
        elif common_name_ratio(counts, lc_names) > 0.5:
            print('SKIP2:', fn, lc_names, file=sys.stderr)
        elif known_name_ratio(counts, lc_names) > 0.5:
            print('SKIP3:', fn, lc_names, file=sys.stderr)
        else:
            print(fn)    # OK
            seen.update(lc_names)
            counts.update(lc_names)
    print(counts.most_common(100), file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
