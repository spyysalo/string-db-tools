#!/usr/bin/env python3

"""
Select standoffs containing at least one new annotated textbound name.
"""

import sys

from random import shuffle
from collections import namedtuple
from argparse import ArgumentParser


Textbound = namedtuple('Textbound', 'id type start end text')


def argparser():
    ap = ArgumentParser()
    ap.add_argument('file', nargs='+')
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


def main(argv):
    args = argparser().parse_args(argv[1:])

    textbounds_by_path = {}
    for fn in args.file:
        textbounds_by_path[fn] = load_textbounds(fn, args)

    seen = set()
    shuffled_files = list(args.file)
    shuffle(shuffled_files)
    for fn in args.file:
        lc_texts = set(t.text.lower() for t in textbounds_by_path[fn])
        if any(t for t in lc_texts if t not in seen):
            print(fn)    # at least one new
        else:
            print('SKIP:', fn, lc_texts, seen)
        seen.update(lc_texts)
        
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
