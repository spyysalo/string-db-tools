#!/usr/bin/env python3

import sys
import os
import re

from logging import error
from argparse import ArgumentParser

from common import stringdb_escape_text


def argparser():
    ap = ArgumentParser()
    ap.add_argument('--force', action='store_true')
    ap.add_argument('txt', nargs='+', help='plain text file(s)')
    return ap


def is_regular_id(id_):
    return re.match(r'^[0-9]+$', id_) is not None


def escape_text(text):
    text = text.replace('\n', '\t')
    text = stringdb_escape_text(text)
    return text


def main(argv):
    args = argparser().parse_args()

    for fn in args.txt:
        with open(fn) as f:
            text = f.read().rstrip()

        id_ = os.path.splitext(os.path.basename(fn))[0]

        if not is_regular_id(id_) and not args.force:
            error(f'unexpected filename {fn} (consider --force?)')
            return -1

        print('\t'.join([
            id_,
            f'PMID:{id_}',
            'AUTHORS',
            'FORUM',
            'YEAR',
            escape_text(text)
        ]))


if __name__ == '__main__':
    sys.exit(main(sys.argv))
