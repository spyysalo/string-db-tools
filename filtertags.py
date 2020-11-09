#!/usr/bin/env python3

"""Filter STRING DB all_matches.tsv format to given IDs."""

import sys

from argparse import ArgumentParser

from common import open_file, load_ids


def argparser():
    ap = ArgumentParser()
    ap.add_argument('--char-offsets', default=False, action='store_true',
                    help='offsets are character- instead of byte-based')
    ap.add_argument('tags', help='tags in all_matches.tsv format')
    ap.add_argument('ids', help='text file with document IDs')
    ap.add_argument('out', help='output file')
    return ap


def filter_tags(tag_fn, out_fn, ids, options):
    out_count = 0
    with open_file(tag_fn, 'r', options) as tag_f:
        with open_file(out_fn, 'w', options) as out_f:
            for ln, line in enumerate(tag_f, start=1):
                id_ = line.split('\t')[0]
                if id_ in ids:
                    print(line, file=out_f, end='')
                    out_count += 1
                if ln % 100000 == 0:
                    print(f'processed {ln}, output {out_count}',
                          file=sys.stderr)
    print(f'output {out_count}/{ln} lines ({out_count/ln:.1%})',
          file=sys.stderr)


def main(argv):
    args = argparser().parse_args(argv[1:])
    ids = load_ids(args.ids, args)
    filter_tags(args.tags, args.out, ids, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
