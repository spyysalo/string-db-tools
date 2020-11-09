#!/usr/bin/env python3

"""Filter STRING DB database_documents.tsv format to given IDs."""

import sys

from argparse import ArgumentParser

from common import DocReader, open_file, load_ids


def argparser():
    ap = ArgumentParser()
    ap.add_argument('--char-offsets', default=False, action='store_true',
                    help='offsets are character- instead of byte-based')
    ap.add_argument('docs', help='documents in database_documents.tsv format')
    ap.add_argument('ids', help='text file with document IDs')
    ap.add_argument('out', help='output file')
    return ap


def filter_documents(doc_fn, out_fn, ids, options):
    out_count = 0
    with open_file(doc_fn, 'r', options) as doc_f:
        doc_reader = DocReader(doc_f)
        with open_file(out_fn, 'w', options) as out_f:
            for doc_idx, doc in enumerate(doc_reader):
                if doc.id in ids:
                    print(doc, file=out_f, flush=True)
                    out_count += 1
                if (doc_idx+1) % 100000 == 0:
                    print(f'processed {doc_idx+1}, output {out_count}',
                          file=sys.stderr)
    print(f'output {out_count}/{doc_idx} documents ({out_count/doc_idx:.1%})',
          file=sys.stderr)


def main(argv):
    args = argparser().parse_args(argv[1:])
    ids = load_ids(args.ids, args)
    filter_documents(args.docs, args.out, ids, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
