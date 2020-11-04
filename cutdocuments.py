#!/usr/bin/env python3

"""Cut out of STRING DB database_documents.tsv format."""

import sys

from argparse import ArgumentParser

from common import DocReader, open_file, safe_str


def argparser():
    ap = ArgumentParser()
    ap.add_argument('--char-offsets', default=False, action='store_true',
                    help='offsets are character- instead of byte-based')
    ap.add_argument('--cut', choices=['tiab'], default='tiab',
                    help='Which part of documents to cut')
    ap.add_argument('docs', help='documents in database_documents.tsv format')
    ap.add_argument('out', help='output file for cut documents')
    return ap


def cut_document(doc, options):
    if options.cut == 'tiab':
        # restrict to title and abstract
        sections = doc.text.split('\t')
        if len(sections) > 2:
            doc.text = '\t'.join(sections[:2])
            return True
    return False


def cut_documents(doc_fn, out_fn, options):
    cut_count = 0
    with open_file(doc_fn, 'r', options) as doc_f:
        doc_reader = DocReader(doc_f)
        with open_file(out_fn, 'w', options) as out_f:
            for doc_idx, doc in enumerate(doc_reader):
                cut_count += cut_document(doc, options)
                print(doc, file=out_f)
    print(f'cut {cut_count}/{doc_idx} documents ({cut_count/doc_idx:.1%})',
          file=sys.stderr)


def main(argv):
    args = argparser().parse_args(argv[1:])
    cut_documents(args.docs, args.out, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
