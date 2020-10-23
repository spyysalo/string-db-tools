#!/usr/bin/env python3

import sys

from argparse import ArgumentParser

from common import LookaheadIterator
from common import parse_stringdb_input_line, parse_stringdb_span_line


def argparser():
    ap = ArgumentParser()
    ap.add_argument('docs', help='documents in database_documents.tsv format')
    ap.add_argument('tags', help='tagged string in all_matches.tsv format')
    return ap


def check_spans(doc_fn, tag_fn, options):
    tag_ln, mismatches, errors = 1, 0, 0
    with open(doc_fn) as doc_f:
        with open(tag_fn) as tag_f:
            tag_iter = LookaheadIterator(tag_f)
            for doc_ln, doc_l in enumerate(doc_f, start=1):
                try:
                    doc = parse_stringdb_input_line(doc_l)
                except:
                    raise ValueError('error parsing {} line {}: {}'.format(
                        doc_fn, doc_ln, doc_l.rstrip('\n')))
                if tag_iter.lookahead is None:
                    continue

                while tag_iter.lookahead is not None:
                    tag_l = tag_iter.lookahead
                    try:
                        span = parse_stringdb_span_line(tag_l)
                    except Exception as e:
                        errors += 1
                        print('error parsing {} line {}: {}: {}'.format(
                            tag_fn, tag_ln, e, tag_l.rstrip('\n')))
                        next(tag_iter)
                        tag_ln += 1
                        continue

                    if doc.doc_id != span.doc_id:
                        # Assume both files are in document order and that
                        # ID mismatch indicates this span is for the next
                        # document.
                        break

                    doc_span_text = doc.text[span.start:span.end+1]
                    if doc_span_text != span.text:
                        print('text mismatch in {}: "{}" vs "{}"'.format(
                            doc.doc_id, doc_span_text, span.text))
                        mismatches += 1

                    next(tag_iter)
                    tag_ln += 1

            if tag_iter.lookahead is not None:
                print('ERROR: extra lines in {}'.format(tag_fn))
            if mismatches or errors:
                print('Checked {} spans, found {} errors and {} mismatches'.\
                      format(tag_ln-1, errors, mismatches))
            else:
                print('OK, checked {} spans'.format(tag_ln-1))


def main(argv):
    args = argparser().parse_args(argv[1:])
    check_spans(args.docs, args.tags, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
