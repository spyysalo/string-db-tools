#!/usr/bin/env python3

import sys

from argparse import ArgumentParser

from common import SpanReader
from common import parse_stringdb_input_line, parse_stringdb_span_line


def argparser():
    ap = ArgumentParser()
    ap.add_argument('docs', help='documents in database_documents.tsv format')
    ap.add_argument('tags', help='tagged string in all_matches.tsv format')
    return ap


def check_spans(doc_fn, tag_fn, options):
    mismatches = 0
    with open(doc_fn) as doc_f:
        with open(tag_fn) as tag_f:
            span_reader = SpanReader(tag_f)
            for doc_ln, doc_l in enumerate(doc_f, start=1):
                try:
                    doc = parse_stringdb_input_line(doc_l)
                except:
                    raise ValueError('error parsing {} line {}: {}'.format(
                        doc_fn, doc_ln, doc_l.rstrip('\n')))

                for span in span_reader.document_spans(doc.id):
                    doc_span_text = doc.text[span.start:span.end+1]
                    if doc_span_text != span.text:
                        print('text mismatch in {}: "{}" vs "{}"'.format(
                            doc.id, doc_span_text, span.text))
                        mismatches += 1

            span_count, errors = span_reader.iter.index-1, span_reader.errors
            if span_reader.current_doc_id() is not None:
                print(f'ERROR: extra lines in {tag_fn}')
            if mismatches or errors:
                print(f'Checked {span_count} spans, found {errors} errors '
                      f'and {mismatches} mismatches')
            else:
                print(f'OK, checked {span_count} spans')


def main(argv):
    args = argparser().parse_args(argv[1:])
    check_spans(args.docs, args.tags, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
