#!/usr/bin/env python3

import sys

from argparse import ArgumentParser

from common import DocReader, SpanReader, open_file, safe_str


def argparser():
    ap = ArgumentParser()
    ap.add_argument('--char-offsets', default=False, action='store_true',
                    help='offsets are character- instead of byte-based')
    ap.add_argument('docs', help='documents in database_documents.tsv format')
    ap.add_argument('tags', help='tagged strings in all_matches.tsv format')
    return ap


def check_spans(doc_fn, tag_fn, options):
    doc_count, span_count, mismatches = 0, 0, 0
    with open_file(doc_fn, 'r', options) as doc_f:
        doc_reader = DocReader(doc_f)
        with open_file(tag_fn, 'r', options) as tag_f:
            span_reader = SpanReader(tag_f)
            for doc in doc_reader:
                for span in span_reader.document_spans(doc.id):
                    doc_span_text = doc.text[span.start:span.end+1]
                    if doc_span_text != span.text:
                        dt, st = safe_str(doc_span_text), safe_str(span.text)
                        print(f'text mismatch in {doc.id}: "{dt}" '
                              f'vs "{st}: {span}"')
                        mismatches += 1
                    span_count += 1
                doc_count += 1
                if doc_count % 10000 == 0:
                    print(f'processed {doc_count} documents '
                          f'({span_count} spans)', file=sys.stderr)
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
