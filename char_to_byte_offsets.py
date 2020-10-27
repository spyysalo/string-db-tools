#!/usr/bin/env python3

"""
Map character to byte offsets in STRING DB all_matches.tsv format.
"""

import sys

from argparse import ArgumentParser

from common import DocReader, SpanReader


def argparser():
    ap = ArgumentParser()
    ap.add_argument('--encoding', default='utf-8', action='store_true',
                    help='input encoding')
    ap.add_argument('docs', help='documents in database_documents.tsv format')
    ap.add_argument('tags', help='tagged strings in all_matches.tsv format')
    return ap


def make_offset_map(text, options):
    offsets, offset = [], 0
    for char in text:
        offsets.append(offset)
        # TODO: consider https://stackoverflow.com/a/55201398
        offset += len(char.encode(options.encoding))
    offsets.append(offset)
    return dict(enumerate(offsets))


def char_to_byte_offsets(doc_fn, tag_fn, options):
    doc_count = 0
    with open(doc_fn, encoding=options.encoding) as doc_f:
        doc_reader = DocReader(doc_f)
        with open(tag_fn, encoding=options.encoding) as tag_f:
            span_reader = SpanReader(tag_f)
            for doc in doc_reader:
                offset_map = make_offset_map(doc.text, options)
                for span in span_reader.document_spans(doc.id):
                    span.start = offset_map[span.start]
                    span.end = offset_map[span.end]    # end inclusive
                    print(span)
                doc_count += 1
                if doc_count % 10000 == 0:
                    print(f'processed {doc_count} documents', file=sys.stderr)

def main(argv):
    args = argparser().parse_args(argv[1:])
    char_to_byte_offsets(args.docs, args.tags, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
