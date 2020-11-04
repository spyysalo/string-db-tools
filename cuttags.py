#!/usr/bin/env python3

"""Cut parts out of STRING DB all_matches.tsv format."""

import sys

from argparse import ArgumentParser
from logging import warning

from common import DocReader, SpanReader, open_file, safe_str


def argparser():
    ap = ArgumentParser()
    ap.add_argument('--char-offsets', default=False, action='store_true',
                    help='offsets are character- instead of byte-based')
    ap.add_argument('--cut', choices=['tiab'], default='tiab',
                    help='Which part of documents to cut')
    ap.add_argument('docs', help='documents in database_documents.tsv format')
    ap.add_argument('tags', help='tagged strings in all_matches.tsv format')
    ap.add_argument('out', help='output file for cut documents')
    return ap


def get_offset_map(doc, options):
    """Return mapping from original to cut document text offsets or None
    if the text is not cut."""
    if options.cut == 'tiab':
        # restrict to title and abstract
        sections = doc.text.split('\t')
        if len(sections) <= 2:
            return None
        else:
            tiab = '\t'.join(sections[:2])
            cut_text = doc.text[len(tiab):]
            offset_map = list(range(len(tiab))) + [None] * len(cut_text)
            return offset_map
    else:
        raise ValueError(options.cut)


def apply_offset_map(spans, offset_map):
    mapped_spans = []
    for span in spans:
        start, end = offset_map[span.start], offset_map[span.end]
        if start is not None and end is not None:
            # mapped span included in remaining text, map and retain
            span.start, span.end = start, end
            mapped_spans.append(span)
        elif start is None and end is None:
            pass    # span was cut entirely from text
        else:
            warning('span cut partially: {span}')
    return mapped_spans


def cut_tags(doc_fn, tag_fn, out_fn, options):
    removed, total = 0, 0
    with open_file(doc_fn, 'r', options) as doc_f:
        doc_reader = DocReader(doc_f)
        with open_file(tag_fn, 'r', options) as tag_f:
            span_reader = SpanReader(tag_f, no_type_mapping=True)
            with open_file(out_fn, 'w', options) as out_f:
                for doc_idx, doc in enumerate(doc_reader):
                    offset_map = get_offset_map(doc, options)
                    if offset_map is None:
                        # no-op, quick copy without parsing
                        for span in span_reader.document_lines(doc.id):
                            print(span, end='', file=out_f)
                            total += 1
                    else:
                        # need to parse, map and filter
                        spans = list(span_reader.document_spans(doc.id))
                        mapped = apply_offset_map(spans, offset_map)
                        removed += len(spans) - len(mapped)
                        total += len(spans)
                        for span in mapped:
                            print(span, file=out_f)
                    if (doc_idx+1) % 100000 == 0:
                        print(f'processed {doc_idx+1} documents',
                              file=sys.stderr)
    print(f'removed {removed}/{total} spans ({removed/total:.1%})',
          file=sys.stderr)


def main(argv):
    args = argparser().parse_args(argv[1:])
    cut_tags(args.docs, args.tags, args.out, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
