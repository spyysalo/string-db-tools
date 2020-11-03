#!/usr/bin/env python3

import sys
import os

from collections import defaultdict
from argparse import ArgumentParser

from common import DocReader, SpanReader


def argparser():
    ap = ArgumentParser()
    ap.add_argument('--char-offsets', default=False, action='store_true',
                    help='offsets are character- instead of byte-based')
    ap.add_argument('docs', help='documents in database_documents.tsv format')
    ap.add_argument('tags', help='tagged strings in all_matches.tsv format')
    ap.add_argument('dir', help='output directory')
    return ap


def open_file(fn, options):
    if not options.char_offsets:
        # https://www.python.org/dev/peps/pep-0383/ (Python 3.1+)
        return open(fn, encoding='ascii', errors='surrogateescape')
    else:
        return open(fn)


def normalize_type(type_):
    type_ = type_.lower()
    type_ = type_[0].upper() + type_[1:]
    return type_


def deduplicate_spans(spans, options):
    """Combine serials and sources for spans with identical boundaries and
    types."""
    make_key = lambda s: (s.start, s.end, s.type)
    span_map = defaultdict(list)
    deduped = []
    for span in spans:
        key = make_key(span)
        span.sources = set([span.source])
        if key not in span_map:
            deduped.append(span)
        else:
            span_map[key][0].serials.extend(span.serials)
            span_map[key][0].sources.update(span.sources)
        span_map[key].append(span)
    for span in spans:
        span.source = ','.join(sorted(span.sources))
    return deduped


def convert_to_standoff(doc_fn, tag_fn, out_dir, options):
    NOTE_TYPE = 'AnnotatorNotes'
    with open_file(doc_fn, options) as doc_f:
        doc_reader = DocReader(doc_f)
        with open_file(tag_fn, options) as tag_f:
            # Read spans that include source information
            span_reader = SpanReader(tag_f, source=True)
            for doc in doc_reader:
                spans = list(span_reader.document_spans(doc.id))
                for span in spans:
                    span.type = normalize_type(span.type)
                spans = deduplicate_spans(spans, options)
                with open(os.path.join(out_dir, f'{doc.id}.txt'), 'w') as f:
                    print(doc.text.replace('\t', '\n'), file=f)
                with open(os.path.join(out_dir, f'{doc.id}.ann'), 'w') as f:
                    for i, span in enumerate(spans, start=1):
                        s, e = span.start, span.end+1    # end-exclusive
                        if len(span.sources) == 2:    # assume two sources
                            t = f'{span.type}'
                        else:
                            t = f'{span.type}-{span.source}'
                        print(f'T{i}\t{t} {s} {e}\t{span.text}', file=f)
                        

def main(argv):
    args = argparser().parse_args(argv[1:])
    convert_to_standoff(args.docs, args.tags, args.dir, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))

