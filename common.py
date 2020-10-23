#!/usr/bin/env python3

# Common functionality for working with STRING DB / JensenLab tagger data

import sys

from collections.abc import Iterator
from itertools import tee
from collections import namedtuple
from itertools import zip_longest


StringDocument = namedtuple(
    'StringDocument',
    'id, other_ids, authors, forum, year, text'
)


StringSpan = namedtuple(
    'StringSpan',
    'doc_id, par_num, sent_num, start, end, text, type_id, serial'
)


class LookaheadIterator(Iterator):
    """Lookahead iterator from http://stackoverflow.com/a/1518097."""

    def __init__(self, it, start=0):
        self._it, self._nextit = tee(iter(it))
        self.index = start - 1
        self._advance()

    def _advance(self):
        self.lookahead = next(self._nextit, None)
        self.index = self.index + 1

    def __next__(self):
        self._advance()
        return next(self._it)

    def __bool__(self):
        return self.lookahead is not None


class SpanReader:
    """Reader for all_matches.tsv format."""

    def __init__(self, stream, raise_on_error=False):
        self.stream = stream
        self.raise_on_error = raise_on_error
        self.iter = LookaheadIterator(stream, start=1)
        self.errors = 0

    def current_doc_id(self):
        """Return id of document at the current position of the stream."""
        if self.iter.lookahead is None:
            return None
        else:
            return self.iter.lookahead.split()[0]

    def document_spans(self, doc_id):
        """Return spans for document doc_id and advance past them.

        If doc_id does not match the current position of the stream, returns
        an empty list without advancing in the stream.
        """
        spans = []
        while self.current_doc_id() == doc_id:
            try:
                line = self.iter.lookahead.rstrip('\n')
                span = parse_stringdb_span_line(line)
                spans.append(span)
            except Exception as e:
                self.errors += 1
                print(f'error parsing {self.stream.name} line '
                      f'{self.iter.index}: {e}: {line}', file=sys.stderr)
                if self.raise_on_error:
                    raise
            next(self.iter)
        return spans


def stringdb_unescape_text(text):
    """Unescape text field in database_documents.tsv format."""
    unescaped = []
    pair_iter = zip_longest(text, text[1:])
    for char, next_ in pair_iter:
        if char == '\\' and next_ == '\\':
            # Double backslash -> single backslash
            unescaped.append('\\')
            next(pair_iter)
        elif char == '\\' and next_ == 't':
            # Backslash + t -> tab character
            unescaped.append('\t')
            next(pair_iter)
        else:
            unescaped.append(char)
    return ''.join(unescaped)


def parse_stringdb_input_line(line):
    """Parse line in database_documents.tsv format, return StringDocument."""
    line = line.rstrip('\n')
    fields = line.split('\t')
    doc_id, other_ids, authors, forum, year, text = fields
    text = stringdb_unescape_text(text)
    return StringDocument(doc_id, other_ids, authors, forum, year, text)


def parse_stringdb_span_line(line):
    """Parse line in all_matches.tsv format, return StringSpan."""
    line = line.rstrip('\n')
    fields = line.split('\t')
    doc_id, par_num, sent_num, start, end, text, type_id, serial = fields
    start, end = int(start), int(end)
    return StringSpan(
        doc_id, par_num, sent_num, start, end, text, type_id, serial)


def stream_documents(fn):
    with open(fn) as f:
        for ln, l in enumerate(f, start=1):
            try:
                document = parse_stringdb_input_line(l)
            except Exception as e:
                raise ValueError('failed to parse {} line {}'.format(fn, ln))
            yield document


if __name__ == '__main__':
    import sys

    # Test I/O
    for fn in sys.argv[1:]:
        for doc in stream_documents(fn):
            print(doc.doc_id, len(doc.text.split()), 'tokens')
