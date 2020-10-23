#!/usr/bin/env python3

# Common functionality for working with STRING DB / JensenLab tagger data

from collections.abc import Iterator
from itertools import tee
from collections import namedtuple
from itertools import zip_longest


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


StringDocument = namedtuple(
    'StringDocument',
    'doc_id, other_ids, authors, forum, year, text'
)


StringSpan = namedtuple(
    'StringSpan',
    'doc_id, par_num, sent_num, start, end, text, type_id, serial'
)


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
