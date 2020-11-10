#!/usr/bin/env python3

import sys
import random

from itertools import chain
from collections import defaultdict, Counter, OrderedDict
from argparse import ArgumentParser

from common import DocReader, SpanReader, unique


def argparser():
    ap = ArgumentParser()
    ap.add_argument('--overlap', default=False, action='store_true',
                    help='apply overlap matching (default: exact)')
    ap.add_argument('--char-offsets', default=False, action='store_true',
                    help='offsets are character- instead of byte-based')
    ap.add_argument('--seed', default=None, type=int,
                    help='random seed')
    ap.add_argument('--save-interval', default=None, type=int)
    ap.add_argument('--sample', default=None, type=float)
    ap.add_argument('--max-docs', default=None, type=int)
    ap.add_argument('--types', default=None)
    ap.add_argument('--output', default='comparison-results.txt')
    ap.add_argument('--names', default=None)
    ap.add_argument('--doc-output', default='comparison-docs.tsv')
    ap.add_argument('--tag-output', default='comparison-tags.tsv')
    ap.add_argument('docs', help='documents in database_documents.tsv format')
    ap.add_argument('tags', nargs='+',
                    help='tagged strings in all_matches.tsv format')
    return ap


def open_file(fn, mode, options):
    if options.char_offsets:
        return open(fn, mode)
    else:
        # https://www.python.org/dev/peps/pep-0383/ (Python 3.1+)
        return open(fn, mode, encoding='ascii', errors='surrogateescape')


def safe_str(string):
    # workaround for 'utf-8' codec can't encode [...]: surrogates not allowed
    return string.encode('utf-8', 'replace').decode()


class Stats:
    def __init__(self, sources):
        for i in range(len(sources)):
            for j in range(i+1, len(sources)):
                s = (sources[i], sources[j])
                self.tp_by_source = Counter({ s: 0 })
                self.fp_by_source = Counter({ s: 0 })
                self.fn_by_source = Counter({ s: 0 })
        self.tp_by_source_and_text = defaultdict(lambda: Counter())
        self.fp_by_source_and_text = defaultdict(lambda: Counter())
        self.fn_by_source_and_text = defaultdict(lambda: Counter())
        self.overlap_by_source_and_text = defaultdict(lambda: Counter())

    def add_stats(self, other):
        for s in other.sources():
            self.tp_by_source[s] += other.tp_by_source[s]
            self.fp_by_source[s] += other.fp_by_source[s]
            self.fn_by_source[s] += other.fn_by_source[s]
            for t in other.tp_by_source_and_text[s].keys():
                self.tp_by_source_and_text[s][t] += \
                    other.tp_by_source_and_text[s][t]
            for t in other.fp_by_source_and_text[s].keys():
                self.fp_by_source_and_text[s][t] += \
                    other.fp_by_source_and_text[s][t]
            for t in other.fn_by_source_and_text[s].keys():
                self.fn_by_source_and_text[s][t] += \
                    other.fn_by_source_and_text[s][t]
            for t in other.overlap_by_source_and_text[s].keys():
                self.overlap_by_source_and_text[s][t] += \
                    other.overlap_by_source_and_text[s][t]

    def add_tp(self, gold, pred, span):
        self.tp_by_source_and_text[(gold, pred)][span.text] += 1
        self.tp_by_source[(gold, pred)] += 1

    def add_fp(self, gold, pred, span):
        self.fp_by_source_and_text[(gold, pred)][span.text] += 1
        self.fp_by_source[(gold, pred)] += 1

    def add_fn(self, gold, pred, span):
        self.fn_by_source_and_text[(gold, pred)][span.text] += 1
        self.fn_by_source[(gold, pred)] += 1

    def add_overlap(self, gold, pred, span1, span2):
        texts = f'{span1.text}\t{span2.text}'
        self.overlap_by_source_and_text[(gold, pred)][texts] += 1

    def most_common_tp(self, sources, number=1000):
        return self.tp_by_source_and_text[sources].most_common(number)

    def most_common_fp(self, sources, number=1000):
        return self.fp_by_source_and_text[sources].most_common(number)

    def most_common_fn(self, sources, number=1000):
        return self.fn_by_source_and_text[sources].most_common(number)

    def most_common_overlaps(self, sources, number=1000):
        return self.overlap_by_source_and_text[sources].most_common(number)

    def sources(self):
        return set(list(self.tp_by_source.keys()) +
                   list(self.fp_by_source.keys()) +
                   list(self.fn_by_source.keys()))

    def total(self, sources):
        return (self.true_positive(sources) +
                self.false_positive(sources) +
                self.false_negative(sources))

    def unique_texts(self, sources):
        return unique(chain(self.tp_by_source_and_text[sources].keys(),
                            self.fp_by_source_and_text[sources].keys(),
                            self.fn_by_source_and_text[sources].keys()))

    def true_positive(self, sources):
        return self.tp_by_source[sources]

    def false_positive(self, sources):
        return self.fp_by_source[sources]

    def false_negative(self, sources):
        return self.fn_by_source[sources]

    def precision(self, sources):
        tp = self.true_positive(sources)
        fp = self.false_positive(sources)
        return tp/(tp+fp) if tp+fp else 0

    def recall(self, sources):
        tp = self.true_positive(sources)
        fn = self.false_negative(sources)
        return tp/(tp+fn) if tp+fn else 0

    def f_score(self, sources):
        prec, rec = self.precision(sources), self.recall(sources)
        return 2*prec*rec/(prec+rec) if prec+rec else 0

    def trim(self, ratio=10000):
        """Remove rare items for memory and computational efficiency."""
        trimmed, total = 0, 0
        for sources in self.sources():
            for s in (self.tp_by_source_and_text[sources],
                      self.fp_by_source_and_text[sources],
                      self.fn_by_source_and_text[sources],
                      self.overlap_by_source_and_text[sources]):
                try:
                    max_count = s.most_common(1)[0][1]
                except IndexError:
                    continue
                for k, v in list(s.items()):
                    if v * ratio < max_count:
                        trimmed += 1
                        del s[k]
                    total += 1
        print(f'trimmed {trimmed}/{total} ({trimmed/total:.1%})',
              file=sys.stderr, flush=True)


def validate_spans(doc_id, doc_text, spans):
    validated = []
    for span in spans:
        doc_span_text = doc_text[span.start:span.end+1]
        if doc_span_text != span.text:
            dt, st = safe_str(doc_span_text), safe_str(span.text)
            print(f'text mismatch in {doc_id}: "{dt}" vs "{st}"',
                  file=sys.stderr, flush=True)
        else:
            validated.append(span)
    return validated


def filter_spans(spans, options):
    """Filter spans to types specified in options (if any)."""
    if not options.types:
        return spans
    else:
        return [s for s in spans if s.type.lower() in options.types]


def deduplicate_spans(spans, options):
    """Combine serials for spans with identical boundaries and types."""
    make_key = lambda s: (s.start, s.end, s.type)
    span_map = defaultdict(list)
    deduped = []
    for span in spans:
        key = make_key(span)
        if key not in span_map:
            deduped.append(span)
        else:
            span_map[key][0].serials.extend(span.serials)
        span_map[key].append(span)
    return deduped


def compare_document_spans(doc_id, source1, source2, spans1, spans2, options):
    # Avoiding O(n^2) comparison: create list of (offset, start/end,
    # span), sort with end<start, and then iterate over the list while
    # maintaining a list of currently open.
    stats = Stats([source1, source2])
    START, END = 's', 'e'    # need END < START for sort to work right
    boundaries = []
    for s in chain(spans1, spans2):
        boundaries.append((s.start, START, s))
        boundaries.append((s.end+1, END, s))    # +1 for end-exclusive
    boundaries.sort()
    
    span_source = {}
    for s in spans1:
        span_source[s] = source1
    for s in spans2:
        span_source[s] = source2

    open_spans = OrderedDict()    # Used as ordered set
    matched_s1, matched_s2 = set(), set()
    for offset, boundary, span in boundaries:
        if boundary == START:    # overlaps with everything currently open
            for other in open_spans.keys():
                if span_source[span] != span_source[other]:
                    # overlapping spans in different sources; compare
                    if span_source[span] == source1:
                        s1, s2 = span, other    # fix order
                    else:
                        assert span_source[span] == source2
                        s1, s2 = other, span    # fix order
                    if (s1.matches(s2) or
                        (options.overlap and s1.overlap_matches(s2))):
                        matched_s1.add(s1)
                        matched_s2.add(s2)
                        stats.add_tp(source1, source2, s1)
                        stats.add_tp(source2, source1, s2)
                    elif s1.overlap_matches(s2):
                        stats.add_overlap(source1, source2, s1, s2)
            open_spans[span] = True
        else:
            assert boundary == END
            del open_spans[span]

    unmatched_s1 = set(spans1) - matched_s1
    unmatched_s2 = set(spans2) - matched_s2
    for s1 in unmatched_s1:
        stats.add_fp(source2, source1, s1)
        stats.add_fn(source1, source2, s1)
    for s2 in unmatched_s2:
        stats.add_fp(source1, source2, s2)
        stats.add_fn(source2, source1, s2)
    return stats

    
def save_results(path, stats, options):
    with open_file(path, 'w', options) as out:
        for sources in sorted(stats.sources()):
            print(f'GOLD: {sources[0]}, PRED: {sources[1]}', file=out)
            tp = stats.true_positive(sources)
            fp = stats.false_positive(sources)
            fn = stats.false_negative(sources)
            prec = stats.precision(sources)
            rec = stats.recall(sources)
            f = stats.f_score(sources)
            print(f'FP: {tp} FP: {fp} FN: {fn} '
                  f'prec: {prec:.1%} rec: {rec:.1%} fscore: {f:.1%}', 
                  file=out)
            for text, count in stats.most_common_tp(sources):
                print(f'TP:\t{count}\t{text}', file=out)
            for text, count in stats.most_common_fp(sources):
                print(f'FP:\t{count}\t{text}', file=out)
            for text, count in stats.most_common_fn(sources):
                print(f'FN:\t{count}\t{text}', file=out)
            for text, count in stats.most_common_overlaps(sources):
                print(f'OVERLAP:\t{count}\t{text}', file=out)
    print(f'saved results in {path}', file=sys.stderr, flush=True)


def select_document_for_output(doc, doc_stats, options):
    """Return whether to include document in output."""
    sources = sorted(doc_stats.sources())[0]    # arbitrary but fixed
    tagged_per_100_words = 100 * doc_stats.total(sources) / len(doc.text)
    unique_lowercase = set(t.lower() for t in doc_stats.unique_texts(sources))
    # if len(unique_lowercase) > 1 and tagged_per_100_words > 1:
    #     print(f'tagged/100 words: {tagged_per_100_words:.1f}')
    #     print('unique (lower)', unique_lowercase)
    if doc_stats.total(sources) < 1:
        # Exclude documents with too few annotations
        return False
    if len(unique_lowercase) < 2:
        # Exclude documents with too few unique mentions
        return False
    if tagged_per_100_words < 1:
        # Exclude documents with too low annotation density
        return False
    # elif doc_stats.total(sources) > 100:
    #     # Exclude documents with too many annotations
    #     return False
    elif doc_stats.f_score(sources) >= 1.0:
        # Exclude documents with too high agreement
        return False
    elif doc_stats.f_score(sources) <= 0.5:
        # Exclude documents with too low agreement
        return False
    else:
        return True


def compare_spans(doc_fn, tag_fns, names, doc_out, tag_out, options):
    if names is None:
        names = tag_fns
    doc_count = 0
    stats = Stats(names)
    with open_file(doc_fn, 'r', options) as doc_f:
        doc_reader = DocReader(doc_f)
        tag_fs = []
        for tag_fn in tag_fns:
            tag_fs.append(open_file(tag_fn, 'r', options))
        span_readers = [
            SpanReader(tag_f, source=name) 
            for tag_f, name in zip(tag_fs, names)
        ]
        for doc_idx, doc in enumerate(doc_reader):
            if options.max_docs and doc_count >= options.max_docs:
                break
            spans = [r.document_spans(doc.id) for r in span_readers]
            spans = [validate_spans(doc.id, doc.text, s) for s in spans]
            spans = [filter_spans(s, options) for s in spans]
            spans = [deduplicate_spans(s, options) for s in spans]
            selected_for_output = False
            for i in range(len(spans)):
                for j in range(i+1, len(spans)):
                    doc_stats = compare_document_spans(
                        doc.id, names[i], names[j], spans[i], spans[j],
                        options)
                    stats.add_stats(doc_stats)
                    if select_document_for_output(doc, doc_stats, options):
                        selected_for_output = True

            if (selected_for_output and
                (options.sample is None or random.random() < options.sample)):
                print(doc, file=doc_out)
                for s in (s for sp in spans for s in sp):
                    print(s, file=tag_out)

            doc_count += 1
            if doc_count % 10000 == 0:
                print(f'processed {doc_count} documents', file=sys.stderr,
                      flush=True)
            if (options.save_interval and 
                doc_count % options.save_interval == 0):
                save_results(options.output, stats, options)
                doc_out.flush()
                tag_out.flush()
                stats.trim()

    save_results(options.output, stats, options)


def main(argv):
    args = argparser().parse_args(argv[1:])
    if args.names is not None:
        args.names = args.names.split(',')
        if len(args.tags) != len(args.names):
            raise ValueError('number of names != number of tag inputs')
    if args.types is not None:
        args.types = set(t.lower() for t in args.types.split(','))
    random.seed(args.seed)

    with open_file(args.doc_output, 'w', args) as doc_out:
        with open_file(args.tag_output, 'w', args) as tag_out:
            compare_spans(args.docs, args.tags, args.names, doc_out, tag_out,
                          args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
