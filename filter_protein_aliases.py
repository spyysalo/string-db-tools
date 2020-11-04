#!/usr/bin/env python3

import sys

from collections import defaultdict
from argparse import ArgumentParser


# Prioritized list of sources to use to select aliases
SOURCE_PRIORITY = [
    'BLAST_UniProt_DE_RecName',
    'Ensembl_UniProt_DE_RecName',
]


TARGET_SOURCES = set(SOURCE_PRIORITY)


def argparser():
    ap = ArgumentParser()
    ap.add_argument('file', help='protein.aliases.v<VER>.txt file')
    return ap


def filter_protein_aliases(fn, options):
    filtered_aliases = defaultdict(list)
    with open(fn) as f:
        next(f)    # skip header line
        for ln, l in enumerate(f, start=1):
            l = l.rstrip('\n')
            protein_id, alias, sources = l.split('\t')
            for source in sources.split():
                if source in TARGET_SOURCES:
                    filtered_aliases[protein_id].append((alias, source))
    for protein_id, aliases_and_sources in filtered_aliases.items():
        if len(aliases_and_sources) < 2:
            continue
        sorted_aliases_and_sources = sorted(
            aliases_and_sources, key=lambda a_s: SOURCE_PRIORITY.index(a_s[1]))
        for alias, source in sorted_aliases_and_sources:
            print(f'{protein_id}\t{alias}\t{source}')
            break    # ignore all but first


def main(argv):
    args = argparser().parse_args(argv[1:])
    filter_protein_aliases(args.file, args)


if __name__ == '__main__':
    sys.exit(main(sys.argv))

