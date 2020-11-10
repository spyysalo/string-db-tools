#!/usr/bin/env python3

import sys

from argparse import ArgumentParser


def argparser():
    ap = ArgumentParser()
    ap.add_argument('types', help='TSV with TEXT TYPE' )
    ap.add_argument('ann', nargs='+', help='standoff annotation')
    return ap


def retype_standoff(fn, type_map, options):
    with open(fn) as f:
        for ln, line in enumerate(f, start=1):
            line = line.rstrip('\n')
            if not line.startswith('T'):
                print(line)    # echo non-textbounds
            else:
                id_, type_span, text = line.split('\t')
                type_, span = type_span.split(' ', 1)
                if text in type_map and type_map[text] != type_:
                    line = f'{id_}\t{type_map[text]} {span}\t{text}'
                print(line)

                    
def load_type_map(fn, options):
    type_map = {}
    with open(fn) as f:
        for ln, l in enumerate(f, start=1):
            text, type_ = l.rstrip('\n').split('\t')
            assert text not in type_map, f'duplicate: {text}'
            type_map[text] = type_
    return type_map

            
def main(argv):
    args = argparser().parse_args(argv[1:])
    type_map = load_type_map(args.types, args)
    for fn in args.ann:
        retype_standoff(fn, type_map, args)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
