#!/usr/bin/env python

import argparse
import sys
import os
import numpy as np
import re

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('filenames', nargs='+', 
        help='List of input files, use "-" for stdin.')
    parser.add_argument('-s', '--suffix', default='.csv',
        help='Suffix of output file.')
    return parser.parse_args()

def parse_exp(s_exp, lines, _out=sys.stdout):
    a = np.array(lines)
    # convert s_exp to its parameters, e.g., 
    # 'exp(total=256,working=1,offset=0)' -> ['256', '1', '0']
    result = re.findall('[0-9]+', s_exp)
    assert len(result) == 3
    result += np.median(a, axis=0).tolist()
    result += np.average(a, axis=0).tolist()
    result += np.std(a, axis=0).tolist()
    print >>_out, '\t'.join(map(str, result))

def parse_one_file(fin, fout):
    s_exp = ''
    lines = []
    for l in fin:
        parts = l.split()
        if len(parts) == 0:
            if s_exp:
                parse_exp(s_exp, lines, fout)
                s_exp = ''
                lines = []
            else:
                continue
        assert len(parts) == 15
        assert parts[3] == 'INFO' and parts[6] == 'times:' and parts[10] == 'sizes:'
        _s_exp = parts[5]
        if s_exp != _s_exp and s_exp:
            parse_exp(s_exp, lines, fout)
            s_exp = ''
            lines = []
        s_exp = _s_exp
        n = map(lambda x: float(parts[x]), [7,8,9,11,12,13,14])
        # t_init, t_criu, t_init+t_criu, t_app, t_init+t_criu+t_app,
        # s_init, s_criu, s_init+s_criu, s_app, s_init+s_criu+s_app, s_upload
        lines.append([n[0], n[1], n[0]+n[1], n[2], n[0]+n[1]+n[2],
            n[3], n[4], n[3]+n[4], n[5], n[3]+n[4]+n[5], n[6]])
    if len(lines) > 0:
        parse_exp(s_exp, lines, fout)

def main():
    args = parse_args()
    for filename in args.filenames:
        if filename == '-':
            parse_one_file(sys.stdin, sys.stdout)
        else:
            with open(filename) as f:
                with open(filename+args.suffix, 'w+') as fout:
                    parse_one_file(f, fout)

if __name__ == '__main__':
    main()
