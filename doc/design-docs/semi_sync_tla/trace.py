#!/usr/bin/env python3
"""Prints a TLC counterexample from out/<cfg>.out compactly: each step's action and the
variables it changed."""
import re, sys
txt = open(sys.argv[1]).read()
states = re.split(r'\nState \d+: ', txt)
prev = {}
for i, st in enumerate(states[1:], 1):
    head, _, body = st.partition('\n')
    act = re.sub(r' line \d+.*', '', head).strip('<>')
    cur = {}
    for m in re.finditer(r'^/\\ (\w+) = (.*?)(?=\n/\\ |\n\n|\Z)', body, re.S | re.M):
        cur[m.group(1)] = ' '.join(m.group(2).split())
    if i == 1:
        print('1 Init')
    else:
        ch = [f'{k}={v}' for k, v in cur.items() if prev.get(k) != v and not k.startswith('n')]
        print(f'{i} {act}\n     ' + '\n     '.join(ch))
    prev = cur
