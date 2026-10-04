#!/usr/bin/env python3
"""Summarizes chaos reports as a markdown table: one row per scenario, one column per results
directory (profile), each cell the failover time, the time without acknowledged writes, and the
violations.

    go/test/endtoend/vtorc/chaos/chaos_summary.py /home/ubuntu/chaos-results/semisync-3vtorc ...
"""
import os
import re
import sys


def cell(report):
    txt = open(report).read()
    fo = re.search(r'failover happened=(true|false) after ([\d.]+)s', txt)
    unavail = re.search(r'unavailable \(no acked write for >= 1s\): ([\d.]+)s in total', txt)
    viol = re.search(r'-- VIOLATIONS \((\d+)\)\n((?:   .*\n)*)', txt)
    parts = []
    if fo:
        parts.append(('failover %ss' % fo.group(2)) if fo.group(1) == 'true' else 'no failover')
    elif re.search(r'topo shard primary never changed', txt):
        parts.append('no failover')
    if unavail:
        parts.append('down %ss' % unavail.group(1))
    lost = re.search(r'DURABILITY: (\d+)/(\d+) acked writes missing', txt)
    if lost:
        parts.append('**LOST %s acked**' % lost.group(1))
    if viol and viol.group(1) != '0':
        kinds = []
        for line in viol.group(2).splitlines():
            line = line.strip()
            for key, short in [('SPLIT BRAIN', 'dual-writable'), ('CONVERGENCE', 'not converged'),
                               ('SEMISYNC: DRAINED', 'drained acker'), ('SEMISYNC', 'semi-sync'),
                               ('DURABILITY', None), ('DEPOSED', 'acked by deposed'),
                               ('ERRANT', 'errant'), ('unexpected failover', 'unexpected failover')]:
                if key in line:
                    if short and short not in kinds:
                        kinds.append(short)
                    break
            else:
                kinds.append(line[:40])
        parts.append('violations: ' + ', '.join(kinds) if kinds else '')
    drained = re.search(r'DRAINED by VTOrc with errant', txt)
    if drained:
        parts.append('old primary drained')
    return '; '.join(p for p in parts if p)


def main(dirs):
    scenarios = {}
    for d in dirs:
        for name in sorted(os.listdir(d)):
            rep = os.path.join(d, name, 'report.txt')
            if os.path.isfile(rep):
                scenarios.setdefault(name, {})[d] = cell(rep)
    print('| Scenario | ' + ' | '.join(os.path.basename(d.rstrip('/')) for d in dirs) + ' |')
    print('|---' * (len(dirs) + 1) + '|')
    for name in sorted(scenarios, key=lambda s: [int(x) if x.isdigit() else x for x in re.split(r'(\d+)', s)]):
        print('| ' + name + ' | ' + ' | '.join(scenarios[name].get(d, '') for d in dirs) + ' |')


if __name__ == '__main__':
    main(sys.argv[1:])
