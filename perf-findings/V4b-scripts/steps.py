#!/usr/bin/env python3
# Break a switch/reverse run into steps: vtctld.log step markers plus the prober's write gap.
# Usage: steps.py OUTDIR  (OUTDIR from switch.sh: vtctld.log, events)
import sys, re, datetime

d = sys.argv[1]
marks = [
    ('stopw', 'Stopping source writes'),
    ('stopped', 'Stopping streams'),
    ('lock1', 'Locking (and then immediately unlocking)'),
    ('pos', 'after having stopped writes'),
    ('catch', 'Waiting for streams to catchup'),
    ('caught', 'Migrating streams'),
    ('reverse', 'Creating reverse streams'),
    ('journal', 'Creating journals for workflow'),
    ('journaled', 'Created journals for workflow'),
    ('routing', 'Updating routing rules for workflow'),
    ('routed', 'Updated routing rules for workflow'),
    ('done', 'Switch writes completed'),
]
ts = {}
lock_times = []
for line in open(d + '/vtctld.log', errors='replace'):
    m = re.match(r'(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) ', line)
    if not m:
        continue
    t = datetime.datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=datetime.timezone.utc).timestamp()
    for k, s in marks:
        if s in line and k not in ts:
            ts[k] = t
    if 'Locking (and then immediately unlocking)' in line:
        lock_times.append(t)
# Prober gap: the longest interval between successful completions.
ev = []
for line in open(d + '/events'):
    f = line.split(' ', 2)
    st, lat, err = int(f[0]) / 1e6, int(f[1]) / 1e6, f[2].strip()
    if err == '""':
        ev.append(st + lat)
ev.sort()
gs, ge = 0, 0
for a, b in zip(ev, ev[1:]):
    if b - a > ge - gs:
        gs, ge = a, b
if 'stopw' not in ts:
    print('no markers')
    sys.exit(0)
t0 = ts['stopw']
ms = lambda a, b: '%4.0f' % ((ts[b] - ts[a]) * 1e3) if a in ts and b in ts else '   -'
locks = '%4.0f' % ((ts['pos'] - lock_times[0]) * 1e3) if lock_times and 'pos' in ts else '   -'
print('gap=%4.0fms gap_start=%+5.0f gap_end_after_routed=%+4.0f | stopw=%s locks=%s(n%d) catch=%s rev=%s journ=%s allow=%s route=%s' % (
    (ge - gs) * 1e3, (gs - t0) * 1e3, (ge - ts.get('routed', ge)) * 1e3,
    ms('stopw', 'stopped'), locks, len(lock_times), ms('catch', 'caught'), ms('reverse', 'journal'),
    ms('journal', 'journaled'), ms('journaled', 'routing'), ms('routing', 'routed')))
