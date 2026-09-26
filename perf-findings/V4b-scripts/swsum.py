#!/usr/bin/env python3
# Summarize abswitch logs: per arm and action, gap and step durations (median, min-max, n).
import re, sys, collections, statistics
d = collections.defaultdict(lambda: collections.defaultdict(list))
for path in sys.argv[1:]:
    for line in open(path):
        m = re.match(r'(\S+?)-(base|ref|new2|new|[a-z0-9]+)-r\d+-p\d+ (switchtraffic|reversetraffic) ', line)
        if not m:
            continue
        key = (m.group(3), m.group(2))
        right = line.split('|', 1)[1]
        for k, v in re.findall(r'(\w+)=\s*([-+]?\d+)', right):
            d[key][k].append(int(v))
        c = re.search(r'cmd=([\d.]+)s', line)
        if c:
            d[key]['cmd'].append(int(float(c.group(1)) * 1000))
cols = ['gap', 'cmd', 'gap_start', 'stopw', 'locks', 'catch', 'rev', 'journ', 'allow', 'route', 'gap_end_after_routed']
print('%-26s' % 'action/arm' + ''.join('%-18s' % c[:16] for c in cols))
for key in sorted(d):
    row = []
    for c in cols:
        v = d[key].get(c, [])
        row.append('%-18s' % ('%d (%d-%d) n%d' % (statistics.median(v), min(v), max(v), len(v)) if v else '-'))
    print('%-26s' % ('%s/%s' % key) + ''.join(row))
