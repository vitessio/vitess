#!/usr/bin/env python3
# Summarize abdrain logs: mean (min-max) per arm and case of each metric.
import re, sys, collections
data = collections.defaultdict(lambda: collections.defaultdict(list))
for path in sys.argv[1:]:
    for line in open(path):
        f = line.split()
        if not f:
            continue
        m = re.match(r'(.+)-r\d+$', f[0])
        if not m:
            continue
        key = m.group(1)
        for kv in f[1:]:
            if '=' in kv:
                k, v = kv.split('=', 1)
                v = v.rstrip('s')
                try:
                    data[key][k].append(float(v))
                except ValueError:
                    pass
metrics = ['dur', 'vttablet_100', 'mysqld_100', 'tgtvtt', 'tgtmy', 'load']
print('%-26s ' % 'arm-case' + ' '.join('%-22s' % m for m in metrics))
for key in sorted(data, key=lambda k: (k.split('-', 1)[1], k)):
    row = []
    for m in metrics:
        v = data[key].get(m, [])
        row.append('%-22s' % ('%.1f (%.1f-%.1f) n%d' % (sum(v) / len(v), min(v), max(v), len(v)) if v else '-'))
    print('%-26s ' % key + ' '.join(row))
