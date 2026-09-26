#!/usr/bin/env python3
# agg.py FILE [BASECFG]: mean per (workload, config) of bench.sh lines; % vs BASECFG (default "base").
# MEDIAN=1 uses medians instead of means. CPU columns show [min-max].
import os, re, sys, statistics
from collections import defaultdict, OrderedDict

f = sys.argv[1]
basecfg = sys.argv[2] if len(sys.argv) > 2 else "base"
agg = statistics.median if os.environ.get("MEDIAN") else statistics.mean
data = OrderedDict()
cfgs = []
for line in open(f):
    p = line.split()
    if len(p) < 5 or "FAILED" in line:
        continue
    m = re.match(r"r(\d+)-(.+)", p[0])
    if not m:
        continue
    cfg, name = m.group(2), p[1]
    kv = dict(x.split("=", 1) for x in p[2:] if "=" in x)
    if cfg not in cfgs:
        cfgs.append(cfg)
    data.setdefault(name, defaultdict(list))[cfg].append(kv)

cols = [("tps", "TPS"), ("qps", "QPS"), ("avg", "avg ms"), ("p95", "p95 ms"), ("p99", "p99 ms"),
        ("vtgate", "vtgate µs/q"), ("tablets", "tablets µs/q"), ("mysqld", "mysqld µs/q")]
print("| workload | config | n | " + " | ".join(c[1] for c in cols) + " | load |")
print("|" + "---|" * (len(cols) + 4))
for name, bycfg in data.items():
    base = {}
    for cfg in cfgs:
        runs = bycfg.get(cfg)
        if not runs:
            continue
        cells = []
        for key, _ in cols:
            vals = [float(r[key]) for r in runs if r.get(key) not in (None, "")]
            if not vals:
                cells.append("-")
                continue
            v = agg(vals)
            if cfg == basecfg:
                base[key] = v
            s = f"{v:.0f}" if v >= 100 else f"{v:.2f}"
            if cfg != basecfg and base.get(key):
                s += f" ({(v / base[key] - 1) * 100:+.1f}%)"
            if key in ("vtgate", "tablets"):
                s += f" [{min(vals):.0f}–{max(vals):.0f}]"
            cells.append(s)
        load = agg([float(r["load"]) for r in runs])
        print(f"| {name} | {cfg} | {len(runs)} | " + " | ".join(cells) + f" | {load:.1f} |")
