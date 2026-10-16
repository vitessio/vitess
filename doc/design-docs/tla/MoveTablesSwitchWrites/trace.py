#!/usr/bin/env python3
"""Print a TLC JSON trace (-dumpTrace json) compactly: action + changed vars."""
import json
import sys

HIDE = {"sw", "capVer", "lastVer", "bufOnce", "gwErr", "invalid", "try"}


def fmt(v):
    if isinstance(v, dict):
        if set(v) == {"ep", "route"}:
            return "{ep:%s,%s}" % (v["ep"], v["route"])
        return "{" + ", ".join("%s:%s" % (k, fmt(x)) for k, x in v.items()) + "}"
    if isinstance(v, list):
        return "<<" + ",".join(fmt(x) for x in v) + ">>"
    return str(v)


def main(path):
    with open(path) as f:
        data = json.load(f)
    ce = data["counterexample"]
    steps = [("Init", ce["state"][0][1])]
    for (_, _), act, (_, dst) in [(a[0], a[1], a[2]) for a in ce["action"]]:
        steps.append((act["name"], dst))
    # A liveness counterexample ends in a lasso back to an earlier state.
    loop_to = None
    if ce["action"] and len(ce["state"]) and ce["action"][-1][2][0] <= ce["action"][-1][0][0]:
        loop_to = ce["action"][-1][2][0]
    prev = None
    for i, (action, vals) in enumerate(steps):
        if prev is None:
            print("%2d %-22s %s" % (i + 1, "Init", ""))
        else:
            changed = []
            for k, v in vals.items():
                if k in HIDE:
                    continue
                if prev.get(k) != v:
                    if isinstance(v, dict) and isinstance(prev.get(k), dict) and k != "cache":
                        d = {kk: vv for kk, vv in v.items() if prev[k].get(kk) != vv}
                        changed.append("%s[%s]" % (k, fmt(d)))
                    else:
                        changed.append("%s=%s" % (k, fmt(v)))
            print("%2d %-22s %s" % (i + 1, action, "  ".join(changed)))
        prev = vals
    if loop_to is not None:
        print("   (the last step returns to state %d: this cycle repeats forever)" % loop_to)


if __name__ == "__main__":
    main(sys.argv[1])
