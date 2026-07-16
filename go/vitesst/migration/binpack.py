#!/usr/bin/env python3
"""Assign every verified package a CI bin, packed by measured runtime.

The vitesst CI workflow builds its job matrix by grouping the migration
manifest's verified packages on their ci_shard field, so this script is what
decides how many CI jobs there are and how long each one runs.

Usage: binpack.py [--budget SECONDS] [--manifest PATH]
"""

import argparse
import json

# Packages whose verify_cmd needs the debug2PC image cannot share a bin with
# packages that need the standard image, since a bin runs under one image.
DEBUG2PC = "debug2pc"


def bin_key(entry):
    return DEBUG2PC if DEBUG2PC in (entry.get("verify_cmd") or "") else "default"


def pack(entries, budget):
    """Greedy first-fit-decreasing: longest packages seed the bins."""
    bins = []
    for entry in sorted(entries, key=lambda e: e.get("runtime_sec") or 0, reverse=True):
        runtime = entry.get("runtime_sec") or 60
        for b in bins:
            if b["runtime"] + runtime <= budget:
                b["packages"].append(entry)
                b["runtime"] += runtime
                break
        else:
            bins.append({"packages": [entry], "runtime": runtime})
    return bins


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--budget", type=int, default=1200, help="target seconds per CI job")
    parser.add_argument("--manifest", default="go/vitesst/migration/manifest.json")
    args = parser.parse_args()

    with open(args.manifest) as f:
        manifest = json.load(f)

    verified = [e for e in manifest["packages"].values() if e["status"] == "verified"]

    for group in ("default", DEBUG2PC):
        entries = [e for e in verified if bin_key(e) == group]
        for i, b in enumerate(pack(entries, args.budget), start=1):
            name = f"{group}-{i}"
            for entry in b["packages"]:
                entry["ci_shard"] = name
            print(f"{name}: {len(b['packages'])} packages, {b['runtime'] / 60:.1f} min")

    with open(args.manifest, "w") as f:
        json.dump(manifest, f, indent=1, sort_keys=True)
        f.write("\n")


if __name__ == "__main__":
    main()
