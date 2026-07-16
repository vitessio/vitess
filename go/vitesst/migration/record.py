#!/usr/bin/env python3
"""Record verification results into the migration manifest.

Reads the tab-separated output of a verification run, one "package result
seconds" line per package, and marks every package that passed as verified at
the given commit. A package that failed, or that lost a race for a host port,
is left alone: only a green run on an idle machine makes a package verified.

Usage: record.py --sha <commit> results.tsv [results.tsv ...]
"""

import argparse
import json

MANIFEST = "go/vitesst/migration/manifest.json"
PREFIX = "vitess.io/vitess/go/test/endtoend/"

PASSED = {"verified", "passed"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sha", required=True, help="commit the packages were verified at")
    parser.add_argument("--manifest", default=MANIFEST)
    parser.add_argument("results", nargs="+")
    args = parser.parse_args()

    with open(args.manifest) as f:
        manifest = json.load(f)

    recorded, skipped = [], []
    for path in args.results:
        with open(path) as f:
            for line in f:
                fields = line.split("\t")
                if len(fields) != 3:
                    continue
                pkg, result, seconds = (field.strip() for field in fields)

                entry = manifest["packages"].get(PREFIX + pkg)
                if entry is None:
                    continue
                if result not in PASSED:
                    skipped.append(f"{pkg} ({result})")
                    continue

                entry["status"] = "verified"
                entry["runtime_sec"] = float(seconds)
                entry["verified_at_sha"] = args.sha
                recorded.append(pkg)

    with open(args.manifest, "w") as f:
        json.dump(manifest, f, indent=1, sort_keys=True)
        f.write("\n")

    print(f"verified: {len(recorded)}")
    for pkg in skipped:
        print(f"not verified: {pkg}")


if __name__ == "__main__":
    main()
