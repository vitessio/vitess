# Copyright 2026 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Groups SQLancer's failures and the findings of the VITESS_DIFF oracle, so that each distinct problem is listed once.

usage: summarize.py <sqlancer-logs-dir>
"""

import collections
import json
import os
import re
import sys


def main(logs):
    failures(os.path.join(logs, "mysql"))
    findings(os.path.join(logs, "vitess", "findings.jsonl"))


def failures(log_dir):
    """Failures that stopped a SQLancer thread: unexpected errors and mismatches of SQLancer's own oracles."""
    if not os.path.isdir(log_dir):
        return
    groups = collections.defaultdict(list)
    for name in sorted(os.listdir(log_dir)):
        if not name.endswith(".log") or name.endswith("-cur.log"):
            continue
        with open(os.path.join(log_dir, name), errors="replace") as f:
            text = f.read()
        failure = re.search(r"^--java\.lang\.AssertionError: (.*)$", text, re.M)
        if not failure:
            continue
        causes = re.findall(r"^--Caused by: (.*)$", text, re.M)
        if causes:
            cause = causes[-1]
            statement = failure.group(1)
        else:
            cause = "result mismatch (potential logic bug)"
            # The oracle logs the reference query, then the query that disagrees with it.
            mismatch = re.search(r"^-- On the database set up by the statements above, .*\n-- .*\n-- (.*)$", text, re.M)
            statement = mismatch.group(1) if mismatch else failure.group(1)
        key = re.sub(r"'[^']*'", "'?'", re.sub(r"\d+", "N", cause))[:160]
        groups[key].append((name, statement))
    if groups:
        print("== Failures that stopped a SQLancer thread (logs/mysql)")
    for key, entries in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        print(f"[{len(entries)}x] {key}")
        for name, statement in entries[:3]:
            print(f"    {name}: {statement[:300]}")


def findings(path):
    """Findings of the VITESS_DIFF oracle, which records problems without stopping."""
    if not os.path.exists(path):
        return
    groups = collections.defaultdict(list)
    with open(path, errors="replace") as f:
        for line in f:
            finding = json.loads(line)
            groups[(finding["category"], finding["signature"])].append(finding)
    print("== VITESS_DIFF findings (logs/vitess); reproductions are in the listed .sql files")
    for (category, signature), entries in sorted(groups.items(), key=lambda kv: (kv[0][0], -len(kv[1]))):
        print(f"[{len(entries)}x] {category}: {signature}")
        for finding in entries[:2]:
            query = finding["query"] or finding["details"].split("\n")[0]
            repro = f" ({finding['repro']})" if finding["repro"] else ""
            print(f"    {query[:300]}{repro}")


if __name__ == "__main__":
    main(sys.argv[1])
