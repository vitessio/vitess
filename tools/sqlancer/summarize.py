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

"""Groups SQLancer failure logs by their root error, so that each distinct problem is listed once."""

import collections
import os
import re
import sys


def main(log_dir):
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
    for key, failures in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        print(f"[{len(failures)}x] {key}")
        for name, statement in failures[:3]:
            print(f"    {name}: {statement[:300]}")


if __name__ == "__main__":
    main(sys.argv[1])
