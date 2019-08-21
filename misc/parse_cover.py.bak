#!/usr/bin/python

# Copyright 2017 Google Inc.
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

# this is a small helper script to parse test coverage and display stats.
import re
import sys

coverage_pattern = re.compile(r"coverage: (\d+).(\d+)% of statements")

no_test_file_count = 0
coverage_count = 0
coverage_sum = 0.0

for line in sys.stdin:
  print line,
  sys.stdout.flush

  if line.find('[no test files]') != -1:
    no_test_file_count += 1
    continue

  m = coverage_pattern.search(line)
  if m != None:
    coverage_count += 1
    coverage_sum += float(m.group(1) + "." + m.group(2))
    continue

directories_covered = coverage_count * 100 / (no_test_file_count + coverage_count)
average_coverage = coverage_sum / coverage_count

print "Directory test coverage: %u%%" % directories_covered
print "Average test coverage: %u%%" % int(average_coverage)
