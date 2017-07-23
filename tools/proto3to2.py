#!/usr/bin/env python

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


import sys
import re
import string

syntax = re.compile(r'^\s*syntax\s*=\s*"proto3";')
package = re.compile(r'^\s*package\s*(\S+);')
missing_optional = re.compile(r'^(\s+)(\S+)(\s+\S+\s*=\s*\S+;)')
map_type = re.compile(r'^(\s*)map\s*<(\S+),\s*(\S+)>\s+(\S+)\s*=\s*(\S+);')

for line in sys.stdin:
  # syntax = "proto3";
  if syntax.match(line):
    print 'syntax = "proto2";'
    continue

  m = package.match(line)
  if m:
    pkg = m.group(1)

    print line

    # Add PHP-specific options.
    print 'import "php.proto";'
    print 'option (php.namespace) = "Vitess.Proto.%s";' % pkg.capitalize()
    print 'option (php.multifile) = true;'
    continue

  # map<key, value> field = index;
  m = map_type.match(line)
  if m:
    (indent, key, value, field, index) = m.groups()

    entry = string.capwords(field, '_').replace('_', '') + 'Entry'

    print indent + 'message %s { optional %s key = 1; optional %s value = 2; }' % (entry, key, value)
    print indent + 'repeated %s %s = %s;' % (entry, field, index)
    continue

  # type field = index;
  m = missing_optional.match(line)
  if m:
    (indent, type, rest) = m.groups()
    if type != 'option':
      print indent + 'optional %s%s' % (type, rest)
      continue

  print line,
