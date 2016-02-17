#!/usr/bin/env python

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
