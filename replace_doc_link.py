# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# ./replace_doc_link doc_link_dir doc_to_be_replaced
#
# This tool replaces all links in specified doc by the new links given in the doc link dir.
#

import os
import re
import sys

if __name__ == '__main__':
  if len(sys.argv) < 3:
      print "Usage: ./replace_doc_link.py doc_link_dir doc"
      exit(1)
  doc_link_dir = sys.argv[1]
  doc = sys.argv[2]
  files = next(os.walk(doc_link_dir))[2]
  filename_dict = {f.split('-')[-1]: os.path.join(doc_link_dir, os.path.splitext(f)[0]) for f in files}
  # print filename_dict
  for line in open(doc).readlines():
    line = line.rstrip() # remove newline
    ans = []
    pos = 0
    for m in re.finditer(r'\[(.*)\]\((.*.md)\)', line):
        title, link = m.group(1), m.group(2)
        filename = link.split('/')[-1]
        if filename in filename_dict:
            ans.append(line[pos:m.start()])
            ans.append('[' + title + ']')
            ans.append('( {{ site.url }}/' + filename_dict[filename] + ')')
            pos = m.end()
    ans.append(line[pos:])
    print ''.join(ans)
