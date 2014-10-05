#!/usr/bin/env python
#
# This script attempts to work around some of the more bizarre and
# quirky behaviours of htlatex.
#
# - We've persuaded htlatex to produce UTF-8, which unfortunately
#   causes it to use huge character sequences to represent even the
#   safe 7-bit ASCII subset of UTF-8.  We fix that up.
#
# - BUT we have to treat angle brackets (for example, redirections in
#   shell script snippets) specially, otherwise they'll break the
#   generated HTML.  (Reported by Johannes Hoff.)
#
# - For some reason, htlatex gives a unique ID to each fancyvrb
#   environment, which makes writing a sane, small CSS stylesheet
#   impossible.  We squish all those IDs down to nothing.

import os
import sys
import re

angle_re = re.compile(r'(&#x003[CE];)')
unicode_re = re.compile(r'&#x00([0-7][0-9A-F]);')
fancyvrb_re = re.compile(r'id="fancyvrb\d+"', re.I)
ligature_re = re.compile(r'&#xFB0([0-4]);')

tmpsuffix = '.tmp.' + str(os.getpid())

def hide_angle(m):
    return m.group(1).lower()

def fix_ascii(m):
    return chr(int(m.group(1), 16))

ligatures = ['ff', 'fi', 'fl', 'ffi', 'ffl']

def expand_ligature(m):
    return ligatures[int(m.group(1))]

for name in sys.argv[1:]:
    tmpname = name + tmpsuffix
    ofp = file(tmpname, 'w')
    for line in file(name):
        line = angle_re.sub(hide_angle, line)
        line = unicode_re.sub(fix_ascii, line)
        line = ligature_re.sub(expand_ligature, line)
        line = fancyvrb_re.sub('id="fancyvrb"', line)
        ofp.write(line)
    ofp.close()
    os.rename(tmpname, name)
