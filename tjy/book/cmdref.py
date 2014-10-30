#!/usr/bin/env python

import getopt
import itertools
import os
import re
import sys

def usage(exitcode):
    print >> sys.stderr, ('usage: %s [-H|--hidden] hg_repo' % 
                          os.path.basename(sys.argv[0]))
    sys.exit(exitcode)

try:
    opts, args = getopt.getopt(sys.argv[1:], 'AHh?', ['all', 'help', 'hidden'])
    opt_all = False
    opt_hidden = False
    for o, a in opts:
        if o in ('-h', '-?', '--help'):
            usage(0)
        if o in ('-A', '--all'):
            opt_all = True
        if o in ('-H', '--hidden'):
            opt_hidden = True
except getopt.GetoptError, err:
    print >> sys.stderr, 'error:', err
    usage(1)

try:
    hg_repo, ltx_file = args
except ValueError:
    usage(1)

if not os.path.isfile(os.path.join(hg_repo, 'mercurial', 'commands.py')):
    print >> sys.stderr, ('error: %r does not contain mercurial code' %
                          hg_repo)
    sys.exit(1)

sys.path.insert(0, hg_repo)

from mercurial import commands

def get_commands():
    seen = {}
    for name, info in sorted(commands.table.iteritems()):
        aliases = name.split('|', 1)
        name = aliases.pop(0).lstrip('^')
        function, options, synopsis = info
        seen[name] = {}
        for shortopt, longopt, arg, desc in options:
            seen[name][longopt] = shortopt
    return seen

def cmd_filter((name, aliases, options)):
    if opt_all:
        return True
    if opt_hidden:
        return name.startswith('debug')
    return not name.startswith('debug')

def scan(ltx_file):
    cmdref_re = re.compile(r'^\\cmdref{(?P<cmd>\w+)}')
    optref_re = re.compile(r'^\\l?optref{(?P<cmd>\w+)}'
                           r'(?:{(?P<short>[^}])})?'
                           r'{(?P<long>[^}]+)}')

    seen = {}
    locs = {}
    for lnum, line in enumerate(open(ltx_file)):
        m = cmdref_re.match(line)
        if m:
            d = m.groupdict()
            cmd = d['cmd']
            seen[cmd] = {}
            locs[cmd] = lnum + 1
            continue
        m = optref_re.match(line)
        if m:
            d = m.groupdict()
            seen[d['cmd']][d['long']] = d['short']
            continue
    return seen, locs
    
documented, locs = scan(ltx_file)
known = get_commands()

doc_set = set(documented)
known_set = set(known)

errors = 0

for nonexistent in sorted(doc_set.difference(known_set)):
    print >> sys.stderr, ('%s:%d: %r command does not exist' %
                          (ltx_file, locs[nonexistent], nonexistent))
    errors += 1

def optcmp(a, b):
    la, sa = a
    lb, sb = b
    sc = cmp(sa, sb)
    if sc:
        return sc
    return cmp(la, lb)

for cmd in doc_set.intersection(known_set):
    doc_opts = documented[cmd]
    known_opts = known[cmd]
    
    do_set = set(doc_opts)
    ko_set = set(known_opts)

    for nonexistent in sorted(do_set.difference(ko_set)):
        print >> sys.stderr, ('%s:%d: %r option to %r command does not exist' %
                              (ltx_file, locs[cmd], nonexistent, cmd))
        errors += 1

    def mycmp(la, lb):
        sa = known_opts[la]
        sb = known_opts[lb]
        return optcmp((la, sa), (lb, sb))

    for undocumented in sorted(ko_set.difference(do_set), cmp=mycmp):
        print >> sys.stderr, ('%s:%d: %r option to %r command not documented' %
                              (ltx_file, locs[cmd], undocumented, cmd))
        shortopt = known_opts[undocumented]
        if shortopt:
            print '\optref{%s}{%s}{%s}' % (cmd, shortopt, undocumented)
        else:
            print '\loptref{%s}{%s}' % (cmd, undocumented)
        errors += 1
    sys.stdout.flush()

if errors:
    sys.exit(1)

sorted_locs = sorted(locs.iteritems(), key=lambda x:x[1])

def next_loc(cmd):
    for i, (name, loc) in enumerate(sorted_locs):
        if name >= cmd:
            return sorted_locs[i-1][1] + 1
    return loc

for undocumented in sorted(known_set.difference(doc_set)):
    print >> sys.stderr, ('%s:%d: %r command not documented' %
                          (ltx_file, next_loc(undocumented), undocumented))
    print '\cmdref{%s}' % undocumented
    for longopt, shortopt in sorted(known[undocumented].items(), cmp=optcmp):
        if shortopt:
            print '\optref{%s}{%s}{%s}' % (undocumented, shortopt, longopt)
        else:
            print '\loptref{%s}{%s}' % (undocumented, longopt)
    sys.stdout.flush()
    errors += 1

sys.exit(errors and 1 or 0)
