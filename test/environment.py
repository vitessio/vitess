#!/usr/bin/python

import logging
import os
import subprocess

# vttop is the toplevel of the vitess source tree
vttop = os.environ['VTTOP']

# vtroot is where everything gets installed
vtroot = os.environ['VTROOT']

# vtdataroot is where to put all the data files
vtdataroot = os.environ.get('VTDATAROOT', '/vt')

# tmproot is the temporary place to put all test files
tmproot = os.path.join(vtdataroot, 'tmp')

# where to start allocating ports from
vtportstart = int(os.environ.get('VTPORTSTART', '6700'))

def setup():
  global tmproot
  try:
    os.makedirs(tmproot)
  except OSError:
    # directory already exists
    pass

# port management: reserve count consecutive ports, returns the first one
def reserve_ports(count):
  global vtportstart
  result = vtportstart
  vtportstart += count
  return result

# compile command line programs, only once
compiled_progs = []
def prog_compile(name):
  if name in compiled_progs:
    return
  compiled_progs.append(name)
  logging.debug('Compiling %s', name)
  proc = subprocess.Popen(['go', 'install'],
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          cwd=os.path.join(vttop, 'go', 'cmd', name))
  stdout, stderr = proc.communicate()
  if proc.returncode:
    raise Exception('Cannot compile ' + name)

# binary management: returns the full path for a binary
def binary_path(name):
  prog_compile(name)
  return os.path.join(vtroot, 'bin', name)
