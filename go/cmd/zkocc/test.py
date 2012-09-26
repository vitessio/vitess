#!/usr/bin/python

import json
from optparse import OptionParser
import os
import shlex
import signal
import socket
from subprocess import check_call, Popen, CalledProcessError, PIPE
import sys
import tempfile
import time

import MySQLdb


devnull = open('/dev/null', 'w')

class TestError(Exception):
  pass

class Break(Exception):
  pass

def pause(prompt):
  if options.debug:
    raw_input(prompt)

pid_map = {}
def _add_proc(proc):
  pid_map[proc.pid] = proc
  with open('.test-pids', 'a') as f:
    print >> f, proc.pid, os.path.basename(proc.args[0])

vttop = os.environ['VTTOP']
vtroot = os.environ['VTROOT']
hostname = socket.gethostname()


def run(cmd, trap_output=False, **kargs):
  args = shlex.split(cmd)
  if trap_output:
    kargs['stdout'] = PIPE
    kargs['stderr'] = PIPE
  if options.verbose:
    print "run:", cmd, ', '.join('%s=%s' % x for x in kargs.iteritems())
  proc = Popen(args, **kargs)
  proc.args = args
  stdout, stderr = proc.communicate()
  if proc.returncode:
    raise TestError('cmd fail:', args, stdout, stderr)
  return stdout, stderr


def run_fail(cmd, **kargs):
  args = shlex.split(cmd)
  kargs['stdout'] = PIPE
  kargs['stderr'] = PIPE
  if options.verbose:
    print "run: (expect fail)", cmd, ', '.join('%s=%s' % x for x in kargs.iteritems())
  proc = Popen(args, **kargs)
  proc.args = args
  stdout, stderr = proc.communicate()
  if proc.returncode == 0:
    raise TestError('expected fail:', args, stdout, stderr)
  return stdout, stderr


# run a daemon - kill when this script exits
def run_bg(cmd, **kargs):
  if options.verbose:
    print "run:", cmd, ', '.join('%s=%s' % x for x in kargs.iteritems())
  args = shlex.split(cmd)
  proc = Popen(args=args, **kargs)
  proc.args = args
  _add_proc(proc)
  return proc

def wait_procs(proc_list, raise_on_error=True):
  for proc in proc_list:
    proc.wait()
  for proc in proc_list:
    if proc.returncode:
      if options.verbose and proc.returncode not in (-9,):
        sys.stderr.write("proc failed: %s %s\n" % (proc.returncode, proc.args))
      if raise_on_error:
        raise CalledProcessError(proc.returncode, proc.args)

def setup():
  setup_procs = []

  # compile all the tools
  run('go build', cwd=vttop+'/go/cmd/zkctl')
  run('go build', cwd=vttop+'/go/cmd/zk')
  run('go build', cwd=vttop+'/go/cmd/zkocc')
  run('go build', cwd=vttop+'/go/cmd/zkclient2')

  setup_procs.append(run_bg(vtroot+'/bin/zkctl -zk.cfg 1@'+hostname+':3801:3802:3803 init'))
  wait_procs(setup_procs)

  with open('.test-zk-client-conf.json', 'w') as f:
    zk_cell_mapping = {'test_nj': 'localhost:3803',
                       'test_ny': 'localhost:3803',
                       'test_ca': 'localhost:3803',
                       'global': 'localhost:3803',}
    json.dump(zk_cell_mapping, f)
  os.putenv('ZK_CLIENT_CONFIG', '.test-zk-client-conf.json')

  run(vtroot+'/bin/zk touch -p /zk/test_nj/vt')
  run(vtroot+'/bin/zk touch -p /zk/test_ny/vt')
  run(vtroot+'/bin/zk touch -p /zk/test_ca/vt')

def remove(path):
  try:
    os.remove(path)
  except OSError as e:
    if options.verbose:
      print >> sys.stderr, e, path

def teardown():
  if options.skip_teardown:
    return
  teardown_procs = []
  teardown_procs.append(run_bg(vtroot+'/bin/zkctl -zk.cfg 1@'+hostname+':3801:3802:3803 teardown'))
  wait_procs(teardown_procs, raise_on_error=False)
  for proc in pid_map.values():
    if proc.pid and proc.returncode is None:
      proc.kill()
  with open('.test-pids') as f:
    for line in f:
      try:
        parts = line.strip().split()
        pid = int(parts[0])
        proc_name = parts[1]
        proc = pid_map.get(pid)
        if not proc or (proc and proc.pid and proc.returncode is None):
          os.kill(pid, signal.SIGTERM)
      except OSError as e:
        if options.verbose:
          print >> sys.stderr, e
  for path in ('.test-pids', '.test-zk-client-conf.json'):
    remove(path)

def _wipe_zk():
  run(vtroot+'/bin/zk rm -rf /zk/test_nj/vt')
  run(vtroot+'/bin/zk rm -rf /zk/test_ny/vt')
  #run(vtroot+'/bin/zk rm -rf /zk/test_ca/vt')
  run(vtroot+'/bin/zk rm -rf /zk/global/vt')

def _check_zk(ping_tablets=False):
  if ping_tablets:
    run(vtroot+'/bin/vtctl -ping-tablets Validate /zk/global/vt/keyspaces')
  else:
    run(vtroot+'/bin/vtctl Validate /zk/global/vt/keyspaces')

def run_test_zkocc():
  _wipe_zk()

  run(vtroot+'/bin/zk touch -p /zk/test_nj/zkocc1')
  run(vtroot+'/bin/zk touch -p /zk/test_nj/zkocc2')
  fd = tempfile.NamedTemporaryFile(delete=False)
  filename1 = fd.name
  fd.write("Test data 1")
  fd.close()
  run(vtroot+'/bin/zk cp '+filename1+' /zk/test_nj/zkocc1/data1')

  fd = tempfile.NamedTemporaryFile(delete=False)
  filename2 = fd.name
  fd.write("Test data 2")
  fd.close()
  run(vtroot+'/bin/zk cp '+filename2+' /zk/test_nj/zkocc1/data2')

  fd = tempfile.NamedTemporaryFile(delete=False)
  filename3 = fd.name
  fd.write("Test data 3")
  fd.close()
  run(vtroot+'/bin/zk cp '+filename3+' /zk/test_nj/zkocc1/data3')

  # preload the test_nj cell
  vtocc_14850 = run_bg(vtroot+'/bin/zkocc -port=14850 -connect-timeout=2s -cache-refresh-interval=1s test_nj')
  time.sleep(1)

  # get test
  out, err = run(vtroot+'/bin/zkclient2 -server localhost:14850 /zk/test_nj/zkocc1/data1', trap_output=True)
  if err != "/zk/test_nj/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=false, Stale=false)\n":
    raise TestError('unexpected get output: ', err)

  # getv test
  out, err = run(vtroot+'/bin/zkclient2 -server localhost:14850 /zk/test_nj/zkocc1/data1 /zk/test_nj/zkocc1/data2', trap_output=True)
  if err != """[0] /zk/test_nj/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=true, Stale=false)
[1] /zk/test_nj/zkocc1/data2 = Test data 2 (NumChildren=0, Version=0, Cached=false, Stale=false)
""":
    raise TestError('unexpected getV output: ', err)

  # children test
  out, err = run(vtroot+'/bin/zkclient2 -server localhost:14850 -mode children /zk/test_nj', trap_output=True)
  if err != """Path = /zk/test_nj
Child[0] = zkocc1
Child[1] = zkocc2
NumChildren = 2
CVersion = 4
Cached = false
Stale = false
""":
    raise TestError('unexpected children output: ', err)

  # start a background process to query the same value over and over again
  # while we kill the zk server and restart it
  outfd = tempfile.NamedTemporaryFile(delete=False)
  filename = outfd.name
  querier = run_bg('/bin/bash -c "while true ; do '+vtroot+'/bin/zkclient2 -server localhost:14850 /zk/test_nj/zkocc1/data1 ; sleep 0.1 ; done"', stderr=outfd.file)
  outfd.close()
  time.sleep(1)

  # kill zk server, sleep a bit, restart zk server, sleep a bit
  run(vtroot+'/bin/zkctl -zk.cfg 1@'+hostname+':3801:3802:3803 shutdown')
  time.sleep(3)
  run(vtroot+'/bin/zkctl -zk.cfg 1@'+hostname+':3801:3802:3803 start')
  time.sleep(3)

  querier.kill()

  # get test
  out, err = run(vtroot+'/bin/zkclient2 -server localhost:14850 /zk/test_nj/zkocc1/data3', trap_output=True)
  if err != "/zk/test_nj/zkocc1/data3 = Test data 3 (NumChildren=0, Version=0, Cached=false, Stale=false)\n":
    raise TestError('unexpected get output: ', err)


  print "Checking", filename
  fd = open(filename, "r")
  state = 0
  for line in fd:
    if line == "/zk/test_nj/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=true, Stale=false)\n":
      stale = False
    elif line == "/zk/test_nj/zkocc1/data1 = Test data 1 (NumChildren=0, Version=0, Cached=true, Stale=true)\n":
      stale = True
    else:
      raise TestError('unexpected line: ', line)
    if state == 0:
      if stale:
        state = 1
    elif state == 1:
      if not stale:
        state = 2
    else:
      if stale:
        raise TestError('unexpected stale state')
  if state != 2:
    raise TestError('unexpected ended stale state')
  fd.close()

  # FIXME(alainjobart): with better test infrastructure, maintain a list of
  # files to clean up.
  remove(filename)
  remove(filename1)
  remove(filename2)
  remove(filename3)
  vtocc_14850.kill()

def run_all():
  run_test_zkocc()

options = None
def main():
  global options
  parser = OptionParser()
  parser.add_option('-v', '--verbose', action='store_true')
  parser.add_option('-d', '--debug', action='store_true')
  parser.add_option('--skip-teardown', action='store_true')
  (options, args) = parser.parse_args()

  if not args:
    args = ['run_all']

  try:
    if args[0] != 'teardown':
      setup()
      if args[0] != 'setup':
        for arg in args:
          globals()[arg]()
          print "GREAT SUCCESS"
  except KeyboardInterrupt:
    pass
  except Break:
    options.skip_teardown = True
  finally:
    teardown()


if __name__ == '__main__':
  main()
