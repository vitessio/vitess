# Copyright 2015 Google Inc. All Rights Reserved.

"""Contains environment specifications for vttest module.

This module is meant to be overwritten upon import into a development
tree with the appropriate values. It works as is in the Vitess tree.
"""

import os
import shutil
import tempfile

# this is the location of the vtocc binary
vtocc_binary = os.path.join(os.environ['VTROOT'], 'bin', 'vtocc')

# this is the location of the vtgate binary
vtgate_binary = os.path.join(os.environ['VTROOT'], 'bin', 'vtgate')

# this is the location of the mysqlctl binary, if mysql_db_mysqlctl is used.
mysqlctl_binary = os.path.join(os.environ['VTROOT'], 'bin', 'mysqlctl')

# this is the base port set by options.
base_port = None


def get_test_directory():
  """Returns the toplevel directory for the tests. Might create it."""
  directory = tempfile.mkdtemp(prefix='vt')
  os.mkdir(get_logs_directory(directory))
  return directory


def get_logs_directory(directory):
  """Returns the directory for logs, might be based on directory.

  Parameters:
    directory: the value returned by get_test_directory().
  """
  return os.path.join(directory, 'logs')


def cleanup_test_directory(directory):
  """Cleans up the test directory after the test is done.

  Parameters:
    directory: the value returned by get_test_directory().
  """
  shutil.rmtree(directory)


def extra_vtgate_parameters():
  """Returns extra parameters to send to vtgate."""
  return []


def extra_vtocc_parameters():
  """Returns extra parameters to send to vtocc."""
  return []


def process_is_healthy(name, addr):
  """Double-checks a process is healthy and ready for RPCs."""
  return True


def get_port(name, instance=0):
  """Returns the port to use for a given process.

  This is only called once per process.
  """
  if name == 'vtgate':
    return base_port
  elif name == 'mysql':
    return base_port + 1
  else:
    return base_port + 2 + instance
