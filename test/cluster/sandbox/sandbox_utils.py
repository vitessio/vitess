"""Sandbox util functions."""

import datetime
import os
import random


def create_log_file(log_dir, filename):
  """Create a log file.

  This function creates a timestamped log file, and updates a non-timestamped
  symlink in the log directory.

  Example: For a log called init.INFO, this function will create a log file
           called init.INFO.20170101-120000.100000 and update a symlink
           init.INFO to point to it.

  Args:
    log_dir: string, Base path for logs.
    filename: string, The base name of the log file.

  Returns:
    The opened file handle.
  """
  timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S.%f')
  symlink_name = os.path.join(log_dir, filename)
  timestamped_name = '%s.%s' % (symlink_name, timestamp)
  if os.path.islink(symlink_name):
    os.remove(symlink_name)
  os.symlink(timestamped_name, symlink_name)
  return open(timestamped_name, 'w')


def generate_random_name():
  with open('naming/adjectives.txt', 'r') as f:
    adjectives = [l.strip() for l in f if l.strip()]
  with open('naming/animals.txt', 'r') as f:
    animals = [l.strip() for l in f if l.strip()]
  return '%s%s' % (random.choice(adjectives), random.choice(animals))

