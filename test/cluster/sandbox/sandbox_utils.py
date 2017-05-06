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

