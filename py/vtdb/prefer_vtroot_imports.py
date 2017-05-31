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

"""Reorder sys.path to put $VTROOT/dist/* paths before others.

This ensures libraries installed there will be preferred over other versions
that may be present at the system level. We do this at runtime because
regardless of what we set in the PYTHONPATH environment variable, the system
dist-packages folder gets prepended sometimes.

To use this, just import it before importing packages that you want to make
sure are overridden from $VTROOT/dist.

from vtdb import prefer_vtroot_imports  # pylint: disable=unused-import
"""

import os
import sys


def _prefer_vtroot_imports():
  """Reorder sys.path to put $VTROOT/dist before others."""

  vtroot = os.environ.get('VTROOT')
  if not vtroot:
    # VTROOT is not set. Don't try anything.
    return
  dist = os.path.join(vtroot, 'dist')

  dist_paths = []
  other_paths = []

  for path in sys.path:
    if path:
      if path.startswith(dist):
        dist_paths.append(path)
      else:
        other_paths.append(path)

  sys.path = [''] + dist_paths + other_paths

_prefer_vtroot_imports()
