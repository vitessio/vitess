# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.
"""This is the setup script for the submodules in the Vitess python client.
"""

from distutils.core import setup


setup(name="vitess",
      packages=["vtctl", "vtdb", "vtproto", "vttest"],
      platforms="Any",
     )
