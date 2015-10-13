# Copyright 2012, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from distutils.core import setup

setup(name = "vitess",
      packages=["net", "vtctl", "vtdb", "vtproto", "vttest"],
      platforms = "Any",
      )
