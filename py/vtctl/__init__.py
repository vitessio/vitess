# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

from vtctl import vtctl_client, gorpc_vtctl_client

def init():
  vtctl_client.register_conn_class('gorpc', gorpc_vtctl_client.GoRpcVtctlClient)

init()
