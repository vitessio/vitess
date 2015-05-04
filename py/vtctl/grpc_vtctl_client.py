# Copyright 2015, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

# This file contains the grpc implementation of the vtctl client.
# It is untested and doesn't work just yet: ExecuteVtctlCommand
# just seems to time out.

import logging
from urlparse import urlparse

import vtctl_client
import vtctl_pb2

class GRPCVtctlClient(vtctl_client.VctlClient):
    """GoRpcVtctlClient is the gRPC implementation of VctlClient.
    It is registered as 'grpc' protocol.
    """

    def __init__(self, addr, timeout, user=None, password=None, encrypted=False,
                keyfile=None, certfile=None):
        self.addr = addr
        self.timeout = timeout
        self.stub = None

    def __str__(self):
        return '<VtctlClient %s>' % self.addr

    def dial(self):
        if self.stub:
            self.stub.close()

        p = urlparse("http://" + self.addr)
        self.stub = vtctl_pb2.early_adopter_create_Vtctl_stub(p.hostname,
                                                              p.port)

    def close(self):
        self.stub.close()
        self.stub = None

    def is_closed(self):
        return self.stub == None

    def execute_vtctl_command(self, args, action_timeout=30.0,
                              lock_timeout=5.0, info_to_debug=False):
        """Executes a remote command on the vtctl server.

        Args:
            args: Command line to run.
            action_timeout: total timeout for the action (float, in seconds).
            lock_timeout: timeout for locking topology (float, in seconds).
            info_to_debug: if set, changes the info messages to debug.

        Returns:
            The console output of the action.
        """
        req = vtctl_pb2.ExecuteVtctlCommandArgs(
            args=args,
            action_timeout=long(action_timeout * 1000000000),
            lock_timeout=long(lock_timeout * 1000000000))
        console_result = ''
        with self.stub as stub:
            for e in stub.ExecuteVtctlCommand(req, action_timeout):
                if e.level == 0:
                    if info_to_debug:
                        logging.debug('%s', e.value)
                    else:
                        logging.info('%s', e.value)
                elif e.level == 1:
                    logging.warning('%s', e.value)
                elif e.level == 2:
                    logging.error('%s', e.value)
                elif e.level == 3:
                    console_result += e.value

        return console_result

vtctl_client.register_conn_class("grpc", GRPCVtctlClient)
