# Copyright 2014, Google Inc. All rights reserved.
# Use of this source code is governed by a BSD-style license that can
# be found in the LICENSE file.

import logging


# mapping from protocol to python class. The protocol matches the string
# used by vtctlclient as a -vtctl_client_protocol
vtctl_client_conn_classes = dict()


def register_conn_class(protocol, c):
  vtctl_client_conn_classes[protocol] = c


def connect(protocol, *pargs, **kargs):
  if not protocol in vtctl_client_conn_classes:
    raise Exception('Unknown vtclient protocol', protocol)
  conn = vtctl_client_conn_classes[protocol](*pargs, **kargs)
  conn.dial()
  return conn
