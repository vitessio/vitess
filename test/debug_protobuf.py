#!/usr/bin/env python

import sys
for i in sys.path:
  print i

from vtproto import topodata_pb2

from google import protobuf
print protobuf
print protobuf.__version__
