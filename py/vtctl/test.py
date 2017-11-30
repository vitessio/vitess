import datetime

import grpc
import json

from vtctl import grpc_vtctl_client

c = grpc_vtctl_client.GRPCVtctlClient('localhost:15999')
print c
