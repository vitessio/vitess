/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package testfiles

import (
	"fmt"
	"os"
	"strconv"
)

// This file contains helper methods and declarations so all unit
// tests use different ports.
//
// We also use it to allocate Zookeeper server IDs.

// Port definitions. Unit tests may run at the same time,
// so they should not use the same ports.
//
// Each port has its own constant; do not introduce ad-hoc `port + N`
// arithmetic at call sites or in this file. New entries must extend
// the range without overlapping any existing constant.
var (
	// vtPortStart is the starting port for all tests.
	vtPortStart = getPortStart()

	// Ports used by the go/vt/topo/etcd2topo package tests.
	GoVtTopoEtcd2topoPort        = vtPortStart     // etcd client URL (plaintext)
	GoVtTopoEtcd2topoPeerPort    = vtPortStart + 1 // etcd peer URL (plaintext)
	GoVtTopoEtcd2topoTLSPort     = vtPortStart + 2 // etcd client URL (TLS)
	GoVtTopoEtcd2topoTLSPeerPort = vtPortStart + 3 // etcd peer URL (TLS)

	// Base port used by the go/vt/topo/zk2topo package tests.
	// zkctl.StartLocalZk consumes three consecutive ports (leader, election,
	// client) starting at this base, so vtPortStart+4..6 are reserved.
	GoVtTopoZk2topoPort = vtPortStart + 4

	// Ports used by the go/vt/topo/consultopo package tests.
	GoVtTopoConsultopoDNSPort     = vtPortStart + 7
	GoVtTopoConsultopoHTTPPort    = vtPortStart + 8
	GoVtTopoConsultopoSerfLANPort = vtPortStart + 9
	GoVtTopoConsultopoSerfWANPort = vtPortStart + 10

	// Ports used by the go/vt/vtctl/workflow package tests for the
	// etcd-backed keyspace routing rules tests.
	GoVtVtctlWorkflowPort     = vtPortStart + 11 // etcd client URL
	GoVtVtctlWorkflowPeerPort = vtPortStart + 12 // etcd peer URL
)

// Zookeeper server ID definitions. Unit tests may run at the
// same time, so they can't use the same Zookeeper server IDs.
var (
	// GoVtTopoZk2topoZkID is used by the go/vt/topo/zk2topo package.
	GoVtTopoZk2topoZkID = 1
)

func getPortStart() int {
	env := os.Getenv("VTPORTSTART")
	if env == "" {
		env = "6700"
	}
	portStart, err := strconv.Atoi(env)
	if err != nil {
		panic(fmt.Errorf("cannot parse VTPORTSTART: %v", err))
	}
	return portStart
}
