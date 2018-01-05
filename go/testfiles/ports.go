/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
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

//
// Port definitions. Unit tests may run at the same time,
// so they should not use the same ports.
//
var (
	// vtPortStart is the starting port for all tests.
	vtPortStart = getPortStart()

	// GoVtTopoEtcd2topoPort is used by the go/vt/topo/etcd2topo package.
	// Takes two ports.
	GoVtTopoEtcd2topoPort = vtPortStart

	// GoVtTopoZk2topoPort is used by the go/vt/topo/zk2topo package.
	// Takes three ports.
	GoVtTopoZk2topoPort = GoVtTopoEtcd2topoPort + 2

	// GoVtTopoConsultopoPort is used by the go/vt/topo/consultopo package.
	// Takes five ports.
	GoVtTopoConsultopoPort = GoVtTopoZk2topoPort + 3
)

//
// Zookeeper server ID definitions. Unit tests may run at the
// same time, so they can't use the same Zookeeper server IDs.
//
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
