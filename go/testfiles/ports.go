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

	// GoVtTabletserverCustomruleZkcustomrulePort is used by the go/vt/tabletserver/customrule/zkcustomrule package.
	// Takes three ports.
	GoVtTabletserverCustomruleZkcustomrulePort = GoVtTopoZk2topoPort + 3

	// GoVtZktopoPort is used by the go/vt/zktopo package.
	// Takes three ports.
	GoVtZktopoPort = GoVtTabletserverCustomruleZkcustomrulePort + 3

	// GoVtEtcdtopoPort is used by the go/vt/etcdtopo package.
	// Takes two ports.
	GoVtEtcdtopoPort = GoVtZktopoPort + 3

	// GoVtTopoConsultopoPort is used by the go/vt/topo/consultopo package.
	// Takes five ports.
	GoVtTopoConsultopoPort = GoVtEtcdtopoPort + 2
)

//
// Zookeeper server ID definitions. Unit tests may run at the
// same time, so they can't use the same Zookeeper server IDs.
//
var (
	// GoVtTopoZk2topoZkID is used by the go/vt/topo/zk2topo package.
	GoVtTopoZk2topoZkID = 1

	// GoVtTabletserverCustomruleZkcustomruleZkID is used by the
	// go/vt/tabletserver/customrule/zkcustomrule package.
	GoVtTabletserverCustomruleZkcustomruleZkID = 2

	// GoVtZktopoZkID is used by the go/vt/zktopo package.
	GoVtZktopoZkID = 3
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
