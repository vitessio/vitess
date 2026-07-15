/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import "time"

const (
	// defaultCell is the default cell name.
	defaultCell = "zone1"

	// defaultMySQLVersion is the default MySQL version.
	defaultMySQLVersion = "8.4"

	// defaultShards is the default number of shards (unsharded).
	defaultShards = 1

	// defaultDurabilityPolicy is the default durability policy, matching the
	// vtctldclient CreateKeyspace default.
	defaultDurabilityPolicy = "none"

	// defaultTopoImplementation is the default topology server implementation.
	defaultTopoImplementation = "etcd2"

	// topoGlobalRoot is the global root path in the topology server.
	topoGlobalRoot = "/vitess/global"

	// sidecarDBName is the sidecar database name passed to CreateKeyspace.
	sidecarDBName = "_vt"

	// etcdClientPort is the etcd client port inside the container.
	etcdClientPort = 2379

	// vtctldHTTPPort and vtctldGRPCPort are the vtctld ports inside the container.
	vtctldHTTPPort = 15000
	vtctldGRPCPort = 15999

	// vtgateHTTPPort, vtgateGRPCPort and vtgateMySQLPort are the vtgate ports
	// inside the container.
	vtgateHTTPPort  = 15001
	vtgateGRPCPort  = 15999
	vtgateMySQLPort = 15306

	// tabletHTTPPort and tabletGRPCPort are the vttablet ports inside every
	// tablet container. Tablets live in separate containers, so the ports do
	// not need to be unique per tablet.
	tabletHTTPPort = 15100
	tabletGRPCPort = 16100

	// tabletMysqlctldGRPCPort is the mysqlctld gRPC port inside every tablet
	// container of a cluster started with WithMysqlctld. The framework exposes
	// it so the test process can reach mysqlctld from the host.
	tabletMysqlctldGRPCPort = 16200

	// tabletMySQLPort is the mysqld port inside tablet and comparison-MySQL
	// containers.
	tabletMySQLPort = 3306

	// vtorcHTTPPort is the VTOrc port inside the container.
	vtorcHTTPPort = 16000

	// firstTabletUID is the UID assigned to the first tablet in a cluster.
	firstTabletUID = 100

	// vtDataRoot is the VTDATAROOT path baked into the vitesst image.
	vtDataRoot = "/vt/vtdataroot"

	// containerFilesDir is a directory in the vitesst image, owned by the
	// vitess user, where the framework and tests drop files into containers.
	containerFilesDir = "/vt/files"

	// imageInitDBPath is where the vitesst image carries the source tree's
	// config/init_db.sql.
	imageInitDBPath = "/vt/config/init_db.sql"

	// vtgateConfigPath is the vtgate config file inside its container. The
	// vtgate watches it, so WriteConfig changes apply to the running process.
	vtgateConfigPath = containerFilesDir + "/vtgate.json"

	// vtgateQueryLogPath is where vtgate writes its query log inside the
	// container.
	vtgateQueryLogPath = containerFilesDir + "/vtgate_querylog.txt"

	// customSQLMarker is the marker line in init_db.sql where custom SQL is
	// spliced in.
	customSQLMarker = "# {{custom_sql}}"

	// defaultStartupTimeout bounds how long a component container may take to
	// become ready. Tablet containers get tabletStartupTimeout because they
	// also run mysqlctl init before vttablet starts.
	defaultStartupTimeout = 2 * time.Minute
	tabletStartupTimeout  = 3 * time.Minute

	// defaultPollInterval is the interval for polling container readiness.
	defaultPollInterval = 100 * time.Millisecond

	// defaultOperationTimeout bounds control-plane operations such as
	// vtctldclient invocations and process-level stop/start waits.
	defaultOperationTimeout = 2 * time.Minute

	// terminateTimeout bounds cluster teardown.
	terminateTimeout = 60 * time.Second
)

// dbaUser is the MySQL user the framework uses for direct tablet and
// comparison-MySQL connections from the host. The framework grants it host
// ('%') access when assembling init_db.sql.
const dbaUser = "vt_dba"
