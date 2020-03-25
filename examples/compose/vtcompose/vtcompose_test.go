/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

var (
	testVtOpts = vtOptions{
		webPort:       DefaultWebPort,
		gRpcPort:      DefaultGrpcPort,
		mySqlPort:     DefaultMysqlPort,
		topologyFlags: DefaultTopologyFlags,
		cell:          DefaultCell,
	}
	testComposeFile       = readFile("./docker-compose.base.yml")
	testKeyspaceInfoMap   = parseKeyspaceInfo(DefaultKeyspaceData)
	testExternalDbInfoMap = parseExternalDbData(DefaultExternalDbData)
	referenceFile         string
)

func TestGenerateCorrectFileWithDefaultOpts(t *testing.T) {
	baseFile := testComposeFile
	finalFile := applyDockerComposePatches(baseFile, testKeyspaceInfoMap, testExternalDbInfoMap, testVtOpts)

	yamlString := string(finalFile)
	assert.YAMLEq(t, referenceFile, yamlString)
}

func TestOptsAppliedThroughoutGeneratedFile(t *testing.T) {
	baseFile := testComposeFile
	options := vtOptions{
		webPort:       55_555,
		gRpcPort:      66_666,
		mySqlPort:     77_777,
		topologyFlags: "-custom -flags",
		cell:          "custom cell",
	}
	finalFile := applyDockerComposePatches(baseFile, testKeyspaceInfoMap, testExternalDbInfoMap, options)
	yamlString := string(finalFile)

	// These asserts are not exhaustive, but should cover most cases.
	assert.NotContains(t, yamlString, strconv.Itoa(DefaultWebPort))
	assert.Contains(t, yamlString, strconv.Itoa(options.webPort))

	assert.NotContains(t, yamlString, strconv.Itoa(DefaultGrpcPort))
	assert.Contains(t, yamlString, strconv.Itoa(options.gRpcPort))

	assert.NotContains(t, yamlString, ":"+strconv.Itoa(DefaultMysqlPort))
	assert.Contains(t, yamlString, ":"+strconv.Itoa(options.webPort))

	assert.NotContains(t, yamlString, fmt.Sprintf("-cell %s", DefaultCell))
	assert.Contains(t, yamlString, fmt.Sprintf("-cell %s", options.cell))

	assert.Contains(t, yamlString, fmt.Sprintf("- TOPOLOGY_FLAGS=%s", options.topologyFlags))
	assert.NotContains(t, yamlString, DefaultTopologyFlags)
}

func init() {
	referenceFile = `
services:
  consul1:
    command: agent -server -bootstrap-expect 3 -ui -disable-host-node-id -client 0.0.0.0
    hostname: consul1
    image: consul:latest
    ports:
    - 8400:8400
    - 8500:8500
    - 8600:8600
  consul2:
    command: agent -server -retry-join consul1 -disable-host-node-id
    depends_on:
    - consul1
    expose:
    - "8400"
    - "8500"
    - "8600"
    hostname: consul2
    image: consul:latest
  consul3:
    command: agent -server -retry-join consul1 -disable-host-node-id
    depends_on:
    - consul1
    expose:
    - "8400"
    - "8500"
    - "8600"
    hostname: consul3
    image: consul:latest
  schemaload_test_keyspace:
    command:
    - sh
    - -c
    - /script/schemaload.sh
    depends_on:
      vttablet101:
        condition: service_healthy
      vttablet201:
        condition: service_healthy
    environment:
    - TOPOLOGY_FLAGS=-topo_implementation consul -topo_global_server_address consul1:8500
      -topo_global_root vitess/global
    - WEB_PORT=8080
    - GRPC_PORT=15999
    - CELL=test
    - KEYSPACE=test_keyspace
    - TARGETTAB=test-0000000101
    - SLEEPTIME=15
    - VSCHEMA_FILE=test_keyspace_vschema.json
    - SCHEMA_FILES=test_keyspace_schema_file.sql
    - POST_LOAD_FILE=
    - EXTERNAL_DB=0
    image: vitess/base
    volumes:
    - .:/script
  schemaload_unsharded_keyspace:
    command:
    - sh
    - -c
    - /script/schemaload.sh
    depends_on:
      vttablet301:
        condition: service_healthy
    environment:
    - TOPOLOGY_FLAGS=-topo_implementation consul -topo_global_server_address consul1:8500
      -topo_global_root vitess/global
    - WEB_PORT=8080
    - GRPC_PORT=15999
    - CELL=test
    - KEYSPACE=unsharded_keyspace
    - TARGETTAB=test-0000000301
    - SLEEPTIME=15
    - VSCHEMA_FILE=unsharded_keyspace_vschema.json
    - SCHEMA_FILES=unsharded_keyspace_schema_file.sql
    - POST_LOAD_FILE=
    - EXTERNAL_DB=0
    image: vitess/base
    volumes:
    - .:/script
  vtctld:
    command:
    - sh
    - -c
    - ' $$VTROOT/bin/vtctld -topo_implementation consul -topo_global_server_address
      consul1:8500 -topo_global_root vitess/global -cell test -workflow_manager_init
      -workflow_manager_use_election -service_map ''grpc-vtctl'' -backup_storage_implementation
      file -file_backup_storage_root $$VTDATAROOT/backups -logtostderr=true -port
      8080 -grpc_port 15999 -pid_file $$VTDATAROOT/tmp/vtctld.pid '
    depends_on:
    - consul1
    - consul2
    - consul3
    image: vitess/base
    ports:
    - 15000:8080
    - "15999"
    volumes:
    - .:/script
  vtgate:
    command:
    - sh
    - -c
    - '/script/run-forever.sh $$VTROOT/bin/vtgate -topo_implementation consul -topo_global_server_address
      consul1:8500 -topo_global_root vitess/global -logtostderr=true -port 8080 -grpc_port
      15999 -mysql_server_port 15306 -mysql_auth_server_impl none -cell test -cells_to_watch
      test -tablet_types_to_wait MASTER,REPLICA,RDONLY -gateway_implementation discoverygateway
      -service_map ''grpc-vtgateservice'' -pid_file $$VTDATAROOT/tmp/vtgate.pid -normalize_queries=true '
    depends_on:
    - vtctld
    image: vitess/base
    ports:
    - 15099:8080
    - "15999"
    - 15306:15306
    volumes:
    - .:/script
  vttablet101:
    command:
    - sh
    - -c
    - /script/vttablet-up.sh 101
    depends_on:
    - vtctld
    environment:
    - TOPOLOGY_FLAGS=-topo_implementation consul -topo_global_server_address consul1:8500
      -topo_global_root vitess/global
    - WEB_PORT=8080
    - GRPC_PORT=15999
    - CELL=test
    - KEYSPACE=test_keyspace
    - SHARD=-80
    - ROLE=master
    - VTHOST=vttablet101
    - EXTERNAL_DB=0
    - DB_PORT=
    - DB_HOST=
    - DB_USER=
    - DB_PASS=
    - DB_CHARSET=
    healthcheck:
      interval: 30s
      retries: 15
      test:
      - CMD-SHELL
      - curl localhost:8080/debug/health
      timeout: 10s
    image: vitess/base
    ports:
    - 15101:8080
    - "15999"
    - "3306"
    volumes:
    - .:/script
  vttablet102:
    command:
    - sh
    - -c
    - /script/vttablet-up.sh 102
    depends_on:
    - vtctld
    environment:
    - TOPOLOGY_FLAGS=-topo_implementation consul -topo_global_server_address consul1:8500
      -topo_global_root vitess/global
    - WEB_PORT=8080
    - GRPC_PORT=15999
    - CELL=test
    - KEYSPACE=test_keyspace
    - SHARD=-80
    - ROLE=replica
    - VTHOST=vttablet102
    - EXTERNAL_DB=0
    - DB_PORT=
    - DB_HOST=
    - DB_USER=
    - DB_PASS=
    - DB_CHARSET=
    healthcheck:
      interval: 30s
      retries: 15
      test:
      - CMD-SHELL
      - curl localhost:8080/debug/health
      timeout: 10s
    image: vitess/base
    ports:
    - 15102:8080
    - "15999"
    - "3306"
    volumes:
    - .:/script
  vttablet201:
    command:
    - sh
    - -c
    - /script/vttablet-up.sh 201
    depends_on:
    - vtctld
    environment:
    - TOPOLOGY_FLAGS=-topo_implementation consul -topo_global_server_address consul1:8500
      -topo_global_root vitess/global
    - WEB_PORT=8080
    - GRPC_PORT=15999
    - CELL=test
    - KEYSPACE=test_keyspace
    - SHARD=80-
    - ROLE=master
    - VTHOST=vttablet201
    - EXTERNAL_DB=0
    - DB_PORT=
    - DB_HOST=
    - DB_USER=
    - DB_PASS=
    - DB_CHARSET=
    healthcheck:
      interval: 30s
      retries: 15
      test:
      - CMD-SHELL
      - curl localhost:8080/debug/health
      timeout: 10s
    image: vitess/base
    ports:
    - 15201:8080
    - "15999"
    - "3306"
    volumes:
    - .:/script
  vttablet202:
    command:
    - sh
    - -c
    - /script/vttablet-up.sh 202
    depends_on:
    - vtctld
    environment:
    - TOPOLOGY_FLAGS=-topo_implementation consul -topo_global_server_address consul1:8500
      -topo_global_root vitess/global
    - WEB_PORT=8080
    - GRPC_PORT=15999
    - CELL=test
    - KEYSPACE=test_keyspace
    - SHARD=80-
    - ROLE=replica
    - VTHOST=vttablet202
    - EXTERNAL_DB=0
    - DB_PORT=
    - DB_HOST=
    - DB_USER=
    - DB_PASS=
    - DB_CHARSET=
    healthcheck:
      interval: 30s
      retries: 15
      test:
      - CMD-SHELL
      - curl localhost:8080/debug/health
      timeout: 10s
    image: vitess/base
    ports:
    - 15202:8080
    - "15999"
    - "3306"
    volumes:
    - .:/script
  vttablet301:
    command:
    - sh
    - -c
    - /script/vttablet-up.sh 301
    depends_on:
    - vtctld
    environment:
    - TOPOLOGY_FLAGS=-topo_implementation consul -topo_global_server_address consul1:8500
      -topo_global_root vitess/global
    - WEB_PORT=8080
    - GRPC_PORT=15999
    - CELL=test
    - KEYSPACE=unsharded_keyspace
    - SHARD=-
    - ROLE=master
    - VTHOST=vttablet301
    - EXTERNAL_DB=0
    - DB_PORT=
    - DB_HOST=
    - DB_USER=
    - DB_PASS=
    - DB_CHARSET=
    healthcheck:
      interval: 30s
      retries: 15
      test:
      - CMD-SHELL
      - curl localhost:8080/debug/health
      timeout: 10s
    image: vitess/base
    ports:
    - 15301:8080
    - "15999"
    - "3306"
    volumes:
    - .:/script
  vtwork:
    command:
    - sh
    - -c
    - '$$VTROOT/bin/vtworker -topo_implementation consul -topo_global_server_address
      consul1:8500 -topo_global_root vitess/global -cell test -logtostderr=true -service_map
      ''grpc-vtworker'' -port 8080 -grpc_port 15999 -use_v3_resharding_mode=true -pid_file
      $$VTDATAROOT/tmp/vtwork.pid '
    depends_on:
    - vtctld
    image: vitess/base
    ports:
    - 15100:8080
    - "15999"
version: "2.1"
`
}
