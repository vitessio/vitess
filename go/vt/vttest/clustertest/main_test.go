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

package clustertest

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"testing"
	"vitess.io/vitess/go/vt/vttest"
)

func TestMain(m *testing.M) {
	flag.Parse()

	var (
		EtcdPort             = 2379
		VtctldHttpPort       = 15000
		VtctldGrpcPort       = 15999
		BaseMysqlPort        = 14000
		BaseVttabletPort     = 15100
		BaseVttabletGrpcPort = 16100
		VtgatePort           = 15001
		VtgateGrpcPort       = 15991
		MySQLServerPort      = 15306
		KeyspaceName         = "commerce"
		Shard                = "0"
		Cell                 = "zone1"
		BaseTabletUID        = 100
		Tablets              = []int{0, 1, 2}
		MysqlCtlProcesses    = make([]vttest.MysqlctlProcess, 0)
		VttabletProcesses    = make([]vttest.VttabletProcess, 0)
		VtgateProcess        = vttest.VtgateProcess{}
		//
		HostName  = "localhost"
		SQLSchema = `create table product( 
		sku varbinary(128),
			description varbinary(128),
			price bigint,
			primary key(sku)
		) ENGINE=InnoDB;
		create table customer(
			id bigint not null auto_increment,
			email varchar(128),
			primary key(id)
		) ENGINE=InnoDB;
		create table corder(
			order_id bigint not null auto_increment,
			customer_id bigint,
			sku varbinary(128),
			price bigint,
			primary key(order_id)
		) ENGINE=InnoDB;`

		VSchema = `{
						"tables": {
							"product": {},
							"customer": {},
							"corder": {}
						}
					}`
	)

	exitCode := func() int {

		etcdProcess := vttest.EtcdProcessInstance(EtcdPort)
		if err := etcdProcess.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			etcdProcess.TearDown()
			return 1
		}
		defer etcdProcess.TearDown()

		vtcltlProcess := vttest.VtctlProcessInstance()
		vtcltlProcess.AddCellInfo()

		vtctldProcess := vttest.VtctldProcessInstance(VtctldHttpPort, VtctldGrpcPort)
		if err := vtctldProcess.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			vtctldProcess.TearDown()
			etcdProcess.TearDown()
			return 1
		}
		var i = 0
		for ; i < len(Tablets); i++ {
			CurrentTabletUID := BaseTabletUID + Tablets[i]
			mysqlCtlProcess := vttest.MysqlCtlProcessInstance(CurrentTabletUID, BaseMysqlPort+Tablets[i])
			MysqlCtlProcesses = append(MysqlCtlProcesses, *mysqlCtlProcess)
			_ = mysqlCtlProcess.Start()

			vttabletProcess := vttest.VttabletProcessInstance(BaseVttabletPort+Tablets[i], BaseVttabletGrpcPort+Tablets[i], CurrentTabletUID, Cell, Shard, "localhost", KeyspaceName, VtctldHttpPort, "replica")
			if i == 2 {
				vttabletProcess.TabletType = "rdonly"
			}
			VttabletProcesses = append(VttabletProcesses, *vttabletProcess)
			if err := vttabletProcess.Setup(); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				VttabletProcessTeardown(VttabletProcesses)
				MysqlCtlProcessTeardown(MysqlCtlProcesses)
				vtctldProcess.TearDown()
				etcdProcess.TearDown()
				return 1
			}
			defer vttabletProcess.TearDown()
		}
		println("Waiting for vtctlclient commands to make master")
		vtctlClient := vttest.VtctlClientProcessInstance("localhost", VtctldGrpcPort)
		//Make one of replica as master
		vtctlClient.InitShardMaster(KeyspaceName, Shard, Cell, BaseTabletUID)
		// Apply SQL Schema
		vtctlClient.ApplySchema(KeyspaceName, SQLSchema)
		// Apply VSchema
		vtctlClient.ApplyVSchema(KeyspaceName, VSchema)
		println("Waiting for vtgate")

		VtgateProcess = *vttest.VtgateProcessInstance(VtgatePort, VtgateGrpcPort, MySQLServerPort, Cell, Cell, HostName, "MASTER,REPLICA")
		if err := VtgateProcess.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			VtgateProcess.TearDown()
			VttabletProcessTeardown(VttabletProcesses)
			MysqlCtlProcessTeardown(MysqlCtlProcesses)
			vtctldProcess.TearDown()
			etcdProcess.TearDown()
			return 1
		}
		println("Done with single shard cluster setup, Now executing testcases.")

		defer vtctldProcess.TearDown()
		defer MysqlCtlProcessTeardown(MysqlCtlProcesses)
		defer VtgateProcess.TearDown()
		return m.Run()
	}()
	os.Exit(exitCode)
}

func MysqlCtlProcessTeardown(MySqlCtlProcesses []vttest.MysqlctlProcess) {
	for _, mysqlctlProcess := range MySqlCtlProcesses {
		_ = mysqlctlProcess.Stop()
	}
}

func VttabletProcessTeardown(VttableProcesses []vttest.VttabletProcess) {
	for _, vttabletProcess := range VttableProcesses {
		_ = vttabletProcess.TearDown()
	}
}

func testURL(t *testing.T, url string, testCaseName string) {
	statusCode := getStatusForURL(url)
	if got, want := statusCode, 200; got != want {
		t.Errorf("select:\n%v want\n%v for %s", got, want, testCaseName)
	}
}

// getStatusForUrl returns the status code for the URL
func getStatusForURL(url string) int {
	resp, _ := http.Get(url)
	if resp != nil {
		return resp.StatusCode
	}
	return 0
}
