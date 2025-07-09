/*
Copyright 2025 The Vitess Authors.

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

package mysqltopo

import (
	"fmt"
	"os"
	"path"
	"testing"

	"vitess.io/vitess/go/vt/log"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttest"
)

var (
	mySQLTopoTestAddr string               // the address of the MySQL server used for testing
	testMySQLServer   *vttest.LocalCluster // global MySQL server instance
)

// TestMain handles global test setup and teardown
func TestMain(m *testing.M) {
	if addr := os.Getenv("MYSQL_TOPO_TEST_ADDR"); addr != "" {
		mySQLTopoTestAddr = addr
		log.Infof("Using custom MySQL server address: %s", mySQLTopoTestAddr)
	} else {
		log.Infof("No custom MySQL server address provided, attempting to set up test MySQL server")

		// Create a custom environment with a specific base port
		// BasePort 13000 means MySQL will be on port 13002 (BasePort + 2)
		env, err := vttest.NewLocalTestEnv(13000)
		if err != nil {
			panic(fmt.Sprintf("Failed to create test environment: %v", err))
		}

		// Set our custom init file to create the topo user
		initFile := path.Join(os.Getenv("VTROOT"), "go/vt/topo/mysqltopo/init_test_db.sql")
		env.InitDBFile = initFile

		// Try to set up a test MySQL server using vttest
		testMySQLServer = &vttest.LocalCluster{
			Config: vttest.Config{
				Topology: &vttestpb.VTTestTopology{
					Keyspaces: []*vttestpb.Keyspace{}, // Empty slice instead of nil
				},
				OnlyMySQL: true,
			},
			Env: env, // Use our custom environment with specific port
		}

		if err := testMySQLServer.Setup(); err != nil {
			panic(fmt.Sprintf("Failed to set up test MySQL server: %v", err))
		}

		// Get the MySQL port from the environment (should be 13002)
		mysqlPort := env.PortForProtocol("mysql", "")

		// Build the connection string using our custom topo user
		host := "127.0.0.1"
		user := "topo"
		pass := "topopass"
		mySQLTopoTestAddr = fmt.Sprintf("%s:%s@%s:%d/", user, pass, host, mysqlPort)
		log.Infof("Started test MySQL server at: %s (port: %d, user: %s)", mySQLTopoTestAddr, mysqlPort, user)
	}

	// Run the tests
	code := m.Run()

	// Clean up the global MySQL server if it was created
	if testMySQLServer != nil {
		log.Infof("Tearing down test MySQL server")
		testMySQLServer.TearDown()
	}

	os.Exit(code)
}
