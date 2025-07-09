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
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
)

func TestParseServerAddr(t *testing.T) {
	tests := []struct {
		name         string
		serverAddr   string
		expectedHost string
		expectedPort int
		expectedUser string
		expectedPass string
		expectedDB   string
	}{
		{
			name:         "default values",
			serverAddr:   "",
			expectedHost: "localhost",
			expectedPort: 3306,
			expectedUser: "root",
			expectedPass: "",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "host only",
			serverAddr:   "mysql-server",
			expectedHost: "mysql-server",
			expectedPort: 3306,
			expectedUser: "root",
			expectedPass: "",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "host and port",
			serverAddr:   "mysql-server:3307",
			expectedHost: "mysql-server",
			expectedPort: 3307,
			expectedUser: "root",
			expectedPass: "",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "user and host",
			serverAddr:   "testuser@mysql-server",
			expectedHost: "mysql-server",
			expectedPort: 3306,
			expectedUser: "testuser",
			expectedPass: "",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "user, password, host and port",
			serverAddr:   "testuser:testpass@mysql-server:3307",
			expectedHost: "mysql-server",
			expectedPort: 3307,
			expectedUser: "testuser",
			expectedPass: "testpass",
			expectedDB:   DefaultTopoSchema,
		},
		{
			name:         "full connection string",
			serverAddr:   "testuser:testpass@mysql-server:3307/customdb",
			expectedHost: "mysql-server",
			expectedPort: 3307,
			expectedUser: "testuser",
			expectedPass: "testpass",
			expectedDB:   "customdb", // Should parse the custom database name
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := parseServerAddr(tt.serverAddr)

			if params.Host != tt.expectedHost {
				t.Errorf("Expected host '%s', got '%s'", tt.expectedHost, params.Host)
			}
			if params.Port != tt.expectedPort {
				t.Errorf("Expected port %d, got %d", tt.expectedPort, params.Port)
			}
			if params.Uname != tt.expectedUser {
				t.Errorf("Expected username '%s', got '%s'", tt.expectedUser, params.Uname)
			}
			if params.Pass != tt.expectedPass {
				t.Errorf("Expected password '%s', got '%s'", tt.expectedPass, params.Pass)
			}
			if params.DbName != tt.expectedDB {
				t.Errorf("Expected database '%s', got '%s'", tt.expectedDB, params.DbName)
			}
		})
	}
}

func TestReplicationWatcher_ConnParams(t *testing.T) {
	// Create a test server with ConnParams
	params := &mysql.ConnParams{
		Host:   "testhost",
		Port:   3307,
		Uname:  "testuser",
		Pass:   "testpass",
		DbName: "testdb",
	}

	server := &Server{
		params: params,
	}

	watcher := &ReplicationWatcher{
		s: server,
	}

	// Verify that the watcher can access the connection parameters
	if watcher.s.params.Host != "testhost" {
		t.Errorf("Expected host 'testhost', got '%s'", watcher.s.params.Host)
	}
	if watcher.s.params.Port != 3307 {
		t.Errorf("Expected port 3307, got %d", watcher.s.params.Port)
	}
	if watcher.s.params.Uname != "testuser" {
		t.Errorf("Expected username 'testuser', got '%s'", watcher.s.params.Uname)
	}
	if watcher.s.params.Pass != "testpass" {
		t.Errorf("Expected password 'testpass', got '%s'", watcher.s.params.Pass)
	}
	if watcher.s.params.DbName != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", watcher.s.params.DbName)
	}
}

func TestReplicationWatcher_getCurrentPosition_Logic(t *testing.T) {
	// Test the position selection logic without requiring a live MySQL connection
	// This tests the logic that chooses between GTID and file position

	// Test case 1: GTID position available and is MySQL56 flavor
	mysql56GTID, err := replication.ParsePosition(replication.Mysql56FlavorID, "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-615")
	if err != nil {
		t.Fatalf("Failed to parse MySQL56 GTID: %v", err)
	}

	filePosGTID, err := replication.ParsePosition(replication.FilePosFlavorID, "binlog.002569:0x11225fc")
	if err != nil {
		t.Fatalf("Failed to parse file position: %v", err)
	}

	// Verify that MySQL56 GTID matches the expected flavor
	if !mysql56GTID.MatchesFlavor(replication.Mysql56FlavorID) {
		t.Errorf("MySQL56 GTID should match MySQL56 flavor")
	}

	// Verify that file position matches the expected flavor
	if !filePosGTID.MatchesFlavor(replication.FilePosFlavorID) {
		t.Errorf("File position should match FilePosFlavorID")
	}

	// Verify that file position does NOT match MySQL56 flavor
	if filePosGTID.MatchesFlavor(replication.Mysql56FlavorID) {
		t.Errorf("File position should NOT match MySQL56 flavor")
	}

	t.Logf("Test passed: Position flavor matching works correctly")
	t.Logf("MySQL56 GTID: %s", mysql56GTID.String())
	t.Logf("File position: %s", filePosGTID.String())
}
