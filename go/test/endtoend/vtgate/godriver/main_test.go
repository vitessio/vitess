/*
Copyright 2020 The Vitess Authors.

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

package godriver

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vitessdriver"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	cell            = "zone1"
	hostname        = "localhost"
	KeyspaceName    = "customer"
	SchemaSQL       = `
create table my_message(
	# required columns
	id bigint NOT NULL COMMENT 'often an event id, can also be auto-increment or a sequence',
	priority tinyint NOT NULL DEFAULT '50' COMMENT 'lower number priorities process first',
	epoch bigint NOT NULL DEFAULT '0' COMMENT 'Vitess increments this each time it sends a message, and is used for incremental backoff doubling',
	time_next bigint DEFAULT 0 COMMENT 'the earliest time the message will be sent in epoch nanoseconds. Must be null if time_acked is set',
	time_acked bigint DEFAULT NULL COMMENT 'the time the message was acked in epoch nanoseconds. Must be null if time_next is set',

	# add as many custom fields here as required
	# optional - these are suggestions
	tenant_id bigint,
	message json,

	# required indexes
	primary key(id),
	index poller_idx(time_acked, priority, time_next desc)

	# add any secondary indexes or foreign keys - no restrictions
) comment 'vitess_message,vt_min_backoff=30,vt_max_backoff=3600,vt_ack_wait=30,vt_purge_after=86400,vt_batch_size=10,vt_cache_size=10000,vt_poller_interval=30'
`
	VSchema = `
{
  "sharded": true,
  "vindexes": {
    "hash": {
      "type": "hash"
    }
  },
  "tables": {
    "my_message": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "hash"
        }
      ]
    }
  }
}
`

	testMessage = "{\"message\":\"hello world\"}"
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		Keyspace := &cluster.Keyspace{
			Name:      KeyspaceName,
			SchemaSQL: SchemaSQL,
			VSchema:   VSchema,
		}
		clusterInstance.VtTabletExtraArgs = []string{
			"--queryserver-config-transaction-timeout", "3",
		}
		if err := clusterInstance.StartKeyspace(*Keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			log.Fatal(err.Error())
			return 1
		}

		// Start vtgate
		clusterInstance.VtGateExtraArgs = []string{"--warn_sharded_only=true"}
		if err := clusterInstance.StartVtgate(); err != nil {
			log.Fatal(err.Error())
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestStreamMessaging(t *testing.T) {
	defer cluster.PanicHandler(t)

	cnf := vitessdriver.Configuration{
		Protocol: "grpc",
		Address:  clusterInstance.Hostname + ":" + strconv.Itoa(clusterInstance.VtgateGrpcPort),
		GRPCDialOptions: []grpc.DialOption{
			grpc.WithDefaultCallOptions(),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                300 * time.Second,
				Timeout:             600 * time.Second,
				PermitWithoutStream: true,
			}),
		},
	}

	// for inserting data
	db, err := vitessdriver.OpenWithConfiguration(cnf)
	require.NoError(t, err)
	defer db.Close()

	// Exec not allowed in streaming
	_, err = db.Exec(fmt.Sprintf("insert into my_message(id, message) values(1, '%s')", testMessage))
	require.NoError(t, err)

	// for streaming messages
	cnf.Streaming = true
	streamDB, err := vitessdriver.OpenWithConfiguration(cnf)
	require.NoError(t, err)
	defer streamDB.Close()

	// Exec not allowed in streaming
	_, err = streamDB.Exec("stream * from my_message")
	assert.EqualError(t, err, "Exec not allowed for streaming connections")

	row := streamDB.QueryRow("stream * from my_message")
	require.NoError(t, row.Err())
}
